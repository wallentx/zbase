pub mod app_model;
pub mod call_model;
pub mod composer_model;
pub mod conversation_model;
pub mod emoji_picker_model;
pub mod file_upload_model;
pub mod find_in_chat_model;
pub mod navigation_model;
pub mod new_chat_model;
pub mod notifications_model;
pub mod overlay_model;
pub mod profile_panel_model;
pub mod quick_switcher_model;
pub mod search_model;
pub mod settings_model;
pub mod sidebar_model;
pub mod thread_pane_model;
pub mod timeline_model;
pub mod workspace_model;

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use crate::domain::{
    affinity::Affinity,
    attachment::{AttachmentKind, AttachmentSource, AttachmentSummary, attachment_kind_from_path},
    call::{CallSessionSummary, CallStatus},
    conversation::{ConversationGroup, ConversationKind, ConversationSummary},
    ids::{
        CallId, ChannelId, ConversationId, DmId, MessageId, SidebarSectionId, UserId, WorkspaceId,
    },
    message::{BroadcastKind, EditMeta, MessageFragment, MessageRecord, MessageSendState},
    presence::{Availability, Presence},
    route::Route,
    search::{SearchFilter, SearchResult},
    user::UserSummary,
};
use crate::util::{
    deep_link::parse_keybase_chat_link,
    formatting::now_unix_ms,
    fuzzy::{
        FuzzyMatch, PreparedFuzzyCandidate, PreparedFuzzyQuery, fuzzy_match, fuzzy_match_prepared,
        prepare_fuzzy_candidate, prepare_fuzzy_query,
    },
};

use self::{
    app_model::{AppModel, Connectivity},
    call_model::CallModel,
    composer_model::{AutocompleteCandidate, AutocompleteState, ComposerMode, ComposerModel},
    conversation_model::ConversationModel,
    emoji_picker_model::{
        EmojiPickerItem, EmojiPickerModel, push_recent_alias, search_emoji_items,
        skin_tone_from_setting_value, skin_tone_to_setting_value,
    },
    file_upload_model::{FileUploadCandidate, FileUploadLightboxModel, UploadTarget},
    find_in_chat_model::FindInChatModel,
    navigation_model::{NavigationModel, RightPaneMode},
    new_chat_model::NewChatModel,
    notifications_model::{
        ActivityItem, ActivityKind, NotificationsModel, ToastAction, ToastNotification,
    },
    overlay_model::{
        FullscreenImageOverlay, OverlayModel, ReactionHoverTooltip, SidebarHoverTooltip,
    },
    profile_panel_model::ProfilePanelModel,
    quick_switcher_model::{QuickSwitcherModel, QuickSwitcherResult, QuickSwitcherResultKind},
    search_model::SearchModel,
    settings_model::{SettingsModel, ThemeMode},
    sidebar_model::{SidebarModel, SidebarRow, SidebarSection},
    thread_pane_model::ThreadPaneModel,
    timeline_model::{MessageRow, TimelineModel, TimelineRow},
    workspace_model::WorkspaceModel,
};

const QUICK_SWITCHER_CONVERSATION_LIMIT: usize = 120;
const QUICK_SWITCHER_UNREAD_LIMIT: usize = 10;
const QUICK_SWITCHER_RECENT_LIMIT: usize = 10;
const QUICK_SWITCHER_AFFINITY_HALF_LIFE_MS: f32 = 14.0 * 24.0 * 60.0 * 60.0 * 1000.0;
const QUICK_SWITCHER_AFFINITY_BOOST_SCALE: f32 = 35.0;
const QUICK_SWITCHER_AFFINITY_MAX_BOOST: i32 = 120;

#[derive(Clone, Debug)]
pub(crate) struct QuickSwitcherMatchCandidate {
    pub(crate) text: String,
    pub(crate) highlightable: bool,
    pub(crate) prepared: PreparedFuzzyCandidate,
}

#[derive(Clone, Debug)]
pub(crate) struct QuickSwitcherSearchEntry {
    pub(crate) conversation_id: ConversationId,
    pub(crate) label: String,
    pub(crate) sublabel: Option<String>,
    pub(crate) kind: QuickSwitcherResultKind,
    pub(crate) route: Route,
    pub(crate) unread_count: u32,
    pub(crate) mention_count: u32,
    pub(crate) dm_participant_count: Option<usize>,
    pub(crate) match_candidates: Vec<QuickSwitcherMatchCandidate>,
}

#[derive(Clone, Debug)]
pub(crate) struct QuickSwitcherSearchCorpus {
    pub(crate) revision: u64,
    pub(crate) entries: Vec<QuickSwitcherSearchEntry>,
    pub(crate) channel_entry_indices: Vec<usize>,
    pub(crate) dm_entry_indices: Vec<usize>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct QuickSwitcherLocalSearchOutput {
    pub(crate) results: Vec<QuickSwitcherResult>,
    pub(crate) matched_entry_indices: Vec<usize>,
    pub(crate) scanned_entries: usize,
    pub(crate) channel_scanned_entries: usize,
    pub(crate) dm_scanned_entries: usize,
    pub(crate) rejected_candidates: usize,
    pub(crate) fuzzy_evaluated: usize,
}

#[derive(Clone, Debug)]
struct ComposerDraftSnapshot {
    text: String,
    attachments: Vec<AttachmentSummary>,
    autocomplete: Option<AutocompleteState>,
    mode: ComposerMode,
}

#[derive(Clone, Debug)]
struct ThreadReplyDraftSnapshot {
    text: String,
    attachments: Vec<AttachmentSummary>,
    autocomplete: Option<AutocompleteState>,
}

#[derive(Clone, Debug)]
pub enum PendingSend {
    TimelineMessage(MessageId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendDispatch {
    NotSent,
    Immediate,
    Pending,
}

pub struct AppModels {
    pub app: AppModel,
    pub workspace: WorkspaceModel,
    pub navigation: NavigationModel,
    pub sidebar: SidebarModel,
    pub conversation: ConversationModel,
    pub timeline: TimelineModel,
    pub composer: ComposerModel,
    pub quick_switcher: QuickSwitcherModel,
    pub emoji_picker: EmojiPickerModel,
    pub find_in_chat: FindInChatModel,
    pub search: SearchModel,
    pub new_chat: NewChatModel,
    pub thread_pane: ThreadPaneModel,
    pub profile_panel: ProfilePanelModel,
    pub overlay: OverlayModel,
    pub call: CallModel,
    pub notifications: NotificationsModel,
    pub settings: SettingsModel,
    quick_switcher_profile_names: HashMap<String, String>,
    quick_switcher_search_corpus: Arc<QuickSwitcherSearchCorpus>,
    quick_switcher_search_corpus_revision: u64,
    quick_switcher_search_corpus_dirty: bool,
    composer_drafts: HashMap<ConversationId, ComposerDraftSnapshot>,
    thread_reply_drafts: HashMap<MessageId, ThreadReplyDraftSnapshot>,
    pending_send_outcomes: HashMap<MessageId, bool>,
    send_attempt_count: usize,
}

impl AppModels {
    pub fn demo() -> Self {
        Self::demo_with_settings(SettingsModel::default())
    }

    pub fn empty_with_settings(settings: SettingsModel) -> Self {
        let mut models = Self::demo_with_settings(settings);
        let workspace_id = WorkspaceId::new("ws_primary");
        let conversation_id = ConversationId::new("conv_placeholder");

        models.app.open_workspaces = vec![workspace_id.clone()];
        models.app.active_workspace_id = workspace_id.clone();
        models.app.global_unread_count = 0;
        models.app.current_user_id = None;
        models.app.current_user_display_name = "You".to_string();
        models.app.current_user_avatar_asset = None;

        models.workspace.workspace_id = workspace_id.clone();
        models.workspace.workspace_name = "Keybase".to_string();
        models.workspace.channels.clear();
        models.workspace.direct_messages.clear();

        models.sidebar.sections.clear();
        models.sidebar.filter.clear();
        models.sidebar.highlighted_route = Some(Route::WorkspaceHome {
            workspace_id: workspace_id.clone(),
        });

        models.navigation.current = Route::WorkspaceHome {
            workspace_id: workspace_id.clone(),
        };
        models.navigation.back_stack.clear();
        models.navigation.forward_stack.clear();
        models.navigation.right_pane = RightPaneMode::Hidden;

        models.conversation.summary = ConversationSummary {
            id: conversation_id.clone(),
            title: "No conversation selected".to_string(),
            kind: ConversationKind::DirectMessage,
            topic: String::new(),
            group: None,
            unread_count: 0,
            mention_count: 0,
            muted: false,
            last_activity_ms: 0,
        };
        models.conversation.member_count = 0;
        models.conversation.pinned_message = None;
        models.conversation.details = None;
        models.conversation.can_post = false;
        models.conversation.is_archived = false;

        models.timeline.conversation_id = conversation_id.clone();
        models.timeline.rows.clear();
        models.timeline.highlighted_message_id = None;
        models.timeline.unread_marker = None;
        models.timeline.pending_scroll_target = None;
        models.timeline.older_cursor = None;
        models.timeline.newer_cursor = None;
        models.timeline.loading_older = false;

        models.search.query.clear();
        models.search.filters.clear();
        models.search.results.clear();
        models.search.highlighted_index = None;
        models.search.is_loading = false;
        models.new_chat = NewChatModel::default();

        models.quick_switcher.query.clear();
        models.quick_switcher.results.clear();
        models.quick_switcher.selected_index = 0;
        models.quick_switcher.loading_messages = false;
        models.quick_switcher_profile_names.clear();
        models.emoji_picker = EmojiPickerModel::default();

        models.find_in_chat.close();

        models.thread_pane.open = false;
        models.thread_pane.root_message_id = None;
        models.thread_pane.replies.clear();
        models.thread_pane.reply_draft.clear();
        models.thread_pane.reply_attachments.clear();
        models.thread_pane.reply_autocomplete = None;
        models.thread_pane.following = false;
        models.thread_pane.loading = false;

        models.profile_panel.user_id = None;
        models.profile_panel.profile = None;
        models.profile_panel.loading = false;
        models.profile_panel.active_social_tab = Default::default();
        models.profile_panel.loading_social_list = false;

        models.overlay.new_chat_open = false;
        models.overlay.quick_switcher_open = false;
        models.overlay.command_palette_open = false;
        models.overlay.emoji_picker_open = false;
        models.overlay.reaction_target_message_id = None;
        models.overlay.fullscreen_image = None;
        models.overlay.active_modal = None;
        models.overlay.profile_card_user_id = None;
        models.overlay.profile_card_position = None;
        models.overlay.sidebar_hover_tooltip = None;
        models.overlay.reaction_hover_tooltip = None;

        models.call.active_call = None;
        models.call.is_muted = false;
        models.call.is_sharing_screen = false;

        models.notifications.toasts.clear();
        models.notifications.notification_center_count = 0;
        models.notifications.activity_items.clear();
        models.notifications.highlighted_index = None;

        models.composer.conversation_id = conversation_id;
        models.composer.mode = ComposerMode::Compose;
        models.composer.draft_text.clear();
        models.composer.attachments.clear();
        models.composer.autocomplete = None;

        models.composer_drafts.clear();
        models.thread_reply_drafts.clear();
        models.pending_send_outcomes.clear();
        models.send_attempt_count = 0;
        models.rebuild_quick_switcher_local_search_corpus();
        models.sync_emoji_picker_from_settings();

        models
    }

    pub fn demo_with_settings(settings: SettingsModel) -> Self {
        let acme_id = WorkspaceId::new("ws_acme");
        let friends_id = WorkspaceId::new("ws_friends");
        let oss_id = WorkspaceId::new("ws_oss");

        let general_channel_id = ChannelId::new("general");
        let design_channel_id = ChannelId::new("design");
        let hangout_channel_id = ChannelId::new("hangout");
        let travel_channel_id = ChannelId::new("travel");
        let zbase_channel_id = ChannelId::new("zbase");
        let gpui_channel_id = ChannelId::new("gpui");
        let alice_dm_id = DmId::new("alice");

        let general_conversation_id = ConversationId::new("conv_general");
        let design_conversation_id = ConversationId::new("conv_design");
        let hangout_conversation_id = ConversationId::new("conv_hangout");
        let travel_conversation_id = ConversationId::new("conv_travel");
        let zbase_conversation_id = ConversationId::new("conv_zbase");
        let gpui_conversation_id = ConversationId::new("conv_gpui");
        let alice_conversation_id = ConversationId::new("conv_alice");
        let alice_id = UserId::new("user_alice");
        let search_query = "gpui".to_string();
        let route = Route::Channel {
            workspace_id: acme_id.clone(),
            channel_id: general_channel_id.clone(),
        };

        let acme_group = ConversationGroup {
            id: "acme".to_string(),
            display_name: "Acme Product".to_string(),
        };
        let friends_group = ConversationGroup {
            id: "friends".to_string(),
            display_name: "Friends".to_string(),
        };
        let oss_group = ConversationGroup {
            id: "oss".to_string(),
            display_name: "Open Source".to_string(),
        };

        let general = ConversationSummary {
            id: general_conversation_id.clone(),
            title: "general".to_string(),
            kind: ConversationKind::Channel,
            topic: "Company-wide announcements".to_string(),
            group: Some(acme_group.clone()),
            unread_count: 3,
            mention_count: 1,
            muted: false,
            last_activity_ms: 1_711_000_000_000,
        };
        let design = ConversationSummary {
            id: design_conversation_id.clone(),
            title: "design".to_string(),
            kind: ConversationKind::Channel,
            topic: "Product design crit and UI system work".to_string(),
            group: Some(acme_group.clone()),
            unread_count: 0,
            mention_count: 0,
            muted: false,
            last_activity_ms: 1_710_000_000_000,
        };
        let hangout = ConversationSummary {
            id: hangout_conversation_id.clone(),
            title: "hangout".to_string(),
            kind: ConversationKind::Channel,
            topic: "Off-topic chat".to_string(),
            group: Some(friends_group.clone()),
            unread_count: 5,
            mention_count: 0,
            muted: false,
            last_activity_ms: 1_709_000_000_000,
        };
        let travel = ConversationSummary {
            id: travel_conversation_id.clone(),
            title: "travel".to_string(),
            kind: ConversationKind::Channel,
            topic: "Trip planning".to_string(),
            group: Some(friends_group.clone()),
            unread_count: 0,
            mention_count: 0,
            muted: false,
            last_activity_ms: 1_708_000_000_000,
        };
        let zbase_conv = ConversationSummary {
            id: zbase_conversation_id.clone(),
            title: "zbase".to_string(),
            kind: ConversationKind::Channel,
            topic: "Keybase UI client".to_string(),
            group: Some(oss_group.clone()),
            unread_count: 2,
            mention_count: 1,
            muted: false,
            last_activity_ms: 1_712_000_000_000,
        };
        let gpui_conv = ConversationSummary {
            id: gpui_conversation_id.clone(),
            title: "gpui".to_string(),
            kind: ConversationKind::Channel,
            topic: "GPUI framework discussion".to_string(),
            group: Some(oss_group.clone()),
            unread_count: 0,
            mention_count: 0,
            muted: false,
            last_activity_ms: 1_707_000_000_000,
        };
        let alice_dm = ConversationSummary {
            id: alice_conversation_id.clone(),
            title: "Alice Johnson".to_string(),
            kind: ConversationKind::DirectMessage,
            topic: "Product design".to_string(),
            group: None,
            unread_count: 1,
            mention_count: 0,
            muted: false,
            last_activity_ms: 1_713_000_000_000,
        };

        let general_message = MessageRecord {
            id: MessageId::new("msg_001"),
            conversation_id: general_conversation_id.clone(),
            author_id: alice_id,
            reply_to: None,
            thread_root_id: None,
            timestamp_ms: Some(1_712_000_000_000),
            event: None,
            link_previews: Vec::new(),
            permalink: "slack://acme/general/p/msg_001".to_string(),
            fragments: vec![
                MessageFragment::Text("Here is the shell layout for the GPUI port.".to_string()),
                MessageFragment::Code {
                    text: "AppWindow -> BodySplit -> TimelineList".to_string(),
                    lang: None,
                },
            ],
            source_text: None,
            attachments: vec![AttachmentSummary {
                name: "slack-shell.png".to_string(),
                kind: AttachmentKind::Image,
                size_bytes: 128_000,
                ..AttachmentSummary::default()
            }],
            reactions: Vec::new(),
            thread_reply_count: 4,
            send_state: MessageSendState::Sent,
            edited: None,
        };

        let composer = ComposerModel {
            conversation_id: general_conversation_id.clone(),
            mode: ComposerMode::Compose,
            draft_text: String::new(),
            attachments: Vec::new(),
            autocomplete: Some(AutocompleteState {
                trigger: '@',
                query: "ali".to_string(),
                trigger_offset: 0,
                selected_index: 0,
                candidates: Vec::new(),
            }),
        };
        let thread_pane = ThreadPaneModel {
            open: true,
            root_message_id: Some(MessageId::new("msg_001")),
            width_px: 360.0,
            following: true,
            replies: Vec::new(),
            reply_draft: String::new(),
            reply_attachments: Vec::new(),
            reply_autocomplete: None,
            loading: false,
        };

        let mut models = Self {
            app: AppModel {
                open_workspaces: vec![acme_id.clone(), friends_id.clone(), oss_id.clone()],
                active_workspace_id: acme_id.clone(),
                connectivity: Connectivity::Online,
                global_unread_count: 4,
                current_user_id: Some(UserId::new("cameron")),
                current_user_display_name: "cameron".to_string(),
                current_user_avatar_asset: Some("assets/avatars/me.svg".to_string()),
            },
            workspace: WorkspaceModel {
                workspace_id: acme_id.clone(),
                workspace_name: "cameron".to_string(),
                channels: vec![
                    general.clone(),
                    design.clone(),
                    hangout.clone(),
                    travel.clone(),
                    zbase_conv.clone(),
                    gpui_conv.clone(),
                ],
                direct_messages: vec![alice_dm.clone()],
            },
            navigation: NavigationModel {
                current: route.clone(),
                back_stack: Vec::new(),
                forward_stack: Vec::new(),
                right_pane: RightPaneMode::Thread,
            },
            sidebar: SidebarModel {
                sections: vec![
                    SidebarSection {
                        id: SidebarSectionId::new("team_acme"),
                        title: "Acme Product".to_string(),
                        rows: vec![
                            SidebarRow {
                                label: "general".to_string(),
                                unread_count: 3,
                                mention_count: 1,
                                route: Route::Channel {
                                    workspace_id: acme_id.clone(),
                                    channel_id: general_channel_id.clone(),
                                },
                            },
                            SidebarRow {
                                label: "design".to_string(),
                                unread_count: 0,
                                mention_count: 0,
                                route: Route::Channel {
                                    workspace_id: acme_id.clone(),
                                    channel_id: design_channel_id.clone(),
                                },
                            },
                        ],
                        collapsed: false,
                    },
                    SidebarSection {
                        id: SidebarSectionId::new("team_friends"),
                        title: "Friends".to_string(),
                        rows: vec![
                            SidebarRow {
                                label: "hangout".to_string(),
                                unread_count: 5,
                                mention_count: 0,
                                route: Route::Channel {
                                    workspace_id: friends_id.clone(),
                                    channel_id: hangout_channel_id.clone(),
                                },
                            },
                            SidebarRow {
                                label: "travel".to_string(),
                                unread_count: 0,
                                mention_count: 0,
                                route: Route::Channel {
                                    workspace_id: friends_id.clone(),
                                    channel_id: travel_channel_id.clone(),
                                },
                            },
                        ],
                        collapsed: false,
                    },
                    SidebarSection {
                        id: SidebarSectionId::new("team_oss"),
                        title: "Open Source".to_string(),
                        rows: vec![
                            SidebarRow {
                                label: "zbase".to_string(),
                                unread_count: 2,
                                mention_count: 1,
                                route: Route::Channel {
                                    workspace_id: oss_id.clone(),
                                    channel_id: zbase_channel_id.clone(),
                                },
                            },
                            SidebarRow {
                                label: "gpui".to_string(),
                                unread_count: 0,
                                mention_count: 0,
                                route: Route::Channel {
                                    workspace_id: oss_id.clone(),
                                    channel_id: gpui_channel_id.clone(),
                                },
                            },
                        ],
                        collapsed: false,
                    },
                    SidebarSection {
                        id: SidebarSectionId::new("dms"),
                        title: "DMs".to_string(),
                        rows: vec![SidebarRow {
                            label: "Alice Johnson".to_string(),
                            unread_count: 1,
                            mention_count: 0,
                            route: Route::DirectMessage {
                                workspace_id: acme_id.clone(),
                                dm_id: alice_dm_id.clone(),
                            },
                        }],
                        collapsed: false,
                    },
                ],
                filter: String::new(),
                highlighted_route: Some(route.clone()),
                width_px: crate::views::SIDEBAR_WIDTH_PX,
            },
            conversation: ConversationModel {
                summary: general.clone(),
                pinned_message: None,
                details: None,
                avatar_asset: None,
                member_count: 24,
                can_post: true,
                is_archived: false,
            },
            timeline: TimelineModel {
                conversation_id: general_conversation_id.clone(),
                current_user_id: Some(UserId::new("user_me")),
                rows: vec![
                    TimelineRow::DateDivider("Today".to_string()),
                    TimelineRow::UnreadDivider("3 unread messages".to_string()),
                    TimelineRow::Message(MessageRow {
                        author: demo_alice(),
                        message: general_message,
                        show_header: true,
                    }),
                ],
                highlighted_message_id: Some(MessageId::new("msg_001")),
                editing_message_id: None,
                unread_marker: Some(MessageId::new("msg_001")),
                affinity_index: HashMap::new(),
                emoji_index: HashMap::new(),
                emoji_source_index: HashMap::new(),
                reaction_index: HashMap::new(),
                author_role_index: HashMap::new(),
                pending_scroll_target: None,
                older_cursor: Some("cursor_older".to_string()),
                newer_cursor: None,
                loading_older: false,
                hovered_message_id: None,
                hovered_message_is_thread: None,
                hovered_message_anchor_x: None,
                hovered_message_anchor_y: None,
                hovered_message_window_left: None,
                hovered_message_window_top: None,
                hovered_message_window_width: None,
                hover_toolbar_settled: false,
                typing_text: Some("Sam is typing…".to_string()),
                quick_react_recent: None,
            },
            composer,
            quick_switcher: QuickSwitcherModel::default(),
            emoji_picker: EmojiPickerModel {
                query: String::new(),
                selected_skin_tone: skin_tone_from_setting_value(
                    settings.emoji_skin_tone.as_deref(),
                ),
                active_group: None,
                hovered: None,
                recent_aliases: settings.emoji_recents.clone(),
                skin_tone_expanded: false,
            },
            find_in_chat: FindInChatModel::default(),
            search: SearchModel {
                query: search_query.clone(),
                filters: Vec::new(),
                results: demo_search_results(&search_query, &[], &acme_id),
                highlighted_index: Some(0),
                is_loading: false,
            },
            new_chat: NewChatModel::default(),
            thread_pane,
            profile_panel: ProfilePanelModel::default(),
            overlay: OverlayModel {
                new_chat_open: false,
                quick_switcher_open: false,
                command_palette_open: false,
                emoji_picker_open: false,
                reaction_target_message_id: None,
                fullscreen_image: None,
                file_upload_lightbox: None,
                active_modal: None,
                profile_card_user_id: None,
                profile_card_position: None,
                sidebar_hover_tooltip: None,
                reaction_hover_tooltip: None,
            },
            call: CallModel {
                active_call: None,
                is_muted: false,
                is_sharing_screen: false,
            },
            notifications: NotificationsModel {
                toasts: Vec::new(),
                notification_center_count: 2,
                activity_items: demo_activity_items(&acme_id),
                highlighted_index: Some(0),
            },
            settings,
            quick_switcher_profile_names: HashMap::new(),
            quick_switcher_search_corpus: Arc::new(QuickSwitcherSearchCorpus {
                revision: 0,
                entries: Vec::new(),
                channel_entry_indices: Vec::new(),
                dm_entry_indices: Vec::new(),
            }),
            quick_switcher_search_corpus_revision: 0,
            quick_switcher_search_corpus_dirty: false,
            composer_drafts: HashMap::new(),
            thread_reply_drafts: HashMap::new(),
            pending_send_outcomes: HashMap::new(),
            send_attempt_count: 0,
        };

        models.store_current_composer_draft();
        models.store_current_thread_draft();
        models.refresh_notification_counts();
        models.apply_saved_sidebar_order();
        models.rebuild_quick_switcher_local_search_corpus();
        models
    }

    pub fn navigate_to(&mut self, route: Route) {
        self.store_current_drafts();
        self.navigation.navigate(route.clone());
        self.sync_route_models(&route);
        self.mark_route_seen(&route, None);
    }

    pub fn navigate_back(&mut self) -> bool {
        self.store_current_drafts();
        let Some(route) = self.navigation.back() else {
            return false;
        };
        self.sync_route_models(&route);
        self.mark_route_seen(&route, None);
        true
    }

    pub fn navigate_forward(&mut self) -> bool {
        self.store_current_drafts();
        let Some(route) = self.navigation.forward() else {
            return false;
        };
        self.sync_route_models(&route);
        self.mark_route_seen(&route, None);
        true
    }

    pub fn toggle_right_pane(&mut self) {
        let pane = if self.navigation.right_pane == RightPaneMode::Hidden {
            RightPaneMode::Thread
        } else {
            RightPaneMode::Hidden
        };
        self.set_right_pane(pane);
    }

    pub fn set_right_pane(&mut self, mut pane: RightPaneMode) {
        if pane != RightPaneMode::Hidden && !self.settings.show_right_pane {
            pane = RightPaneMode::Hidden;
        }

        self.navigation.set_right_pane(pane.clone());
        self.thread_pane.open = matches!(pane, RightPaneMode::Thread);
    }

    pub fn open_thread(&mut self, root_id: MessageId) {
        self.store_current_thread_draft();
        if self.thread_pane.root_message_id.as_ref() != Some(&root_id) {
            self.thread_pane.replies.clear();
        }
        self.thread_pane.root_message_id = Some(root_id.clone());
        if let Some(saved) = self.thread_reply_drafts.get(&root_id).cloned() {
            self.thread_pane.reply_draft = saved.text;
            self.thread_pane.reply_attachments = saved.attachments;
            self.thread_pane.reply_autocomplete = saved.autocomplete;
        } else {
            self.thread_pane.reply_draft.clear();
            self.thread_pane.reply_attachments.clear();
            self.thread_pane.reply_autocomplete = None;
        }
        self.thread_pane.following = true;
        self.thread_pane.loading = true;
        self.set_right_pane(RightPaneMode::Thread);
        self.mark_route_seen(&self.navigation.current.clone(), Some(&root_id));
    }

    pub fn set_thread_width(&mut self, width_px: f32) {
        self.thread_pane.width_px = width_px.clamp(280.0, 520.0);
    }

    pub fn set_sidebar_width(&mut self, width_px: f32) {
        self.sidebar.width_px = width_px.clamp(160.0, 360.0);
    }

    pub fn update_quick_switcher_profile_names(
        &mut self,
        profiles: HashMap<String, String>,
    ) -> bool {
        if self.quick_switcher_profile_names == profiles {
            return false;
        }
        self.quick_switcher_profile_names = profiles;
        self.quick_switcher_search_corpus_dirty = true;
        true
    }

    pub fn upsert_quick_switcher_profile_name(
        &mut self,
        user_id: &UserId,
        display_name: &str,
    ) -> bool {
        let trimmed = display_name.trim();
        if trimmed.is_empty() {
            return false;
        }
        let key = user_id.0.to_ascii_lowercase();
        if self
            .quick_switcher_profile_names
            .get(&key)
            .is_some_and(|current| current == trimmed)
        {
            return false;
        }
        self.quick_switcher_profile_names
            .insert(key, trimmed.to_string());
        self.quick_switcher_search_corpus_dirty = true;
        true
    }

    pub fn rebuild_quick_switcher_local_search_corpus(&mut self) {
        self.quick_switcher_search_corpus_revision =
            self.quick_switcher_search_corpus_revision.wrapping_add(1);
        let revision = self.quick_switcher_search_corpus_revision;
        self.quick_switcher_search_corpus = Arc::new(build_quick_switcher_search_corpus(
            &self.workspace.channels,
            &self.workspace.direct_messages,
            &self.quick_switcher_profile_names,
            &self.app.active_workspace_id,
            revision,
        ));
        self.quick_switcher_search_corpus_dirty = false;
    }

    pub fn quick_switcher_local_search_snapshot(&self) -> Arc<QuickSwitcherSearchCorpus> {
        Arc::clone(&self.quick_switcher_search_corpus)
    }

    pub fn flush_quick_switcher_local_search_corpus_if_dirty(&mut self) -> bool {
        if !self.quick_switcher_search_corpus_dirty {
            return false;
        }
        self.rebuild_quick_switcher_local_search_corpus();
        true
    }

    pub fn update_quick_switcher_query(&mut self, query: String) {
        let _ = self.flush_quick_switcher_local_search_corpus_if_dirty();
        self.quick_switcher.query = query;
        self.quick_switcher.results.clear();
        self.quick_switcher.selected_index = 0;
        self.quick_switcher.loading_messages = false;

        let trimmed = self.quick_switcher.query.trim();
        if trimmed.is_empty() {
            let mut unread_conversation_ids = HashSet::new();

            let mut unread = self
                .workspace
                .channels
                .iter()
                .chain(self.workspace.direct_messages.iter())
                .filter(|summary| {
                    !summary.muted && (summary.unread_count > 0 || summary.mention_count > 0)
                })
                .cloned()
                .collect::<Vec<_>>();
            unread.sort_by(|left, right| {
                right
                    .mention_count
                    .cmp(&left.mention_count)
                    .then_with(|| right.unread_count.cmp(&left.unread_count))
                    .then_with(|| left.title.cmp(&right.title))
            });
            for summary in unread.into_iter().take(QUICK_SWITCHER_UNREAD_LIMIT) {
                unread_conversation_ids.insert(summary.id.clone());
                self.quick_switcher.results.push(QuickSwitcherResult {
                    label: quick_switcher_label_for_summary(
                        &summary,
                        &self.quick_switcher_profile_names,
                    ),
                    sublabel: quick_switcher_sublabel_for_summary(&summary),
                    kind: QuickSwitcherResultKind::UnreadChannel,
                    route: quick_switcher_route_for_summary(
                        &summary,
                        &self.app.active_workspace_id,
                    ),
                    conversation_id: summary.id,
                    message_id: None,
                    match_ranges: Vec::new(),
                });
            }

            let mut recent_results = Vec::new();
            let mut seen_conversation_ids = unread_conversation_ids;

            for route in self.navigation.back_stack.iter().rev() {
                if !matches!(route, Route::Channel { .. } | Route::DirectMessage { .. }) {
                    continue;
                }
                let Some(summary) = quick_switcher_summary_for_route(
                    route,
                    &self.workspace.channels,
                    &self.workspace.direct_messages,
                ) else {
                    continue;
                };
                if !seen_conversation_ids.insert(summary.id.clone()) {
                    continue;
                }
                recent_results.push(QuickSwitcherResult {
                    label: quick_switcher_label_for_summary(
                        summary,
                        &self.quick_switcher_profile_names,
                    ),
                    sublabel: Some("Last visited".to_string()),
                    kind: quick_switcher_result_kind_for_conversation_kind(summary.kind.clone()),
                    route: route.clone(),
                    conversation_id: summary.id.clone(),
                    message_id: None,
                    match_ranges: Vec::new(),
                });
                if recent_results.len() >= QUICK_SWITCHER_RECENT_LIMIT {
                    break;
                }
            }

            if recent_results.len() < QUICK_SWITCHER_RECENT_LIMIT {
                let current = &self.navigation.current;
                if matches!(current, Route::Channel { .. } | Route::DirectMessage { .. })
                    && let Some(summary) = quick_switcher_summary_for_route(
                        current,
                        &self.workspace.channels,
                        &self.workspace.direct_messages,
                    )
                    && seen_conversation_ids.insert(summary.id.clone())
                {
                    recent_results.push(QuickSwitcherResult {
                        label: quick_switcher_label_for_summary(
                            summary,
                            &self.quick_switcher_profile_names,
                        ),
                        sublabel: Some("Current channel".to_string()),
                        kind: quick_switcher_result_kind_for_conversation_kind(
                            summary.kind.clone(),
                        ),
                        route: current.clone(),
                        conversation_id: summary.id.clone(),
                        message_id: None,
                        match_ranges: Vec::new(),
                    });
                }
            }

            if recent_results.len() < QUICK_SWITCHER_RECENT_LIMIT {
                let mut persisted_affinity = self
                    .settings
                    .quick_switcher_affinity
                    .iter()
                    .map(|(conversation_id, (affinity, last_updated_ms))| {
                        (conversation_id.as_str(), *affinity, *last_updated_ms)
                    })
                    .collect::<Vec<_>>();
                persisted_affinity.sort_by(|left, right| {
                    right
                        .2
                        .cmp(&left.2)
                        .then_with(|| right.1.total_cmp(&left.1))
                        .then_with(|| left.0.cmp(right.0))
                });
                for (conversation_id, _affinity, _last_updated_ms) in persisted_affinity {
                    if recent_results.len() >= QUICK_SWITCHER_RECENT_LIMIT {
                        break;
                    }
                    let Some(summary) = self
                        .workspace
                        .channels
                        .iter()
                        .chain(self.workspace.direct_messages.iter())
                        .find(|summary| summary.id.0 == conversation_id)
                    else {
                        continue;
                    };
                    if !seen_conversation_ids.insert(summary.id.clone()) {
                        continue;
                    }
                    recent_results.push(QuickSwitcherResult {
                        label: quick_switcher_label_for_summary(
                            summary,
                            &self.quick_switcher_profile_names,
                        ),
                        sublabel: Some("Recent".to_string()),
                        kind: quick_switcher_result_kind_for_conversation_kind(
                            summary.kind.clone(),
                        ),
                        route: quick_switcher_route_for_summary(
                            summary,
                            &self.app.active_workspace_id,
                        ),
                        conversation_id: summary.id.clone(),
                        message_id: None,
                        match_ranges: Vec::new(),
                    });
                }
            }

            self.quick_switcher.results.extend(recent_results);
            return;
        }

        let local = compute_quick_switcher_local_results(
            trimmed,
            &self.quick_switcher_search_corpus,
            None,
            &[],
            None,
            Some(&self.settings.quick_switcher_affinity),
            now_unix_ms(),
        );
        self.quick_switcher.results = local.results;
    }

    pub fn move_quick_switcher_selection(&mut self, direction: isize) {
        if self.quick_switcher.results.is_empty() {
            self.quick_switcher.selected_index = 0;
            return;
        }
        let current = self.quick_switcher.selected_index as isize;
        let next =
            (current + direction).clamp(0, self.quick_switcher.results.len() as isize - 1) as usize;
        self.quick_switcher.selected_index = next;
    }

    pub fn quick_switcher_selected_result(&self) -> Option<&QuickSwitcherResult> {
        self.quick_switcher
            .results
            .get(self.quick_switcher.selected_index)
    }

    pub fn record_quick_switcher_selection_affinity(&mut self, conversation_id: &ConversationId) {
        self.record_quick_switcher_selection_affinity_at(conversation_id, now_unix_ms());
    }

    pub(crate) fn record_quick_switcher_selection_affinity_at(
        &mut self,
        conversation_id: &ConversationId,
        now_ms: i64,
    ) {
        let key = conversation_id.0.clone();
        let (existing_affinity, existing_updated_ms) = self
            .settings
            .quick_switcher_affinity
            .get(&key)
            .copied()
            .unwrap_or((0.0, now_ms));
        let decayed_affinity =
            quick_switcher_decayed_affinity(existing_affinity, existing_updated_ms, now_ms);
        self.settings
            .quick_switcher_affinity
            .insert(key, (decayed_affinity + 1.0, now_ms));
    }

    pub fn apply_quick_switcher_message_results(&mut self, results: &[SearchResult]) {
        const QUICK_SWITCHER_MESSAGE_LIMIT: usize = 14;
        let active_conversation_id = quick_switcher_active_conversation_id(
            &self.navigation.current,
            &self.workspace.channels,
            &self.workspace.direct_messages,
        );
        self.quick_switcher
            .results
            .retain(|result| result.kind != QuickSwitcherResultKind::Message);
        let mut seen = HashSet::new();
        let mut in_active_conversation = Vec::new();
        let mut outside_active_conversation = Vec::new();
        for result in results {
            let key = format!("{}|{}", result.conversation_id.0, result.message.id.0);
            if !seen.insert(key) {
                continue;
            }
            if active_conversation_id.as_ref() == Some(&result.conversation_id) {
                in_active_conversation.push(result);
            } else {
                outside_active_conversation.push(result);
            }
        }
        self.quick_switcher.results.extend(
            in_active_conversation
                .into_iter()
                .chain(outside_active_conversation)
                .take(QUICK_SWITCHER_MESSAGE_LIMIT)
                .map(|result| QuickSwitcherResult {
                    label: quick_switcher_message_label(
                        &result.conversation_id,
                        &self.workspace.channels,
                        &self.workspace.direct_messages,
                        &self.quick_switcher_profile_names,
                    ),
                    sublabel: Some(result.snippet.clone()),
                    kind: QuickSwitcherResultKind::Message,
                    route: result.route.clone(),
                    conversation_id: result.conversation_id.clone(),
                    message_id: Some(result.message.id.clone()),
                    match_ranges: result.snippet_highlight_ranges.clone(),
                }),
        );
        if self.quick_switcher.results.is_empty() {
            self.quick_switcher.selected_index = 0;
        } else {
            self.quick_switcher.selected_index = self
                .quick_switcher
                .selected_index
                .min(self.quick_switcher.results.len().saturating_sub(1));
        }
        self.quick_switcher.loading_messages = false;
    }

    pub fn resolve_deep_link(&self, deep_link: &str) -> Option<(Route, MessageId)> {
        let parsed = parse_keybase_chat_link(deep_link)?;
        let summary = match &parsed.channel {
            Some(channel) => self.workspace.channels.iter().find(|summary| {
                let Some(group) = summary.group.as_ref() else {
                    return false;
                };
                if !group.id.eq_ignore_ascii_case(&parsed.team) {
                    return false;
                }
                summary.title.eq_ignore_ascii_case(channel)
                    || summary.topic.eq_ignore_ascii_case(channel)
            }),
            None => self.workspace.direct_messages.iter().find(|summary| {
                normalize_tlf_name(&summary.topic) == normalize_tlf_name(&parsed.team)
            }),
        }?;
        let route = quick_switcher_route_for_summary(summary, &self.app.active_workspace_id);
        Some((route, MessageId::new(parsed.message_id)))
    }

    pub fn resolve_channel_mention(&self, channel_name: &str) -> Option<Route> {
        let current_summary = quick_switcher_summary_for_route(
            &self.navigation.current,
            &self.workspace.channels,
            &self.workspace.direct_messages,
        )?;
        let current_group = current_summary.group.as_ref()?;
        let target = self.workspace.channels.iter().find(|summary| {
            let Some(group) = summary.group.as_ref() else {
                return false;
            };
            group.id == current_group.id
                && (summary.title.eq_ignore_ascii_case(channel_name)
                    || summary.topic.eq_ignore_ascii_case(channel_name))
        })?;
        Some(quick_switcher_route_for_summary(
            target,
            &self.app.active_workspace_id,
        ))
    }

    pub fn sync_live_search_query(&mut self, query: String) {
        if self.search.query == query {
            return;
        }

        self.search.query = query;
        self.refresh_search_results();

        if let Route::Search {
            workspace_id,
            query: current_query,
        } = &mut self.navigation.current
        {
            *current_query = self.search.query.clone();
            self.search.results =
                demo_search_results(current_query, &self.search.filters, workspace_id);
            self.search.highlighted_index = (!self.search.results.is_empty()).then_some(0);
        }
    }

    pub fn refresh_search_results(&mut self) {
        self.search.results = demo_search_results(
            &self.search.query,
            &self.search.filters,
            &self.workspace.workspace_id,
        );
        self.search.highlighted_index = (!self.search.results.is_empty()).then_some(
            self.search
                .highlighted_index
                .unwrap_or(0)
                .min(self.search.results.len().saturating_sub(1)),
        );
    }

    pub fn toggle_search_filter(&mut self, filter: SearchFilter) {
        if let Some(index) = self
            .search
            .filters
            .iter()
            .position(|existing| *existing == filter)
        {
            self.search.filters.remove(index);
        } else {
            self.search.filters.push(filter);
        }
        self.refresh_search_results();
    }

    pub fn move_search_highlight(&mut self, direction: isize) {
        if self.search.results.is_empty() {
            self.search.highlighted_index = None;
            return;
        }

        let current = self.search.highlighted_index.unwrap_or(0) as isize;
        let next = (current + direction).clamp(0, self.search.results.len() as isize - 1);
        self.search.highlighted_index = Some(next as usize);
    }

    pub fn move_sidebar_highlight(&mut self, direction: isize) {
        let routes = self.visible_sidebar_routes();
        if routes.is_empty() {
            self.sidebar.highlighted_route = None;
            return;
        }

        let current = self
            .sidebar
            .highlighted_route
            .as_ref()
            .and_then(|route| routes.iter().position(|candidate| candidate == route))
            .unwrap_or(0) as isize;
        let next = (current + direction).clamp(0, routes.len() as isize - 1) as usize;
        self.sidebar.highlighted_route = Some(routes[next].clone());
    }

    pub fn move_activity_highlight(&mut self, direction: isize) {
        if self.notifications.activity_items.is_empty() {
            self.notifications.highlighted_index = None;
            return;
        }

        let current = self.notifications.highlighted_index.unwrap_or(0) as isize;
        let next = (current + direction)
            .clamp(0, self.notifications.activity_items.len() as isize - 1)
            as usize;
        self.notifications.highlighted_index = Some(next);
    }

    pub fn move_timeline_highlight(&mut self, direction: isize) {
        let message_ids = self.timeline_message_ids();
        if message_ids.is_empty() {
            self.timeline.highlighted_message_id = None;
            return;
        }

        let current = self
            .timeline
            .highlighted_message_id
            .as_ref()
            .and_then(|message_id| {
                message_ids
                    .iter()
                    .position(|candidate| candidate == message_id)
            })
            .unwrap_or_else(|| message_ids.len().saturating_sub(1)) as isize;
        let next = (current + direction).clamp(0, message_ids.len() as isize - 1) as usize;
        self.timeline.highlighted_message_id = Some(message_ids[next].clone());
    }

    pub fn highlight_timeline_message(&mut self, message_id: &MessageId) {
        if self.timeline.highlighted_message_id.as_ref() == Some(message_id) {
            self.timeline.highlighted_message_id = None;
        } else {
            self.timeline.highlighted_message_id = Some(message_id.clone());
        }
    }

    pub fn set_sidebar_filter(&mut self, filter: String) {
        self.sidebar.filter = filter;
    }

    pub fn toggle_sidebar_section(&mut self, section_id: &SidebarSectionId) {
        if let Some(section) = self
            .sidebar
            .sections
            .iter_mut()
            .find(|section| &section.id == section_id)
        {
            section.collapsed = !section.collapsed;
        }
    }

    pub fn reorder_sidebar_section(
        &mut self,
        dragged_id: &SidebarSectionId,
        target_id: &SidebarSectionId,
    ) {
        let Some(from) = self
            .sidebar
            .sections
            .iter()
            .position(|s| &s.id == dragged_id)
        else {
            return;
        };
        let Some(to) = self
            .sidebar
            .sections
            .iter()
            .position(|s| &s.id == target_id)
        else {
            return;
        };
        if from == to {
            return;
        }
        let section = self.sidebar.sections.remove(from);
        self.sidebar.sections.insert(to, section);
    }

    pub fn apply_saved_sidebar_order(&mut self) {
        let account_key = &self.app.active_workspace_id.0;
        if let Some(saved_order) = self.settings.sidebar_section_order.get(account_key) {
            let saved_order = saved_order.clone();
            self.sidebar.sections.sort_by(|a, b| {
                let pos_a = saved_order
                    .iter()
                    .position(|id| id == &a.id.0)
                    .unwrap_or(usize::MAX);
                let pos_b = saved_order
                    .iter()
                    .position(|id| id == &b.id.0)
                    .unwrap_or(usize::MAX);
                pos_a.cmp(&pos_b)
            });
        }
        if let Some(unread_idx) = self
            .sidebar
            .sections
            .iter()
            .position(|section| section.id.0 == "unread")
        {
            let unread = self.sidebar.sections.remove(unread_idx);
            self.sidebar.sections.insert(0, unread);
        }
        if let Some(collapsed_set) = self.settings.sidebar_collapsed_sections.get(account_key) {
            for section in &mut self.sidebar.sections {
                if section.id.0 == "unread" {
                    continue;
                }
                section.collapsed = collapsed_set.contains(&section.id.0);
            }
        }
    }

    pub fn expand_section_for_route(&mut self, route: &Route) {
        for section in &mut self.sidebar.sections {
            if section.rows.iter().any(|row| &row.route == route) {
                section.collapsed = false;
            }
        }
    }

    pub fn update_composer_draft_text(&mut self, text: String) {
        if self.composer.draft_text == text {
            return;
        }
        self.composer.draft_text = text;
        if let Some(snapshot) = self.composer_drafts.get_mut(&self.composer.conversation_id) {
            snapshot.text = self.composer.draft_text.clone();
        } else {
            self.store_current_composer_draft();
        }
    }

    pub fn update_thread_reply_draft(&mut self, text: String) {
        if self.thread_pane.reply_draft == text {
            return;
        }
        self.thread_pane.reply_draft = text;
        if let Some(root_id) = self.thread_pane.root_message_id.as_ref()
            && let Some(snapshot) = self.thread_reply_drafts.get_mut(root_id)
        {
            snapshot.text = self.thread_pane.reply_draft.clone();
        } else {
            self.store_current_thread_draft();
        }
    }

    pub fn open_file_upload_lightbox(&mut self, paths: Vec<PathBuf>, target: UploadTarget) -> bool {
        let mut candidates = Vec::new();
        for path in paths {
            let path_text = path.to_string_lossy().trim().to_string();
            if path_text.is_empty() {
                continue;
            }
            let filename = path
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| !name.trim().is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| path_text.clone());
            let size_bytes = std::fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
            let kind = attachment_kind_from_path(&path);
            let (width, height) = if matches!(kind, AttachmentKind::Image) {
                image::image_dimensions(&path)
                    .map(|(width, height)| (Some(width), Some(height)))
                    .unwrap_or((None, None))
            } else {
                (None, None)
            };
            candidates.push(FileUploadCandidate {
                path,
                filename,
                kind,
                size_bytes,
                width,
                height,
                caption: String::new(),
            });
        }
        if candidates.is_empty() {
            return false;
        }
        self.dismiss_overlays();
        self.overlay.file_upload_lightbox = Some(FileUploadLightboxModel {
            candidates,
            current_index: 0,
            target,
        });
        true
    }

    pub fn file_upload_update_caption(&mut self, caption: String) -> bool {
        let Some(lightbox) = self.overlay.file_upload_lightbox.as_mut() else {
            return false;
        };
        let Some(candidate) = lightbox.current_candidate_mut() else {
            return false;
        };
        if candidate.caption == caption {
            return false;
        }
        candidate.caption = caption;
        true
    }

    pub fn file_upload_next(&mut self) -> bool {
        let Some(lightbox) = self.overlay.file_upload_lightbox.as_mut() else {
            return false;
        };
        let next = lightbox.current_index.saturating_add(1);
        if next >= lightbox.candidates.len() {
            return false;
        }
        lightbox.current_index = next;
        true
    }

    pub fn file_upload_send_current(&mut self) -> bool {
        let Some(lightbox) = self.overlay.file_upload_lightbox.as_ref() else {
            return false;
        };
        if lightbox.current_candidate().is_none() {
            return false;
        }
        self.overlay.file_upload_lightbox = None;
        true
    }

    pub fn file_upload_send_all(&mut self) -> bool {
        let Some(lightbox) = self.overlay.file_upload_lightbox.as_ref() else {
            return false;
        };
        if lightbox.candidates.is_empty() {
            return false;
        }
        self.overlay.file_upload_lightbox = None;
        true
    }

    pub fn cancel_file_upload_lightbox(&mut self) -> bool {
        self.overlay.file_upload_lightbox.take().is_some()
    }

    pub fn add_composer_attachment(&mut self) {
        self.composer.attachments.push(next_demo_attachment(
            self.composer.attachments.len(),
            "composer",
        ));
        self.store_current_composer_draft();
    }

    pub fn remove_composer_attachment(&mut self, index: usize) {
        if index < self.composer.attachments.len() {
            self.composer.attachments.remove(index);
            self.store_current_composer_draft();
        }
    }

    pub fn set_composer_autocomplete(&mut self, autocomplete: Option<AutocompleteState>) {
        self.composer.autocomplete = autocomplete;
        self.store_current_composer_draft();
    }

    pub fn add_thread_attachment(&mut self) {
        self.thread_pane
            .reply_attachments
            .push(next_demo_attachment(
                self.thread_pane.reply_attachments.len(),
                "thread",
            ));
        self.store_current_thread_draft();
    }

    pub fn remove_thread_attachment(&mut self, index: usize) {
        if index < self.thread_pane.reply_attachments.len() {
            self.thread_pane.reply_attachments.remove(index);
            self.store_current_thread_draft();
        }
    }

    pub fn set_thread_autocomplete(&mut self, autocomplete: Option<AutocompleteState>) {
        self.thread_pane.reply_autocomplete = autocomplete;
        self.store_current_thread_draft();
    }

    pub fn mention_autocomplete_candidates(
        &self,
        query: &str,
        max_results: usize,
    ) -> Vec<AutocompleteCandidate> {
        let show_broadcast = matches!(self.conversation.summary.kind, ConversationKind::Channel);
        let Some(details) = self.conversation.details.as_ref() else {
            return if show_broadcast {
                mention_broadcast_candidates(query, max_results)
            } else {
                Vec::new()
            };
        };
        let prepared = prepare_fuzzy_query(query);
        let mut scored = Vec::new();
        for member in &details.members {
            let username = resolve_mention_username(
                &member.user_id.0,
                &member.display_name,
                &self.quick_switcher_profile_names,
            );
            let display_name = self
                .quick_switcher_profile_names
                .get(&username.to_ascii_lowercase())
                .cloned()
                .unwrap_or_else(|| member.display_name.clone());
            let score = if query.trim().is_empty() {
                Some(0)
            } else {
                let username_match = fuzzy_match_prepared(
                    prepared.as_ref().expect("checked non-empty query"),
                    &prepare_fuzzy_candidate(&username),
                )
                .map(|result| result.score);
                let display_match = fuzzy_match_prepared(
                    prepared.as_ref().expect("checked non-empty query"),
                    &prepare_fuzzy_candidate(&display_name),
                )
                .map(|result| result.score);
                username_match.into_iter().chain(display_match).max()
            };
            let Some(score) = score else {
                continue;
            };
            let affinity_rank = mention_affinity_rank(member.affinity);
            scored.push((
                score,
                affinity_rank,
                display_name.to_ascii_lowercase(),
                AutocompleteCandidate::MentionUser {
                    username,
                    display_name,
                    avatar_asset: member.avatar_asset.clone(),
                },
            ));
        }
        scored.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| right.1.cmp(&left.1))
                .then_with(|| left.2.cmp(&right.2))
        });
        let mut candidates = scored
            .into_iter()
            .map(|(_, _, _, candidate)| candidate)
            .collect::<Vec<_>>();
        if show_broadcast {
            candidates.extend(mention_broadcast_candidates(query, max_results));
        }
        candidates.truncate(max_results.max(1));
        candidates
    }

    pub fn emoji_autocomplete_candidates(
        query: &str,
        custom_items: &[EmojiPickerItem],
        max_results: usize,
    ) -> Vec<AutocompleteCandidate> {
        search_emoji_items(query, custom_items, max_results)
            .into_iter()
            .map(|item| match item {
                EmojiPickerItem::Stock(emoji) => {
                    let glyph = emoji.as_str().to_string();
                    let label = emoji
                        .shortcode()
                        .map(|shortcode| format!(":{shortcode}:"))
                        .unwrap_or_else(|| emoji.name().to_string());
                    AutocompleteCandidate::Emoji {
                        label,
                        insert_text: glyph.clone(),
                        glyph: Some(glyph),
                    }
                }
                EmojiPickerItem::Custom { alias, .. } => {
                    let label = format!(":{alias}:");
                    AutocompleteCandidate::Emoji {
                        label: label.clone(),
                        insert_text: label,
                        glyph: None,
                    }
                }
            })
            .collect()
    }

    pub fn edit_message(&mut self, message_id: &MessageId) -> bool {
        let Some(message) = self.find_message(message_id).cloned() else {
            return false;
        };

        self.composer.mode = ComposerMode::Edit {
            message_id: message.id.clone(),
        };
        self.timeline.editing_message_id = Some(message.id.clone());
        self.composer.draft_text = message
            .source_text
            .clone()
            .unwrap_or_else(|| flatten_fragments(&message.fragments));
        self.composer.attachments = message.attachments.clone();
        self.composer.autocomplete = None;
        self.store_current_composer_draft();
        true
    }

    pub fn cancel_edit(&mut self) -> bool {
        if !matches!(self.composer.mode, ComposerMode::Edit { .. }) {
            return false;
        }
        self.composer.mode = ComposerMode::Compose;
        self.timeline.editing_message_id = None;
        self.composer.draft_text.clear();
        self.composer.attachments.clear();
        self.composer.autocomplete = None;
        self.store_current_composer_draft();
        true
    }

    pub fn find_last_own_message_id(&self) -> Option<MessageId> {
        let current_user_id = self.current_user_id_lowercase()?;
        self.timeline.rows.iter().rev().find_map(|row| {
            let TimelineRow::Message(msg_row) = row else {
                return None;
            };
            if msg_row.message.author_id.0.to_ascii_lowercase() == current_user_id
                && msg_row.message.event.is_none()
            {
                Some(msg_row.message.id.clone())
            } else {
                None
            }
        })
    }

    fn current_user_id_lowercase(&self) -> Option<String> {
        self.app
            .current_user_id
            .as_ref()
            .map(|id| id.0.to_ascii_lowercase())
    }

    pub fn delete_message(&mut self, message_id: &MessageId) -> bool {
        let before_len = self.timeline.rows.len();
        self.timeline.rows.retain(
            |row| !matches!(row, TimelineRow::Message(message) if &message.message.id == message_id),
        );
        let deleted = self.timeline.rows.len() != before_len;

        if deleted {
            if self.thread_pane.root_message_id.as_ref() == Some(message_id) {
                self.thread_pane.root_message_id = None;
                self.thread_pane.replies.clear();
                self.thread_pane.reply_draft.clear();
                self.thread_pane.reply_attachments.clear();
                self.thread_pane.reply_autocomplete = None;
                self.set_right_pane(RightPaneMode::Hidden);
            }

            if matches!(
                self.composer.mode,
                ComposerMode::Edit { message_id: ref editing_id } if editing_id == message_id
            ) {
                self.composer.mode = ComposerMode::Compose;
                self.timeline.editing_message_id = None;
                self.composer.draft_text.clear();
                self.composer.attachments.clear();
                self.composer.autocomplete = None;
                self.store_current_composer_draft();
            }

            self.push_toast(
                "Message deleted",
                Some(ToastAction::OpenCurrentConversation),
            );
        }

        deleted
    }

    pub fn open_reaction_picker_for_message(&mut self, message_id: &MessageId) {
        self.overlay.new_chat_open = false;
        self.overlay.emoji_picker_open = true;
        self.overlay.reaction_target_message_id = Some(message_id.clone());
        self.overlay.fullscreen_image = None;
        self.overlay.file_upload_lightbox = None;
        self.overlay.active_modal = None;
        self.overlay.profile_card_user_id = None;
        self.overlay.profile_card_position = None;
        self.emoji_picker.query.clear();
        self.emoji_picker.active_group = None;
        self.emoji_picker.hovered = None;
        self.emoji_picker.skin_tone_expanded = false;
        self.sync_emoji_picker_from_settings();
    }

    pub fn set_emoji_picker_query(&mut self, query: String) {
        self.emoji_picker.query = query;
    }

    pub fn set_emoji_picker_active_group(&mut self, group: Option<emojis::Group>) {
        self.emoji_picker.active_group = group;
    }

    pub fn set_emoji_picker_hovered(&mut self, hovered: Option<EmojiPickerItem>) {
        self.emoji_picker.hovered = hovered;
    }

    pub fn set_emoji_picker_skin_tone_expanded(&mut self, expanded: bool) {
        self.emoji_picker.skin_tone_expanded = expanded;
    }

    pub fn set_emoji_picker_skin_tone(&mut self, tone: Option<emojis::SkinTone>) {
        self.emoji_picker.selected_skin_tone = tone;
        self.settings.emoji_skin_tone = skin_tone_to_setting_value(tone);
    }

    pub fn add_recent_emoji_alias(&mut self, alias: String) {
        push_recent_alias(&mut self.emoji_picker.recent_aliases, alias, 32);
        self.settings.emoji_recents = self.emoji_picker.recent_aliases.clone();
        self.refresh_quick_react_recent();
    }

    pub fn refresh_quick_react_recent(&mut self) {
        use crate::models::timeline_model::QuickReactRecent;
        use crate::views::timeline::QUICK_REACT_EMOJI;

        let mut builtin: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (alias, unicode) in QUICK_REACT_EMOJI {
            builtin.insert(alias.to_ascii_lowercase());
            builtin.insert(unicode.to_string());
        }

        let is_builtin = |alias: &str| {
            let bare = alias
                .strip_prefix(':')
                .and_then(|s| s.strip_suffix(':'))
                .unwrap_or(alias);
            let lower = bare.to_ascii_lowercase();
            if builtin.contains(&lower) {
                return true;
            }
            if builtin.contains(alias) {
                return true;
            }
            if let Some(stock) = emojis::get_by_shortcode(&lower) {
                builtin.contains(&stock.to_string())
            } else {
                false
            }
        };

        self.timeline.quick_react_recent = self
            .emoji_picker
            .recent_aliases
            .iter()
            .find(|alias| !is_builtin(alias))
            .and_then(|alias| {
                let bare = alias
                    .strip_prefix(':')
                    .and_then(|s| s.strip_suffix(':'))
                    .unwrap_or(alias);
                if let Some(render) = self.timeline.emoji_index.get(&bare.to_ascii_lowercase()) {
                    Some(QuickReactRecent {
                        alias: alias.clone(),
                        unicode: render.unicode.clone(),
                        asset_path: render.asset_path.clone(),
                    })
                } else if let Some(stock) = emojis::get_by_shortcode(bare) {
                    Some(QuickReactRecent {
                        alias: bare.to_string(),
                        unicode: Some(stock.to_string()),
                        asset_path: None,
                    })
                } else if emojis::get(alias).is_some() {
                    Some(QuickReactRecent {
                        alias: alias.clone(),
                        unicode: Some(alias.clone()),
                        asset_path: None,
                    })
                } else {
                    Some(QuickReactRecent {
                        alias: alias.clone(),
                        unicode: None,
                        asset_path: None,
                    })
                }
            });
    }

    pub fn react_to_message(&mut self, message_id: &MessageId, emoji: &str) {
        let reaction = emoji.trim();
        if reaction.is_empty() {
            return;
        }
        let current_user_id = self
            .current_user_id_lowercase()
            .unwrap_or_else(|| self.app.current_user_display_name.to_ascii_lowercase());
        let current_user_name = self.app.current_user_display_name.clone();
        let message_reactions = self
            .timeline
            .reaction_index
            .entry(message_id.clone())
            .or_default();
        let mut removed_reaction = false;
        if let Some(existing_index) = message_reactions
            .iter()
            .position(|entry| entry.emoji.eq_ignore_ascii_case(reaction))
        {
            let remove_entry;
            {
                let existing = &mut message_reactions[existing_index];
                if existing
                    .actors
                    .iter()
                    .any(|actor| actor.user_id.eq_ignore_ascii_case(&current_user_id))
                {
                    existing
                        .actors
                        .retain(|actor| !actor.user_id.eq_ignore_ascii_case(&current_user_id));
                    removed_reaction = true;
                } else {
                    existing
                        .actors
                        .push(crate::models::timeline_model::ReactionActorRender {
                            user_id: current_user_id.clone(),
                            display_name: current_user_name.clone(),
                        });
                }
                existing.count = existing.actors.len();
                existing.reacted_by_me = !removed_reaction;
                remove_entry = existing.actors.is_empty();
            }
            if remove_entry {
                message_reactions.remove(existing_index);
            }
        } else {
            message_reactions.push(crate::models::timeline_model::MessageReactionRender {
                emoji: reaction.to_string(),
                source_ref: None,
                count: 1,
                actors: vec![crate::models::timeline_model::ReactionActorRender {
                    user_id: current_user_id,
                    display_name: current_user_name,
                }],
                reacted_by_me: true,
            });
            message_reactions.sort_by(|left, right| left.emoji.cmp(&right.emoji));
        }
        self.overlay.reaction_target_message_id = None;
        self.overlay.emoji_picker_open = false;
        self.push_toast(
            if removed_reaction {
                "Removed reaction"
            } else {
                "Added reaction"
            },
            Some(ToastAction::OpenCurrentConversation),
        );
    }

    pub fn open_attachment_modal(&mut self, label: &str) {
        self.overlay.active_modal = Some(format!("Attach to {label}"));
    }

    pub fn open_image_lightbox(
        &mut self,
        source: AttachmentSource,
        caption: Option<String>,
        width: Option<u32>,
        height: Option<u32>,
    ) {
        self.dismiss_overlays();
        self.overlay.fullscreen_image = Some(FullscreenImageOverlay {
            source,
            caption: caption
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            width,
            height,
        });
    }

    pub fn toggle_quick_switcher(&mut self) {
        let next = !self.overlay.quick_switcher_open;
        self.dismiss_overlays();
        self.overlay.quick_switcher_open = next;
        if next {
            self.update_quick_switcher_query(String::new());
        }
    }

    pub fn toggle_command_palette(&mut self) {
        let next = !self.overlay.command_palette_open;
        self.dismiss_overlays();
        self.overlay.command_palette_open = next;
    }

    pub fn toggle_emoji_picker(&mut self) {
        let next = !self.overlay.emoji_picker_open;
        self.overlay.emoji_picker_open = next;
        if !next {
            self.overlay.reaction_target_message_id = None;
        }
        if next {
            self.overlay.new_chat_open = false;
            self.overlay.reaction_target_message_id = None;
            self.overlay.fullscreen_image = None;
            self.overlay.file_upload_lightbox = None;
            self.overlay.active_modal = None;
            self.overlay.profile_card_user_id = None;
            self.overlay.profile_card_position = None;
            self.emoji_picker.query.clear();
            self.emoji_picker.active_group = None;
            self.emoji_picker.hovered = None;
            self.emoji_picker.skin_tone_expanded = false;
            self.sync_emoji_picker_from_settings();
        }
    }

    pub fn show_profile_card(&mut self, user_id: UserId) {
        self.overlay.new_chat_open = false;
        self.overlay.profile_card_user_id = Some(user_id);
        self.overlay.profile_card_position = None;
        self.overlay.active_modal = None;
        self.overlay.reaction_target_message_id = None;
        self.overlay.emoji_picker_open = false;
    }

    pub fn hide_profile_card(&mut self) {
        self.overlay.profile_card_user_id = None;
        self.overlay.profile_card_position = None;
    }

    pub fn show_sidebar_hover_tooltip(
        &mut self,
        text: String,
        anchor_x: f32,
        anchor_y: f32,
        width_px: f32,
    ) {
        self.overlay.sidebar_hover_tooltip = Some(SidebarHoverTooltip {
            text,
            anchor_x,
            anchor_y,
            width_px,
        });
    }

    pub fn hide_sidebar_hover_tooltip(&mut self) {
        self.overlay.sidebar_hover_tooltip = None;
    }

    pub fn show_reaction_hover_tooltip(
        &mut self,
        text: String,
        anchor_x: f32,
        anchor_y: f32,
        width_px: f32,
    ) {
        self.overlay.reaction_hover_tooltip = Some(ReactionHoverTooltip {
            text,
            anchor_x,
            anchor_y,
            width_px,
        });
    }

    pub fn hide_reaction_hover_tooltip(&mut self) {
        self.overlay.reaction_hover_tooltip = None;
    }

    pub fn dismiss_overlays(&mut self) {
        self.overlay.new_chat_open = false;
        self.overlay.quick_switcher_open = false;
        self.overlay.command_palette_open = false;
        self.overlay.emoji_picker_open = false;
        self.overlay.reaction_target_message_id = None;
        self.overlay.fullscreen_image = None;
        self.overlay.file_upload_lightbox = None;
        self.overlay.active_modal = None;
        self.overlay.profile_card_user_id = None;
        self.overlay.profile_card_position = None;
        self.overlay.sidebar_hover_tooltip = None;
        self.overlay.reaction_hover_tooltip = None;
        self.quick_switcher.query.clear();
        self.quick_switcher.results.clear();
        self.quick_switcher.selected_index = 0;
        self.quick_switcher.loading_messages = false;
        self.emoji_picker.query.clear();
        self.emoji_picker.skin_tone_expanded = false;
        self.emoji_picker.hovered = None;
    }

    pub fn sync_emoji_picker_from_settings(&mut self) {
        self.emoji_picker.recent_aliases = self.settings.emoji_recents.clone();
        self.emoji_picker.selected_skin_tone =
            skin_tone_from_setting_value(self.settings.emoji_skin_tone.as_deref());
    }

    pub fn push_toast(&mut self, title: impl Into<String>, action: Option<ToastAction>) {
        self.notifications.toasts.push(ToastNotification {
            title: title.into(),
            action,
        });

        if self.notifications.toasts.len() > 3 {
            self.notifications.toasts.remove(0);
        }
    }

    pub fn dismiss_toast(&mut self, index: usize) {
        if index < self.notifications.toasts.len() {
            self.notifications.toasts.remove(index);
        }
    }

    pub fn send_composer_message(&mut self) -> (SendDispatch, Option<PendingSend>) {
        let draft = self.composer.draft_text.trim();
        if draft.is_empty() {
            return (SendDispatch::NotSent, None);
        }

        if let ComposerMode::Edit { message_id } = self.composer.mode.clone() {
            let attachments = self.composer.attachments.clone();
            let updated_text = draft.to_string();
            let updated_at_ms = now_unix_ms();

            if let Some(message) = self.find_message_mut(&message_id) {
                message.fragments = vec![MessageFragment::Text(updated_text.clone())];
                message.source_text = Some(updated_text);
                message.attachments = attachments;
                message.timestamp_ms = Some(updated_at_ms);
                message.edited = Some(EditMeta {
                    edit_id: message.id.clone(),
                    edited_at_ms: Some(updated_at_ms),
                });
            }

            self.composer.mode = ComposerMode::Compose;
            self.timeline.editing_message_id = None;
            self.composer.draft_text.clear();
            self.composer.attachments.clear();
            self.composer.autocomplete = None;
            self.store_current_composer_draft();
            self.push_toast("Message updated", Some(ToastAction::FocusComposer));
            return (SendDispatch::Immediate, None);
        }

        self.composer.mode = ComposerMode::Compose;
        self.composer.draft_text.clear();
        self.composer.attachments.clear();
        self.composer.autocomplete = None;
        self.store_current_composer_draft();
        (SendDispatch::Immediate, None)
    }

    pub fn toggle_thread_following(&mut self) -> bool {
        if self.thread_pane.root_message_id.is_none() {
            return false;
        }

        self.thread_pane.following = !self.thread_pane.following;
        self.push_toast(
            if self.thread_pane.following {
                "Thread notifications enabled"
            } else {
                "Thread notifications muted"
            },
            Some(ToastAction::OpenThread),
        );
        true
    }

    pub fn send_thread_reply(&mut self) -> (SendDispatch, Option<PendingSend>) {
        if self.thread_pane.root_message_id.is_none() {
            return (SendDispatch::NotSent, None);
        }

        let draft = self.thread_pane.reply_draft.trim();
        if draft.is_empty() {
            return (SendDispatch::NotSent, None);
        }

        self.thread_pane.reply_draft.clear();
        self.thread_pane.reply_attachments.clear();
        self.thread_pane.reply_autocomplete = None;
        self.thread_pane.following = true;
        self.store_current_thread_draft();
        (SendDispatch::Immediate, None)
    }

    pub fn complete_pending_send(&mut self, pending: &PendingSend) -> Option<bool> {
        match pending {
            PendingSend::TimelineMessage(message_id) => {
                let success = self.pending_send_outcomes.remove(message_id)?;
                let message = self.find_message_mut(message_id)?;
                message.send_state = if success {
                    MessageSendState::Sent
                } else {
                    MessageSendState::Failed
                };
                if success {
                    message.timestamp_ms = Some(now_unix_ms());
                }

                self.push_toast(
                    if success {
                        format!("Sent a message in {}", self.conversation.summary.title)
                    } else {
                        "Message failed to send".to_string()
                    },
                    Some(ToastAction::OpenCurrentConversation),
                );
                Some(success)
            }
        }
    }

    pub fn cycle_theme(&mut self) {
        self.settings.theme_mode = match self.settings.theme_mode {
            ThemeMode::Light => ThemeMode::Dark,
            ThemeMode::Dark => ThemeMode::System,
            ThemeMode::System => ThemeMode::Light,
        };
    }

    pub fn dismiss_current_pinned_banner(&mut self) -> bool {
        let conversation_id = self.conversation.summary.id.0.clone();
        let Some(pinned_item_id) = self
            .conversation
            .pinned_message
            .as_ref()
            .map(|item| item.id.clone())
        else {
            return false;
        };

        self.settings
            .dismissed_pinned_items
            .insert(conversation_id, pinned_item_id);
        self.conversation.pinned_message = None;
        true
    }

    pub fn open_activity_item(&mut self, index: usize) -> Option<(Route, Option<MessageId>)> {
        let item = self.notifications.activity_items.get_mut(index)?;
        item.unread = false;
        let route = item.route.clone();
        let message_id = item.message_id.clone();
        self.notifications.highlighted_index = Some(index);
        self.refresh_notification_counts();
        Some((route, message_id))
    }

    pub fn start_or_open_call(&mut self) -> Route {
        if let Some(route) = self.active_call_route() {
            return route;
        }

        let call_id = CallId::new(format!("call_{}", self.conversation.summary.title));
        self.call.active_call = Some(CallSessionSummary {
            call_id: call_id.clone(),
            status: if matches!(
                self.conversation.summary.kind,
                ConversationKind::DirectMessage
            ) {
                CallStatus::ActiveVideo
            } else {
                CallStatus::ActiveAudio
            },
            participants: active_call_participants(&self.conversation.summary),
        });
        self.call.is_muted = false;
        self.call.is_sharing_screen = false;
        self.push_toast("Call started", Some(ToastAction::OpenActiveCall));
        Route::ActiveCall {
            workspace_id: self.app.active_workspace_id.clone(),
            call_id,
        }
    }

    pub fn active_call_route(&self) -> Option<Route> {
        self.call
            .active_call
            .as_ref()
            .map(|session| Route::ActiveCall {
                workspace_id: self.app.active_workspace_id.clone(),
                call_id: session.call_id.clone(),
            })
    }

    pub fn leave_call(&mut self) {
        self.call.active_call = None;
        self.call.is_muted = false;
        self.call.is_sharing_screen = false;
        self.push_toast("Call ended", Some(ToastAction::OpenCurrentConversation));
    }

    pub fn toggle_call_mute(&mut self) {
        self.call.is_muted = !self.call.is_muted;
    }

    pub fn toggle_call_screen_share(&mut self) {
        self.call.is_sharing_screen = !self.call.is_sharing_screen;
        if let Some(call) = self.call.active_call.as_mut() {
            call.status = if self.call.is_sharing_screen {
                CallStatus::SharingScreen
            } else if matches!(
                self.conversation.summary.kind,
                ConversationKind::DirectMessage
            ) {
                CallStatus::ActiveVideo
            } else {
                CallStatus::ActiveAudio
            };
        }
    }

    pub fn cycle_call_status(&mut self) {
        let Some(call) = self.call.active_call.as_mut() else {
            return;
        };

        call.status = match call.status {
            CallStatus::Ringing => CallStatus::Connecting,
            CallStatus::Connecting => CallStatus::ActiveAudio,
            CallStatus::ActiveAudio => CallStatus::ActiveVideo,
            CallStatus::ActiveVideo => CallStatus::Reconnecting,
            CallStatus::Reconnecting => CallStatus::ActiveAudio,
            CallStatus::SharingScreen => CallStatus::Reconnecting,
            CallStatus::Idle => CallStatus::Ringing,
        };
    }

    fn sync_route_models(&mut self, route: &Route) {
        self.find_in_chat.close();
        match route {
            Route::Channel {
                channel_id,
                workspace_id,
                ..
            } => {
                self.app.active_workspace_id = workspace_id.clone();

                let (summary, member_count, timeline, composer_seed) = match channel_id.0.as_str() {
                    "design" => (
                        self.workspace
                            .channels
                            .iter()
                            .find(|c| c.title == "design")
                            .cloned()
                            .expect("missing design demo channel"),
                        12,
                        design_timeline(),
                        String::new(),
                    ),
                    "general" => (
                        self.workspace
                            .channels
                            .iter()
                            .find(|c| c.title == "general")
                            .cloned()
                            .expect("missing general demo channel"),
                        24,
                        general_timeline(),
                        String::new(),
                    ),
                    _ => {
                        let channel_name = channel_id.0.as_str();
                        let summary = self
                            .workspace
                            .channels
                            .iter()
                            .find(|c| c.title == channel_name)
                            .cloned()
                            .unwrap_or_else(|| ConversationSummary {
                                id: ConversationId::new(format!("conv_{channel_name}")),
                                title: channel_name.to_string(),
                                kind: ConversationKind::Channel,
                                topic: format!("#{channel_name}"),
                                group: None,
                                unread_count: 0,
                                mention_count: 0,
                                muted: false,
                                last_activity_ms: 0,
                            });
                        (summary, 8, general_timeline(), String::new())
                    }
                };

                self.conversation = ConversationModel {
                    summary: summary.clone(),
                    pinned_message: None,
                    details: None,
                    avatar_asset: None,
                    member_count,
                    can_post: true,
                    is_archived: false,
                };
                self.timeline = timeline;
                self.composer = self.restore_composer_for_conversation(&summary, composer_seed);
            }
            Route::DirectMessage { .. } => {
                let summary = self
                    .workspace
                    .direct_messages
                    .first()
                    .cloned()
                    .expect("missing demo dm");
                self.conversation = ConversationModel {
                    summary: summary.clone(),
                    pinned_message: None,
                    details: None,
                    avatar_asset: None,
                    member_count: 2,
                    can_post: true,
                    is_archived: false,
                };
                self.timeline = dm_timeline();
                self.composer = self.restore_composer_for_conversation(&summary, String::new());
            }
            Route::Search {
                query,
                workspace_id,
            } => {
                self.search.query = query.clone();
                self.search.results =
                    demo_search_results(query, &self.search.filters, workspace_id);
                self.search.highlighted_index = (!self.search.results.is_empty()).then_some(0);
                self.set_right_pane(RightPaneMode::Hidden);
            }
            Route::Activity { .. }
            | Route::Preferences
            | Route::WorkspaceHome { .. } => {
                self.set_right_pane(RightPaneMode::Hidden);
            }
            Route::ActiveCall { .. } => {
                self.set_right_pane(RightPaneMode::Hidden);
                if self.call.active_call.is_none() {
                    let _ = self.start_or_open_call();
                }
            }
        }

        self.sidebar.highlighted_route = self
            .visible_sidebar_routes()
            .into_iter()
            .find(|candidate| candidate == route)
            .or_else(|| self.sidebar.highlighted_route.clone());

        self.notifications.highlighted_index = if self.notifications.activity_items.is_empty() {
            None
        } else {
            Some(
                self.notifications
                    .highlighted_index
                    .unwrap_or(0)
                    .min(self.notifications.activity_items.len().saturating_sub(1)),
            )
        };

        if self.timeline.highlighted_message_id.is_none() {
            self.timeline.highlighted_message_id = self.default_timeline_highlight();
        }
    }

    fn restore_composer_for_conversation(
        &self,
        summary: &ConversationSummary,
        default_text: String,
    ) -> ComposerModel {
        let saved = self.composer_drafts.get(&summary.id).cloned();
        ComposerModel {
            conversation_id: summary.id.clone(),
            mode: saved
                .as_ref()
                .map(|snapshot| snapshot.mode.clone())
                .unwrap_or(ComposerMode::Compose),
            draft_text: saved
                .as_ref()
                .map(|snapshot| snapshot.text.clone())
                .unwrap_or(default_text),
            attachments: saved
                .as_ref()
                .map(|snapshot| snapshot.attachments.clone())
                .unwrap_or_default(),
            autocomplete: saved.and_then(|snapshot| snapshot.autocomplete),
        }
    }

    fn store_current_drafts(&mut self) {
        self.store_current_composer_draft();
        self.store_current_thread_draft();
    }

    fn store_current_composer_draft(&mut self) {
        self.composer_drafts.insert(
            self.composer.conversation_id.clone(),
            ComposerDraftSnapshot {
                text: self.composer.draft_text.clone(),
                attachments: self.composer.attachments.clone(),
                autocomplete: self.composer.autocomplete.clone(),
                mode: self.composer.mode.clone(),
            },
        );
    }

    fn store_current_thread_draft(&mut self) {
        let Some(root_id) = self.thread_pane.root_message_id.clone() else {
            return;
        };

        self.thread_reply_drafts.insert(
            root_id,
            ThreadReplyDraftSnapshot {
                text: self.thread_pane.reply_draft.clone(),
                attachments: self.thread_pane.reply_attachments.clone(),
                autocomplete: self.thread_pane.reply_autocomplete.clone(),
            },
        );
    }

    fn register_pending_send(&mut self, message_id: &MessageId) {
        self.send_attempt_count += 1;
        let will_succeed = !self.send_attempt_count.is_multiple_of(4);
        self.pending_send_outcomes
            .insert(message_id.clone(), will_succeed);
    }

    fn mark_route_seen(&mut self, route: &Route, message_id: Option<&MessageId>) {
        match route {
            Route::Channel { .. } | Route::DirectMessage { .. } => {
                if let Some(conversation_id) = conversation_id_for_route(route) {
                    self.clear_workspace_counts(&conversation_id);
                }
            }
            Route::Preferences => {}
            _ => {}
        }

        for item in &mut self.notifications.activity_items {
            let route_match = &item.route == route;
            let message_match = message_id
                .map(|message_id| item.message_id.as_ref() == Some(message_id))
                .unwrap_or(false);

            if route_match || message_match {
                item.unread = false;
            }
        }

        self.refresh_notification_counts();
    }

    fn clear_workspace_counts(&mut self, conversation_id: &ConversationId) {
        for summary in &mut self.workspace.channels {
            if &summary.id == conversation_id {
                summary.unread_count = 0;
                summary.mention_count = 0;
            }
        }

        for summary in &mut self.workspace.direct_messages {
            if &summary.id == conversation_id {
                summary.unread_count = 0;
                summary.mention_count = 0;
            }
        }

        for section in &mut self.sidebar.sections {
            for row in &mut section.rows {
                if conversation_id_for_route(&row.route).as_ref() == Some(conversation_id) {
                    row.unread_count = 0;
                    row.mention_count = 0;
                }
            }
        }
    }

    fn refresh_notification_counts(&mut self) {
        let unread_total = self
            .workspace
            .channels
            .iter()
            .chain(self.workspace.direct_messages.iter())
            .fold(0u64, |total, conversation| {
                total.saturating_add(u64::from(conversation.unread_count))
            });
        self.app.global_unread_count = unread_total.min(u64::from(u32::MAX)) as u32;

        self.notifications.notification_center_count = self
            .notifications
            .activity_items
            .iter()
            .filter(|item| item.unread)
            .count() as u32;

        self.notifications.highlighted_index = if self.notifications.activity_items.is_empty() {
            None
        } else {
            Some(
                self.notifications
                    .highlighted_index
                    .unwrap_or(0)
                    .min(self.notifications.activity_items.len().saturating_sub(1)),
            )
        };
    }

    fn find_message(&self, message_id: &MessageId) -> Option<&MessageRecord> {
        self.timeline.rows.iter().find_map(|row| {
            let TimelineRow::Message(message_row) = row else {
                return None;
            };
            (&message_row.message.id == message_id).then_some(&message_row.message)
        })
    }

    fn find_message_mut(&mut self, message_id: &MessageId) -> Option<&mut MessageRecord> {
        self.timeline.rows.iter_mut().find_map(|row| {
            let TimelineRow::Message(message_row) = row else {
                return None;
            };
            (&message_row.message.id == message_id).then_some(&mut message_row.message)
        })
    }

    fn visible_sidebar_routes(&self) -> Vec<Route> {
        let filter = self.sidebar.filter.trim().to_lowercase();
        self.sidebar
            .sections
            .iter()
            .filter(|section| !section.collapsed)
            .flat_map(|section| section.rows.iter())
            .filter(|row| {
                filter.is_empty()
                    || row.label.to_lowercase().contains(&filter)
                    || row.route.label().to_lowercase().contains(&filter)
            })
            .map(|row| row.route.clone())
            .collect()
    }

    fn timeline_message_ids(&self) -> Vec<MessageId> {
        self.timeline
            .rows
            .iter()
            .filter_map(|row| match row {
                TimelineRow::Message(message) => Some(message.message.id.clone()),
                _ => None,
            })
            .collect()
    }

    fn default_timeline_highlight(&self) -> Option<MessageId> {
        self.timeline_message_ids().into_iter().last()
    }
}

fn demo_alice() -> UserSummary {
    UserSummary {
        id: UserId::new("user_alice"),
        display_name: "Alice Johnson".to_string(),
        title: "Staff Product Designer".to_string(),
        avatar_asset: Some("assets/avatars/alice.svg".to_string()),
        presence: Presence {
            availability: Availability::Active,
            status_text: Some("Reviewing the desktop shell".to_string()),
        },
        affinity: Affinity::Positive,
    }
}

fn demo_sam() -> UserSummary {
    UserSummary {
        id: UserId::new("user_sam"),
        display_name: "Sam Rivera".to_string(),
        title: "Rust UI Engineer".to_string(),
        avatar_asset: Some("assets/avatars/sam.svg".to_string()),
        presence: Presence {
            availability: Availability::Active,
            status_text: Some("Profiling the timeline".to_string()),
        },
        affinity: Affinity::None,
    }
}

fn demo_me() -> UserSummary {
    UserSummary {
        id: UserId::new("user_me"),
        display_name: "You".to_string(),
        title: "Desktop Client Engineer".to_string(),
        avatar_asset: Some("assets/avatars/me.svg".to_string()),
        presence: Presence {
            availability: Availability::Active,
            status_text: Some("Driving the GPUI shell".to_string()),
        },
        affinity: Affinity::None,
    }
}

fn demo_search_results(
    query: &str,
    filters: &[SearchFilter],
    workspace_id: &WorkspaceId,
) -> Vec<SearchResult> {
    let results = vec![
        SearchResult {
            conversation_id: ConversationId::new("conv_general"),
            route: Route::Channel {
                workspace_id: workspace_id.clone(),
                channel_id: ChannelId::new("general"),
            },
            snippet: "Here is the shell layout for the GPUI port.".to_string(),
            snippet_highlight_ranges: Vec::new(),
            message: MessageRecord {
                id: MessageId::new("msg_001"),
                conversation_id: ConversationId::new("conv_general"),
                author_id: UserId::new("user_alice"),
                reply_to: None,
                thread_root_id: None,
                timestamp_ms: Some(1_712_000_000_000),
                event: None,
                link_previews: Vec::new(),
                permalink: "slack://acme/general/p/msg_001".to_string(),
                fragments: vec![
                    MessageFragment::Text(
                        "Here is the shell layout for the GPUI port.".to_string(),
                    ),
                    MessageFragment::Code {
                        text: "AppWindow -> BodySplit -> TimelineList".to_string(),
                        lang: None,
                    },
                ],
                source_text: None,
                attachments: vec![AttachmentSummary {
                    name: "slack-shell.png".to_string(),
                    kind: AttachmentKind::Image,
                    size_bytes: 128_000,
                    ..AttachmentSummary::default()
                }],
                reactions: Vec::new(),
                thread_reply_count: 4,
                send_state: MessageSendState::Sent,
                edited: None,
            },
        },
        SearchResult {
            conversation_id: ConversationId::new("conv_design"),
            route: Route::Channel {
                workspace_id: workspace_id.clone(),
                channel_id: ChannelId::new("design"),
            },
            snippet: "The shell feels right. Next step is focus routing.".to_string(),
            snippet_highlight_ranges: Vec::new(),
            message: MessageRecord {
                id: MessageId::new("msg_design_001"),
                conversation_id: ConversationId::new("conv_design"),
                author_id: UserId::new("user_sam"),
                reply_to: None,
                thread_root_id: None,
                timestamp_ms: Some(1_711_600_000_000),
                event: None,
                link_previews: Vec::new(),
                permalink: "slack://acme/design/p/msg_design_001".to_string(),
                fragments: vec![
                    MessageFragment::Text(
                        "The shell feels right. Next step is focus routing.".to_string(),
                    ),
                    MessageFragment::Quote(
                        "Keep the root view stateful and the panes presentational.".to_string(),
                    ),
                ],
                source_text: None,
                attachments: Vec::new(),
                reactions: Vec::new(),
                thread_reply_count: 2,
                send_state: MessageSendState::Sent,
                edited: None,
            },
        },
        SearchResult {
            conversation_id: ConversationId::new("conv_alice"),
            route: Route::DirectMessage {
                workspace_id: workspace_id.clone(),
                dm_id: DmId::new("alice"),
            },
            snippet: "Can you make the right pane toggles keyboard accessible too?".to_string(),
            snippet_highlight_ranges: Vec::new(),
            message: MessageRecord {
                id: MessageId::new("msg_dm_001"),
                conversation_id: ConversationId::new("conv_alice"),
                author_id: UserId::new("user_alice"),
                reply_to: None,
                thread_root_id: None,
                timestamp_ms: Some(1_712_200_000_000),
                event: None,
                link_previews: Vec::new(),
                permalink: "slack://acme/dm/alice/p/msg_dm_001".to_string(),
                fragments: vec![
                    MessageFragment::Text(
                        "Can you make the right pane toggles keyboard accessible too?".to_string(),
                    ),
                    MessageFragment::Text(
                        "I also dropped notes in https://docs.acme.dev/ui".to_string(),
                    ),
                ],
                source_text: None,
                attachments: Vec::new(),
                reactions: Vec::new(),
                thread_reply_count: 1,
                send_state: MessageSendState::Sent,
                edited: None,
            },
        },
    ];

    let query = query.trim().to_lowercase();

    results
        .into_iter()
        .filter(|result| {
            query.is_empty()
                || result.snippet.to_lowercase().contains(&query)
                || flatten_fragments(&result.message.fragments)
                    .to_lowercase()
                    .contains(&query)
        })
        .filter(|result| {
            filters
                .iter()
                .all(|filter| search_filter_matches(filter, result))
        })
        .collect()
}

fn demo_activity_items(workspace_id: &WorkspaceId) -> Vec<ActivityItem> {
    vec![
        ActivityItem {
            kind: ActivityKind::Mention,
            title: "Alice mentioned you in #general".to_string(),
            detail: "She asked for the GPUI shell layout so implementation can start.".to_string(),
            route: Route::Channel {
                workspace_id: workspace_id.clone(),
                channel_id: ChannelId::new("general"),
            },
            message_id: Some(MessageId::new("msg_001")),
            unread: true,
        },
        ActivityItem {
            kind: ActivityKind::ThreadReply,
            title: "New replies in the design thread".to_string(),
            detail: "Sam followed up on focus routing and pane ownership.".to_string(),
            route: Route::Channel {
                workspace_id: workspace_id.clone(),
                channel_id: ChannelId::new("design"),
            },
            message_id: Some(MessageId::new("msg_design_001")),
            unread: true,
        },
        ActivityItem {
            kind: ActivityKind::Reminder,
            title: "Preferences review".to_string(),
            detail: "Confirm whether the right pane should stay globally enabled.".to_string(),
            route: Route::Preferences,
            message_id: None,
            unread: false,
        },
        ActivityItem {
            kind: ActivityKind::Reaction,
            title: "Alice reacted in your DM".to_string(),
            detail: "She wants keyboard-accessible thread controls in the same pass.".to_string(),
            route: Route::DirectMessage {
                workspace_id: workspace_id.clone(),
                dm_id: DmId::new("alice"),
            },
            message_id: Some(MessageId::new("msg_dm_001")),
            unread: false,
        },
    ]
}

fn general_timeline() -> TimelineModel {
    let author = demo_alice();
    let conversation_id = ConversationId::new("conv_general");
    TimelineModel {
        conversation_id: conversation_id.clone(),
        current_user_id: Some(UserId::new("user_me")),
        rows: vec![
            TimelineRow::DateDivider("Today".to_string()),
            TimelineRow::UnreadDivider("3 unread messages".to_string()),
            TimelineRow::Message(MessageRow {
                author,
                message: MessageRecord {
                    id: MessageId::new("msg_001"),
                    conversation_id,
                    author_id: UserId::new("user_alice"),
                    reply_to: None,
                    thread_root_id: None,
                    timestamp_ms: Some(1_712_000_000_000),
                    event: None,
                    link_previews: Vec::new(),
                    permalink: "slack://acme/general/p/msg_001".to_string(),
                    fragments: vec![
                        MessageFragment::Text(
                            "Here is the shell layout for the GPUI port.".to_string(),
                        ),
                        MessageFragment::Code {
                            text: "AppWindow -> BodySplit -> TimelineList".to_string(),
                            lang: None,
                        },
                    ],
                    source_text: None,
                    attachments: vec![AttachmentSummary {
                        name: "slack-shell.png".to_string(),
                        kind: AttachmentKind::Image,
                        size_bytes: 128_000,
                        ..AttachmentSummary::default()
                    }],
                    reactions: Vec::new(),
                    thread_reply_count: 4,
                    send_state: MessageSendState::Sent,
                    edited: None,
                },
                show_header: true,
            }),
        ],
        highlighted_message_id: Some(MessageId::new("msg_001")),
        editing_message_id: None,
        unread_marker: Some(MessageId::new("msg_001")),
        affinity_index: HashMap::new(),
        emoji_index: HashMap::new(),
        emoji_source_index: HashMap::new(),
        reaction_index: HashMap::new(),
        author_role_index: HashMap::new(),
        pending_scroll_target: None,
        older_cursor: Some("cursor_older".to_string()),
        newer_cursor: None,
        loading_older: false,
        hovered_message_id: None,
        hovered_message_is_thread: None,
        hovered_message_anchor_x: None,
        hovered_message_anchor_y: None,
        hovered_message_window_left: None,
        hovered_message_window_top: None,
        hovered_message_window_width: None,
        hover_toolbar_settled: false,
        typing_text: Some("Sam is typing…".to_string()),
        quick_react_recent: None,
    }
}

fn design_timeline() -> TimelineModel {
    let conversation_id = ConversationId::new("conv_design");
    TimelineModel {
        conversation_id: conversation_id.clone(),
        current_user_id: Some(UserId::new("user_me")),
        rows: vec![
            TimelineRow::DateDivider("Yesterday".to_string()),
            TimelineRow::Message(MessageRow {
                author: demo_sam(),
                message: MessageRecord {
                    id: MessageId::new("msg_design_001"),
                    conversation_id: conversation_id.clone(),
                    author_id: UserId::new("user_sam"),
                    reply_to: None,
                    thread_root_id: None,
                    timestamp_ms: Some(1_711_600_000_000),
                    event: None,
                    link_previews: Vec::new(),
                    permalink: "slack://acme/design/p/msg_design_001".to_string(),
                    fragments: vec![
                        MessageFragment::Text(
                            "The shell feels right. Next step is focus routing.".to_string(),
                        ),
                        MessageFragment::Quote(
                            "Keep the root view stateful and the panes presentational.".to_string(),
                        ),
                    ],
                    source_text: None,
                    attachments: Vec::new(),
                    reactions: Vec::new(),
                    thread_reply_count: 2,
                    send_state: MessageSendState::Sent,
                    edited: None,
                },
                show_header: true,
            }),
            TimelineRow::Message(MessageRow {
                author: demo_sam(),
                message: MessageRecord {
                    id: MessageId::new("msg_design_002"),
                    conversation_id,
                    author_id: UserId::new("user_sam"),
                    reply_to: None,
                    thread_root_id: None,
                    timestamp_ms: Some(1_711_601_000_000),
                    event: None,
                    link_previews: Vec::new(),
                    permalink: "slack://acme/design/p/msg_design_002".to_string(),
                    fragments: vec![MessageFragment::Text(
                        "We should add a command layer before building the real composer."
                            .to_string(),
                    )],
                    source_text: None,
                    attachments: Vec::new(),
                    reactions: Vec::new(),
                    thread_reply_count: 0,
                    send_state: MessageSendState::Sent,
                    edited: None,
                },
                show_header: false,
            }),
        ],
        highlighted_message_id: Some(MessageId::new("msg_design_002")),
        editing_message_id: None,
        unread_marker: None,
        affinity_index: HashMap::new(),
        emoji_index: HashMap::new(),
        emoji_source_index: HashMap::new(),
        reaction_index: HashMap::new(),
        author_role_index: HashMap::new(),
        pending_scroll_target: None,
        older_cursor: Some("cursor_design_older".to_string()),
        newer_cursor: None,
        loading_older: false,
        hovered_message_id: None,
        hovered_message_is_thread: None,
        hovered_message_anchor_x: None,
        hovered_message_anchor_y: None,
        hovered_message_window_left: None,
        hovered_message_window_top: None,
        hovered_message_window_width: None,
        hover_toolbar_settled: false,
        typing_text: None,
        quick_react_recent: None,
    }
}

fn dm_timeline() -> TimelineModel {
    let conversation_id = ConversationId::new("conv_alice");
    TimelineModel {
        conversation_id: conversation_id.clone(),
        current_user_id: Some(UserId::new("user_me")),
        rows: vec![
            TimelineRow::DateDivider("Today".to_string()),
            TimelineRow::Message(MessageRow {
                author: demo_alice(),
                message: MessageRecord {
                    id: MessageId::new("msg_dm_001"),
                    conversation_id,
                    author_id: UserId::new("user_alice"),
                    reply_to: None,
                    thread_root_id: None,
                    timestamp_ms: Some(1_712_200_000_000),
                    event: None,
                    link_previews: Vec::new(),
                    permalink: "slack://acme/dm/alice/p/msg_dm_001".to_string(),
                    fragments: vec![MessageFragment::Text(
                        "Can you make the right pane toggles keyboard accessible too?".to_string(),
                    )],
                    source_text: None,
                    attachments: Vec::new(),
                    reactions: Vec::new(),
                    thread_reply_count: 1,
                    send_state: MessageSendState::Sent,
                    edited: None,
                },
                show_header: true,
            }),
        ],
        highlighted_message_id: Some(MessageId::new("msg_dm_001")),
        editing_message_id: None,
        unread_marker: None,
        affinity_index: HashMap::new(),
        emoji_index: HashMap::new(),
        emoji_source_index: HashMap::new(),
        reaction_index: HashMap::new(),
        author_role_index: HashMap::new(),
        pending_scroll_target: None,
        older_cursor: None,
        newer_cursor: None,
        loading_older: false,
        hovered_message_id: None,
        hovered_message_is_thread: None,
        hovered_message_anchor_x: None,
        hovered_message_anchor_y: None,
        hovered_message_window_left: None,
        hovered_message_window_top: None,
        hovered_message_window_width: None,
        hover_toolbar_settled: false,
        typing_text: None,
        quick_react_recent: None,
    }
}

fn flatten_fragments(fragments: &[MessageFragment]) -> String {
    fragments
        .iter()
        .map(|fragment| match fragment {
            MessageFragment::Text(text)
            | MessageFragment::Code { text, .. }
            | MessageFragment::Quote(text) => text.clone(),
            MessageFragment::InlineCode(text) => format!("`{text}`"),
            MessageFragment::Emoji { alias, .. } => format!(":{alias}:"),
            MessageFragment::Mention(user_id) => format!("@{}", user_id.0),
            MessageFragment::ChannelMention { name } => format!("#{name}"),
            MessageFragment::BroadcastMention(BroadcastKind::Here) => "@here".to_string(),
            MessageFragment::BroadcastMention(BroadcastKind::All) => "@channel".to_string(),
            MessageFragment::Link { display, .. } => display.clone(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn search_filter_matches(filter: &SearchFilter, result: &SearchResult) -> bool {
    match filter {
        SearchFilter::FromUser(user) => {
            let author = match result.message.author_id.0.as_str() {
                "user_alice" => "alice",
                "user_sam" => "sam",
                "user_me" => "you",
                other => other,
            };
            author.contains(&user.to_lowercase())
        }
        SearchFilter::InChannel(channel) => match &result.route {
            Route::Channel { channel_id, .. } => channel_id.0.contains(&channel.to_lowercase()),
            Route::DirectMessage { .. } => channel == "dm",
            _ => false,
        },
        SearchFilter::HasFile => !result.message.attachments.is_empty(),
        SearchFilter::HasLink => flatten_fragments(&result.message.fragments).contains("http"),
        SearchFilter::MentionsMe => flatten_fragments(&result.message.fragments)
            .to_lowercase()
            .contains("you"),
    }
}

fn next_demo_attachment(index: usize, prefix: &str) -> AttachmentSummary {
    let (name, kind, size_bytes) = match index % 3 {
        0 => (format!("{prefix}-notes.md"), AttachmentKind::File, 16_500),
        1 => (format!("{prefix}-mock.png"), AttachmentKind::Image, 248_000),
        _ => (
            format!("{prefix}-clip.mov"),
            AttachmentKind::Video,
            3_200_000,
        ),
    };

    AttachmentSummary {
        name,
        kind,
        size_bytes,
        ..AttachmentSummary::default()
    }
}

fn quick_switcher_label_for_summary(
    summary: &ConversationSummary,
    profile_names: &HashMap<String, String>,
) -> String {
    match summary.kind {
        ConversationKind::Channel => {
            if let Some(group) = summary.group.as_ref() {
                format!("{} #{}", group.display_name, summary.title)
            } else {
                format!("#{}", summary.title)
            }
        }
        ConversationKind::DirectMessage | ConversationKind::GroupDirectMessage => {
            let usernames = quick_switcher_dm_usernames(&summary.title);
            if usernames.is_empty() {
                return summary.title.clone();
            }
            let display_names = quick_switcher_dm_display_names(&usernames, profile_names);
            display_names.join(", ")
        }
    }
}

fn quick_switcher_match_for_dm(
    query: &str,
    raw_title: &str,
    display_label: &str,
    profile_names: &HashMap<String, String>,
) -> Option<FuzzyMatch> {
    let candidates = quick_switcher_dm_match_candidates(raw_title, display_label, profile_names);
    let prepared_query = prepare_fuzzy_query(query)?;
    let mut rejected = 0usize;
    let mut fuzzy_evaluated = 0usize;
    quick_switcher_match_for_candidates_prepared(
        &prepared_query,
        &candidates,
        &mut rejected,
        &mut fuzzy_evaluated,
    )
}

fn quick_switcher_match_candidate(
    text: String,
    highlightable: bool,
) -> QuickSwitcherMatchCandidate {
    let prepared = prepare_fuzzy_candidate(&text);
    QuickSwitcherMatchCandidate {
        text,
        highlightable,
        prepared,
    }
}

fn quick_switcher_dm_match_candidates(
    raw_title: &str,
    display_label: &str,
    profile_names: &HashMap<String, String>,
) -> Vec<QuickSwitcherMatchCandidate> {
    let usernames = quick_switcher_dm_usernames(raw_title);
    if usernames.is_empty() {
        return vec![quick_switcher_match_candidate(
            display_label.to_string(),
            true,
        )];
    }
    let display_names = quick_switcher_dm_display_names(&usernames, profile_names);
    let username_label = usernames.join(", ");
    let mut candidates = Vec::new();
    candidates.push(quick_switcher_match_candidate(
        display_label.to_string(),
        true,
    ));
    if username_label != display_label {
        candidates.push(quick_switcher_match_candidate(username_label, false));
    }
    candidates.extend(
        quick_switcher_permutation_labels(&display_names)
            .into_iter()
            .map(|text| quick_switcher_match_candidate(text, false)),
    );
    candidates.extend(
        quick_switcher_permutation_labels(&usernames)
            .into_iter()
            .map(|text| quick_switcher_match_candidate(text, false)),
    );
    dedupe_quick_switcher_match_candidates(candidates)
}

fn dedupe_quick_switcher_match_candidates(
    candidates: Vec<QuickSwitcherMatchCandidate>,
) -> Vec<QuickSwitcherMatchCandidate> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for candidate in candidates {
        if seen.insert(candidate.text.clone()) {
            deduped.push(candidate);
        }
    }
    deduped
}

fn quick_switcher_match_for_candidates_prepared(
    query: &PreparedFuzzyQuery,
    candidates: &[QuickSwitcherMatchCandidate],
    rejected_candidates: &mut usize,
    fuzzy_evaluated: &mut usize,
) -> Option<FuzzyMatch> {
    let mut best: Option<FuzzyMatch> = None;
    for candidate in candidates {
        if candidate.prepared.char_len < query.char_len {
            *rejected_candidates = rejected_candidates.saturating_add(1);
            continue;
        }
        if query.char_mask & !candidate.prepared.char_mask != 0 {
            *rejected_candidates = rejected_candidates.saturating_add(1);
            continue;
        }
        *fuzzy_evaluated = fuzzy_evaluated.saturating_add(1);
        let Some(mut matched) = fuzzy_match_prepared(query, &candidate.prepared) else {
            continue;
        };
        if !candidate.highlightable {
            matched.ranges.clear();
        }
        match best.as_ref() {
            Some(existing) if existing.score >= matched.score => {}
            _ => best = Some(matched),
        }
    }
    best
}

fn quick_switcher_dm_usernames(raw_title: &str) -> Vec<String> {
    raw_title
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn quick_switcher_dm_participant_count(raw_title: &str) -> usize {
    quick_switcher_dm_usernames(raw_title).len().max(1)
}

fn quick_switcher_dm_display_names(
    usernames: &[String],
    profile_names: &HashMap<String, String>,
) -> Vec<String> {
    usernames
        .iter()
        .map(|username| {
            profile_names
                .get(&username.to_ascii_lowercase())
                .cloned()
                .unwrap_or_else(|| username.clone())
        })
        .collect()
}

fn quick_switcher_permutation_labels(parts: &[String]) -> Vec<String> {
    if parts.len() <= 1 {
        return Vec::new();
    }
    let original = parts.join(", ");
    let mut labels = Vec::new();
    let mut reversed = parts.to_vec();
    reversed.reverse();
    let reversed_label = reversed.join(", ");
    if reversed_label != original {
        labels.push(reversed_label);
    }
    let mut sorted = parts.to_vec();
    sorted.sort_by_key(|value| value.to_ascii_lowercase());
    let sorted_label = sorted.join(", ");
    if sorted_label != original && !labels.contains(&sorted_label) {
        labels.push(sorted_label);
    }
    labels
}

fn quick_switcher_sublabel_for_summary(summary: &ConversationSummary) -> Option<String> {
    match summary.kind {
        ConversationKind::Channel => summary
            .group
            .as_ref()
            .map(|group| group.id.clone())
            .or_else(|| (!summary.topic.trim().is_empty()).then(|| summary.topic.clone())),
        ConversationKind::DirectMessage | ConversationKind::GroupDirectMessage => {
            (!summary.topic.trim().is_empty()).then(|| summary.topic.clone())
        }
    }
}

fn normalize_tlf_name(tlf: &str) -> String {
    let mut parts: Vec<&str> = tlf
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    parts.sort_unstable_by(|a, b| a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase()));
    parts.join(",")
}

fn quick_switcher_route_for_summary(
    summary: &ConversationSummary,
    workspace_id: &WorkspaceId,
) -> Route {
    match summary.kind {
        ConversationKind::Channel => Route::Channel {
            workspace_id: workspace_id.clone(),
            channel_id: ChannelId::new(summary.id.0.clone()),
        },
        ConversationKind::DirectMessage | ConversationKind::GroupDirectMessage => {
            Route::DirectMessage {
                workspace_id: workspace_id.clone(),
                dm_id: DmId::new(summary.id.0.clone()),
            }
        }
    }
}

fn quick_switcher_result_kind_for_conversation_kind(
    kind: ConversationKind,
) -> QuickSwitcherResultKind {
    match kind {
        ConversationKind::Channel => QuickSwitcherResultKind::Channel,
        ConversationKind::DirectMessage | ConversationKind::GroupDirectMessage => {
            QuickSwitcherResultKind::DirectMessage
        }
    }
}

fn quick_switcher_summary_for_route<'a>(
    route: &Route,
    channels: &'a [ConversationSummary],
    direct_messages: &'a [ConversationSummary],
) -> Option<&'a ConversationSummary> {
    match route {
        Route::Channel { channel_id, .. } => channels
            .iter()
            .find(|summary| summary.id.0 == channel_id.0 || summary.title == channel_id.0),
        Route::DirectMessage { dm_id, .. } => direct_messages
            .iter()
            .find(|summary| summary.id.0 == dm_id.0 || summary.title == dm_id.0),
        _ => None,
    }
}

fn quick_switcher_active_conversation_id(
    route: &Route,
    channels: &[ConversationSummary],
    direct_messages: &[ConversationSummary],
) -> Option<ConversationId> {
    quick_switcher_summary_for_route(route, channels, direct_messages)
        .map(|summary| summary.id.clone())
}

fn build_quick_switcher_search_corpus(
    channels: &[ConversationSummary],
    direct_messages: &[ConversationSummary],
    profile_names: &HashMap<String, String>,
    workspace_id: &WorkspaceId,
    revision: u64,
) -> QuickSwitcherSearchCorpus {
    let mut entries = Vec::with_capacity(channels.len() + direct_messages.len());
    let mut channel_entry_indices = Vec::with_capacity(channels.len());
    let mut dm_entry_indices = Vec::with_capacity(direct_messages.len());

    for summary in channels {
        let label = quick_switcher_label_for_summary(summary, profile_names);
        let entry = QuickSwitcherSearchEntry {
            conversation_id: summary.id.clone(),
            label: label.clone(),
            sublabel: quick_switcher_sublabel_for_summary(summary),
            kind: quick_switcher_result_kind_for_conversation_kind(summary.kind.clone()),
            route: quick_switcher_route_for_summary(summary, workspace_id),
            unread_count: summary.unread_count,
            mention_count: summary.mention_count,
            dm_participant_count: None,
            match_candidates: vec![quick_switcher_match_candidate(label, true)],
        };
        channel_entry_indices.push(entries.len());
        entries.push(entry);
    }

    for summary in direct_messages {
        let label = quick_switcher_label_for_summary(summary, profile_names);
        let entry = QuickSwitcherSearchEntry {
            conversation_id: summary.id.clone(),
            label: label.clone(),
            sublabel: quick_switcher_sublabel_for_summary(summary),
            kind: quick_switcher_result_kind_for_conversation_kind(summary.kind.clone()),
            route: quick_switcher_route_for_summary(summary, workspace_id),
            unread_count: summary.unread_count,
            mention_count: summary.mention_count,
            dm_participant_count: Some(quick_switcher_dm_participant_count(&summary.title)),
            match_candidates: quick_switcher_dm_match_candidates(
                &summary.title,
                &label,
                profile_names,
            ),
        };
        dm_entry_indices.push(entries.len());
        entries.push(entry);
    }

    QuickSwitcherSearchCorpus {
        revision,
        entries,
        channel_entry_indices,
        dm_entry_indices,
    }
}

fn quick_switcher_message_label(
    conversation_id: &ConversationId,
    channels: &[ConversationSummary],
    direct_messages: &[ConversationSummary],
    profile_names: &HashMap<String, String>,
) -> String {
    channels
        .iter()
        .chain(direct_messages.iter())
        .find(|summary| summary.id == *conversation_id)
        .map(|summary| quick_switcher_label_for_summary(summary, profile_names))
        .unwrap_or_else(|| conversation_id.0.clone())
}

fn mention_affinity_rank(affinity: Affinity) -> i32 {
    match affinity {
        Affinity::Positive => 2,
        Affinity::None => 1,
        Affinity::Broken => 0,
    }
}

fn mention_broadcast_candidates(query: &str, max_results: usize) -> Vec<AutocompleteCandidate> {
    let normalized = query.trim().to_ascii_lowercase();
    let mut items = vec![
        ("here", "Notify active members"),
        ("channel", "Notify everyone in channel"),
        ("everyone", "Notify everyone in channel"),
    ]
    .into_iter()
    .filter(|(keyword, _)| {
        normalized.is_empty()
            || keyword.starts_with(&normalized)
            || fuzzy_match(&normalized, keyword).is_some()
    })
    .map(
        |(keyword, description)| AutocompleteCandidate::MentionBroadcast {
            keyword: keyword.to_string(),
            description: description.to_string(),
        },
    )
    .collect::<Vec<_>>();
    items.truncate(max_results.max(1));
    items
}

fn resolve_mention_username(
    raw_user_id: &str,
    display_name: &str,
    profile_names: &HashMap<String, String>,
) -> String {
    let raw = raw_user_id.trim();
    let raw_lower = raw.to_ascii_lowercase();
    if let Some((username, _)) = profile_names.get_key_value(&raw_lower) {
        return username.clone();
    }

    let display_lower = display_name.trim().to_ascii_lowercase();
    if !display_lower.is_empty()
        && let Some((username, _)) = profile_names
            .iter()
            .find(|(_, full_name)| full_name.trim().eq_ignore_ascii_case(display_name))
    {
        return username.clone();
    }

    if raw.contains(' ') {
        if let Some((username, _)) = profile_names
            .iter()
            .find(|(_, full_name)| full_name.trim().eq_ignore_ascii_case(raw))
        {
            return username.clone();
        }
    }

    raw_lower
}

fn quick_switcher_decayed_affinity(affinity: f32, last_updated_ms: i64, now_ms: i64) -> f32 {
    if !affinity.is_finite() || affinity <= 0.0 {
        return 0.0;
    }
    let elapsed_ms = now_ms.saturating_sub(last_updated_ms).max(0) as f32;
    if elapsed_ms <= 0.0 {
        return affinity;
    }
    let decay_exponent =
        -(std::f32::consts::LN_2 * elapsed_ms / QUICK_SWITCHER_AFFINITY_HALF_LIFE_MS);
    affinity * decay_exponent.exp()
}

fn quick_switcher_affinity_boost(affinity: f32) -> i32 {
    if !affinity.is_finite() || affinity <= 0.0 {
        return 0;
    }
    let scaled = (QUICK_SWITCHER_AFFINITY_BOOST_SCALE * (1.0 + affinity).ln()).round() as i32;
    scaled.clamp(0, QUICK_SWITCHER_AFFINITY_MAX_BOOST)
}

fn quick_switcher_affinity_boost_for_conversation(
    conversation_id: &ConversationId,
    affinity_by_conversation_id: Option<&HashMap<String, (f32, i64)>>,
    now_ms: i64,
) -> i32 {
    let Some((affinity, last_updated_ms)) =
        affinity_by_conversation_id.and_then(|map| map.get(&conversation_id.0))
    else {
        return 0;
    };
    let decayed_affinity = quick_switcher_decayed_affinity(*affinity, *last_updated_ms, now_ms);
    quick_switcher_affinity_boost(decayed_affinity)
}

fn compare_quick_switcher_scored(
    left: &(i32, Option<usize>, String, QuickSwitcherResult),
    right: &(i32, Option<usize>, String, QuickSwitcherResult),
) -> std::cmp::Ordering {
    let left_count = left.1.unwrap_or(usize::MAX);
    let right_count = right.1.unwrap_or(usize::MAX);
    right
        .0
        .cmp(&left.0)
        .then_with(|| left_count.cmp(&right_count))
        .then_with(|| left.2.cmp(&right.2))
}

fn push_quick_switcher_topk(
    topk: &mut Vec<(i32, Option<usize>, String, QuickSwitcherResult)>,
    scored: (i32, Option<usize>, String, QuickSwitcherResult),
    limit: usize,
) {
    if topk.len() < limit {
        topk.push(scored);
        return;
    }
    let mut worst_idx = 0usize;
    for idx in 1..topk.len() {
        if compare_quick_switcher_scored(&topk[idx], &topk[worst_idx])
            == std::cmp::Ordering::Greater
        {
            worst_idx = idx;
        }
    }
    if compare_quick_switcher_scored(&scored, &topk[worst_idx]) == std::cmp::Ordering::Less {
        topk[worst_idx] = scored;
    }
}

pub(crate) fn compute_quick_switcher_local_results(
    query: &str,
    corpus: &QuickSwitcherSearchCorpus,
    previous_query: Option<&str>,
    previous_matched_entry_indices: &[usize],
    previous_corpus_revision: Option<u64>,
    affinity_by_conversation_id: Option<&HashMap<String, (f32, i64)>>,
    now_ms: i64,
) -> QuickSwitcherLocalSearchOutput {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return QuickSwitcherLocalSearchOutput::default();
    }
    let Some(prepared_query) = prepare_fuzzy_query(trimmed) else {
        return QuickSwitcherLocalSearchOutput::default();
    };

    let previous_trimmed = previous_query.map(str::trim).unwrap_or_default();
    let use_incremental_narrowing = !previous_trimmed.is_empty()
        && trimmed.starts_with(previous_trimmed)
        && previous_corpus_revision == Some(corpus.revision)
        && !previous_matched_entry_indices.is_empty();

    let mut incremental_channel_indices = Vec::new();
    let mut incremental_dm_indices = Vec::new();
    if use_incremental_narrowing {
        for entry_idx in previous_matched_entry_indices {
            let Some(entry) = corpus.entries.get(*entry_idx) else {
                continue;
            };
            if matches!(entry.kind, QuickSwitcherResultKind::Channel) {
                incremental_channel_indices.push(*entry_idx);
            } else {
                incremental_dm_indices.push(*entry_idx);
            }
        }
    }

    let channel_indices = if use_incremental_narrowing {
        incremental_channel_indices.as_slice()
    } else {
        corpus.channel_entry_indices.as_slice()
    };
    let dm_indices = if use_incremental_narrowing {
        incremental_dm_indices.as_slice()
    } else {
        corpus.dm_entry_indices.as_slice()
    };

    let mut scored_topk: Vec<(i32, Option<usize>, String, QuickSwitcherResult)> = Vec::new();
    let mut matched_entry_indices = Vec::new();
    let mut scanned_entries = 0usize;
    let mut channel_scanned_entries = 0usize;
    let mut dm_scanned_entries = 0usize;
    let mut rejected_candidates = 0usize;
    let mut fuzzy_evaluated = 0usize;
    let affinity_boost_enabled = trimmed.chars().count() >= 2;

    let mut evaluate_entry = |entry_idx: usize, is_channel: bool| {
        let Some(entry) = corpus.entries.get(entry_idx) else {
            return;
        };
        scanned_entries = scanned_entries.saturating_add(1);
        if is_channel {
            channel_scanned_entries = channel_scanned_entries.saturating_add(1);
        } else {
            dm_scanned_entries = dm_scanned_entries.saturating_add(1);
        }
        let Some(matched) = quick_switcher_match_for_candidates_prepared(
            &prepared_query,
            &entry.match_candidates,
            &mut rejected_candidates,
            &mut fuzzy_evaluated,
        ) else {
            return;
        };
        matched_entry_indices.push(entry_idx);
        let unread_boost = if entry.unread_count > 0 { 50 } else { 0 };
        let mention_boost = if entry.mention_count > 0 { 70 } else { 0 };
        let participant_penalty = entry
            .dm_participant_count
            .map(|participants| participants.saturating_sub(1) as i32 * 30)
            .unwrap_or(0);
        let affinity_boost = if affinity_boost_enabled {
            quick_switcher_affinity_boost_for_conversation(
                &entry.conversation_id,
                affinity_by_conversation_id,
                now_ms,
            )
        } else {
            0
        };
        let score =
            matched.score + unread_boost + mention_boost + affinity_boost - participant_penalty;
        let scored = (
            score,
            entry.dm_participant_count,
            entry.label.clone(),
            QuickSwitcherResult {
                label: entry.label.clone(),
                sublabel: entry.sublabel.clone(),
                kind: entry.kind.clone(),
                route: entry.route.clone(),
                conversation_id: entry.conversation_id.clone(),
                message_id: None,
                match_ranges: matched.ranges,
            },
        );
        push_quick_switcher_topk(&mut scored_topk, scored, QUICK_SWITCHER_CONVERSATION_LIMIT);
    };

    for entry_idx in channel_indices {
        evaluate_entry(*entry_idx, true);
    }
    for entry_idx in dm_indices {
        evaluate_entry(*entry_idx, false);
    }

    scored_topk.sort_by(compare_quick_switcher_scored);

    QuickSwitcherLocalSearchOutput {
        results: scored_topk
            .into_iter()
            .map(|(_, _, _, result)| result)
            .collect(),
        matched_entry_indices,
        scanned_entries,
        channel_scanned_entries,
        dm_scanned_entries,
        rejected_candidates,
        fuzzy_evaluated,
    }
}

fn conversation_id_for_route(route: &Route) -> Option<ConversationId> {
    match route {
        Route::Channel { channel_id, .. } => Some(match channel_id.0.as_str() {
            "design" => ConversationId::new("conv_design"),
            _ => ConversationId::new("conv_general"),
        }),
        Route::DirectMessage { .. } => Some(ConversationId::new("conv_alice")),
        _ => None,
    }
}

fn active_call_participants(conversation: &ConversationSummary) -> Vec<UserSummary> {
    match conversation.kind {
        ConversationKind::DirectMessage => vec![demo_me(), demo_alice()],
        ConversationKind::Channel | ConversationKind::GroupDirectMessage => {
            if conversation.title == "design" {
                vec![demo_me(), demo_sam(), demo_alice()]
            } else {
                vec![demo_me(), demo_alice(), demo_sam()]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channel_summary(id: &str, title: &str) -> ConversationSummary {
        ConversationSummary {
            id: ConversationId::new(id),
            title: title.to_string(),
            kind: ConversationKind::Channel,
            topic: String::new(),
            group: None,
            unread_count: 0,
            mention_count: 0,
            muted: false,
            last_activity_ms: 0,
        }
    }

    fn dm_summary(id: &str, title: &str, kind: ConversationKind) -> ConversationSummary {
        ConversationSummary {
            id: ConversationId::new(id),
            title: title.to_string(),
            kind,
            topic: String::new(),
            group: None,
            unread_count: 0,
            mention_count: 0,
            muted: false,
            last_activity_ms: 0,
        }
    }

    #[test]
    fn quick_switcher_dm_match_supports_name_reordering() {
        let mut profile_names = HashMap::new();
        profile_names.insert("gene".to_string(), "Gene Hoffman".to_string());
        profile_names.insert("butterbot".to_string(), "Butter Bot".to_string());

        let matched = quick_switcher_match_for_dm(
            "gebut",
            "butterbot, gene",
            "Butter Bot, Gene Hoffman",
            &profile_names,
        );
        assert!(matched.is_some(), "expected reordered DM match");
    }

    #[test]
    fn quick_switcher_prefers_smaller_group_dm_for_same_match() {
        let mut models = AppModels::empty_with_settings(SettingsModel::default());
        models.workspace.channels.clear();
        models.workspace.direct_messages = vec![
            dm_summary(
                "conv_superset",
                "butterbot, cameroncooper, chrisfoudy, hoffmang",
                ConversationKind::GroupDirectMessage,
            ),
            dm_summary(
                "conv_subset",
                "butterbot, cameroncooper, hoffmang",
                ConversationKind::GroupDirectMessage,
            ),
        ];
        models.update_quick_switcher_profile_names(
            [
                ("cameroncooper".to_string(), "Cameron Cooper".to_string()),
                ("hoffmang".to_string(), "Gene Hoffman".to_string()),
                ("chrisfoudy".to_string(), "Chris Foudy".to_string()),
                ("butterbot".to_string(), "Butter Bot".to_string()),
            ]
            .into_iter()
            .collect(),
        );

        models.update_quick_switcher_query("butgen".to_string());
        let ranked_ids = models
            .quick_switcher
            .results
            .iter()
            .map(|result| result.conversation_id.0.clone())
            .collect::<Vec<_>>();

        let subset_ix = ranked_ids
            .iter()
            .position(|id| id == "conv_subset")
            .expect("subset DM should be present");
        let superset_ix = ranked_ids
            .iter()
            .position(|id| id == "conv_superset")
            .expect("superset DM should be present");
        assert!(
            subset_ix < superset_ix,
            "expected smaller participant group to rank earlier"
        );
    }

    #[test]
    fn quick_switcher_shows_recent_conversations_when_unread_empty() {
        let mut models = AppModels::empty_with_settings(SettingsModel::default());
        let workspace_id = models.workspace.workspace_id.clone();
        models.workspace.channels = vec![
            channel_summary("conv_chan_1", "general"),
            channel_summary("conv_chan_2", "design"),
            channel_summary("conv_chan_3", "eng"),
            channel_summary("conv_chan_4", "product"),
        ];
        models.workspace.direct_messages = vec![
            dm_summary("conv_dm_1", "alice", ConversationKind::DirectMessage),
            dm_summary("conv_dm_2", "bob", ConversationKind::DirectMessage),
        ];
        models.navigation.current = Route::Channel {
            workspace_id: workspace_id.clone(),
            channel_id: ChannelId::new("conv_chan_1"),
        };
        models.navigation.back_stack = vec![
            Route::Channel {
                workspace_id: workspace_id.clone(),
                channel_id: ChannelId::new("conv_chan_1"),
            },
            Route::DirectMessage {
                workspace_id: workspace_id.clone(),
                dm_id: DmId::new("conv_dm_1"),
            },
            Route::Channel {
                workspace_id: workspace_id.clone(),
                channel_id: ChannelId::new("conv_chan_2"),
            },
            Route::DirectMessage {
                workspace_id: workspace_id.clone(),
                dm_id: DmId::new("conv_dm_2"),
            },
            Route::Channel {
                workspace_id: workspace_id.clone(),
                channel_id: ChannelId::new("conv_chan_3"),
            },
            Route::Channel {
                workspace_id,
                channel_id: ChannelId::new("conv_chan_4"),
            },
        ];

        models.update_quick_switcher_query(String::new());

        let recent_ids = models
            .quick_switcher
            .results
            .iter()
            .map(|result| result.conversation_id.0.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            recent_ids,
            vec![
                "conv_chan_4",
                "conv_chan_3",
                "conv_dm_2",
                "conv_chan_2",
                "conv_dm_1",
                "conv_chan_1",
            ]
        );
    }

    #[test]
    fn quick_switcher_uses_persisted_affinity_for_recent_conversations() {
        let mut settings = SettingsModel::default();
        settings
            .quick_switcher_affinity
            .insert("conv_chan_2".to_string(), (2.0, 2_000));
        settings
            .quick_switcher_affinity
            .insert("conv_dm_1".to_string(), (1.0, 1_000));
        let mut models = AppModels::empty_with_settings(settings);
        let workspace_id = models.workspace.workspace_id.clone();
        models.workspace.channels = vec![
            channel_summary("conv_chan_1", "general"),
            channel_summary("conv_chan_2", "design"),
        ];
        models.workspace.direct_messages = vec![dm_summary(
            "conv_dm_1",
            "alice",
            ConversationKind::DirectMessage,
        )];
        models.navigation.current = Route::WorkspaceHome { workspace_id };
        models.navigation.back_stack.clear();

        models.update_quick_switcher_query(String::new());

        let recent_ids = models
            .quick_switcher
            .results
            .iter()
            .filter(|result| result.kind != QuickSwitcherResultKind::UnreadChannel)
            .map(|result| result.conversation_id.0.as_str())
            .collect::<Vec<_>>();
        assert_eq!(recent_ids, vec!["conv_chan_2", "conv_dm_1"]);
    }

    fn quick_switcher_dm_corpus(entries: &[(&str, &str)]) -> QuickSwitcherSearchCorpus {
        let workspace_id = WorkspaceId::new("ws_test");
        let direct_messages = entries
            .iter()
            .map(|(id, title)| dm_summary(id, title, ConversationKind::DirectMessage))
            .collect::<Vec<_>>();
        let profile_names = HashMap::new();
        build_quick_switcher_search_corpus(&[], &direct_messages, &profile_names, &workspace_id, 1)
    }

    #[test]
    fn quick_switcher_affinity_boost_prioritizes_preferred_match() {
        let corpus =
            quick_switcher_dm_corpus(&[("conv_geoff_a", "geoff a"), ("conv_geoff_b", "geoff b")]);
        let now_ms = 1_710_000_000_000i64;
        let mut affinity = HashMap::new();
        affinity.insert("conv_geoff_b".to_string(), (6.0, now_ms));

        let local = compute_quick_switcher_local_results(
            "geoff",
            &corpus,
            None,
            &[],
            None,
            Some(&affinity),
            now_ms,
        );
        assert_eq!(
            local
                .results
                .first()
                .map(|result| result.conversation_id.0.as_str()),
            Some("conv_geoff_b")
        );
    }

    #[test]
    fn quick_switcher_affinity_decay_reduces_stale_bias() {
        let corpus =
            quick_switcher_dm_corpus(&[("conv_geoff_a", "geoff a"), ("conv_geoff_b", "geoff b")]);
        let now_ms = 1_710_000_000_000i64;
        let sixty_days_ms = 60 * 24 * 60 * 60 * 1000_i64;
        let mut affinity = HashMap::new();
        affinity.insert("conv_geoff_a".to_string(), (30.0, now_ms - sixty_days_ms));
        affinity.insert("conv_geoff_b".to_string(), (4.0, now_ms));

        let geoff_a_boost = quick_switcher_affinity_boost_for_conversation(
            &ConversationId::new("conv_geoff_a"),
            Some(&affinity),
            now_ms,
        );
        let geoff_b_boost = quick_switcher_affinity_boost_for_conversation(
            &ConversationId::new("conv_geoff_b"),
            Some(&affinity),
            now_ms,
        );
        assert!(geoff_b_boost > geoff_a_boost);

        let local = compute_quick_switcher_local_results(
            "geoff",
            &corpus,
            None,
            &[],
            None,
            Some(&affinity),
            now_ms,
        );
        assert_eq!(
            local
                .results
                .first()
                .map(|result| result.conversation_id.0.as_str()),
            Some("conv_geoff_b")
        );
    }

    #[test]
    fn mention_autocomplete_matches_username_or_profile_name() {
        use crate::domain::channel_details::{
            ChannelDetails, ChannelMemberPreview, NotificationLevel,
        };

        let mut models = AppModels::empty_with_settings(SettingsModel::default());
        let conversation_id = models.conversation.summary.id.clone();
        let member = ChannelMemberPreview {
            user_id: UserId::new("cameroncooper"),
            display_name: "cameroncooper".to_string(),
            avatar_asset: None,
            affinity: Affinity::Positive,
            is_team_admin_or_owner: false,
        };
        models.conversation.details = Some(ChannelDetails {
            conversation_id,
            title: "general".to_string(),
            topic: String::new(),
            kind: ConversationKind::Channel,
            group: None,
            member_count: 1,
            members: vec![member.clone()],
            member_preview: vec![member],
            notification_level: NotificationLevel::All,
            pinned_items: Vec::new(),
            can_edit_topic: false,
            can_manage_members: false,
            can_archive: false,
            can_leave: true,
            can_post: true,
            created_at: None,
            description: None,
            is_archived: false,
        });
        models.upsert_quick_switcher_profile_name(&UserId::new("cameroncooper"), "Cameron Cooper");

        let by_username = models.mention_autocomplete_candidates("camer", 8);
        assert!(by_username.iter().any(|candidate| {
            matches!(
                candidate,
                AutocompleteCandidate::MentionUser { username, .. } if username == "cameroncooper"
            )
        }));

        let by_profile = models.mention_autocomplete_candidates("cooper", 8);
        assert!(by_profile.iter().any(|candidate| {
            matches!(
                candidate,
                AutocompleteCandidate::MentionUser {
                    username,
                    display_name,
                    ..
                } if username == "cameroncooper" && display_name == "Cameron Cooper"
            )
        }));
    }

    #[test]
    fn emoji_autocomplete_keeps_custom_as_shortcode() {
        let custom_items = vec![EmojiPickerItem::Custom {
            alias: "party_parrot".to_string(),
            unicode: None,
            asset_path: Some("/tmp/party_parrot.png".to_string()),
        }];

        let results = AppModels::emoji_autocomplete_candidates("party", &custom_items, 8);
        let custom = results.into_iter().find(|candidate| {
            matches!(
                candidate,
                AutocompleteCandidate::Emoji { label, .. } if label == ":party_parrot:"
            )
        });
        assert!(custom.is_some());
        assert_eq!(
            custom
                .expect("custom candidate should be present")
                .completion_text(),
            ":party_parrot: ".to_string()
        );
    }

    #[test]
    fn mention_autocomplete_resolves_display_name_to_username() {
        use crate::domain::channel_details::{
            ChannelDetails, ChannelMemberPreview, NotificationLevel,
        };

        let mut models = AppModels::empty_with_settings(SettingsModel::default());
        let conversation_id = models.conversation.summary.id.clone();
        let member = ChannelMemberPreview {
            user_id: UserId::new("cameron cooper"),
            display_name: "Cameron Cooper".to_string(),
            avatar_asset: None,
            affinity: Affinity::Positive,
            is_team_admin_or_owner: false,
        };
        models.conversation.details = Some(ChannelDetails {
            conversation_id,
            title: "general".to_string(),
            topic: String::new(),
            kind: ConversationKind::Channel,
            group: None,
            member_count: 1,
            members: vec![member.clone()],
            member_preview: vec![member],
            notification_level: NotificationLevel::All,
            pinned_items: Vec::new(),
            can_edit_topic: false,
            can_manage_members: false,
            can_archive: false,
            can_leave: true,
            can_post: true,
            created_at: None,
            description: None,
            is_archived: false,
        });
        models.upsert_quick_switcher_profile_name(&UserId::new("cameroncooper"), "Cameron Cooper");

        let results = models.mention_autocomplete_candidates("camer", 8);
        let candidate = results.into_iter().find_map(|candidate| match candidate {
            AutocompleteCandidate::MentionUser {
                username,
                display_name,
                ..
            } => Some((username, display_name)),
            _ => None,
        });
        assert_eq!(
            candidate,
            Some(("cameroncooper".to_string(), "Cameron Cooper".to_string()))
        );
    }

    #[test]
    fn mention_autocomplete_excludes_broadcast_in_dm() {
        let models = AppModels::empty_with_settings(SettingsModel::default());
        assert!(
            matches!(
                models.conversation.summary.kind,
                ConversationKind::DirectMessage
            ),
            "empty models should default to DM conversation kind"
        );

        let results = models.mention_autocomplete_candidates("", 8);
        assert!(
            results.iter().all(|candidate| !matches!(
                candidate,
                AutocompleteCandidate::MentionBroadcast { .. }
            )),
            "DM mention autocomplete should not include broadcast mentions"
        );
    }

    #[test]
    fn mention_autocomplete_includes_broadcast_in_channel_without_details() {
        let mut models = AppModels::empty_with_settings(SettingsModel::default());
        models.conversation.summary.kind = ConversationKind::Channel;
        models.conversation.details = None;

        let results = models.mention_autocomplete_candidates("", 8);
        assert!(
            results.iter().any(|candidate| matches!(
                candidate,
                AutocompleteCandidate::MentionBroadcast { .. }
            )),
            "Channel mention autocomplete should include broadcast mentions even before details load"
        );
    }
}
