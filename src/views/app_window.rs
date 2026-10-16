use crate::services::og_service::{OgFetchResult, OgService};
use crate::state::state::{BootPhase, TimelineKey};
use crate::{
    app::{commands, theme::resolve_theme},
    domain::{
        affinity::Affinity,
        attachment::AttachmentSource,
        backend::{BackendCapabilities, ProviderMessageRef},
        channel_details::{ChannelDetails, ChannelMemberPreview, NotificationLevel},
        conversation::{ConversationKind, ConversationSummary},
        ids::{ConversationId, MessageId, SidebarSectionId, UserId, WorkspaceId},
        message::{ChatEvent, LinkPreview, MessageFragment, MessageRecord, MessageSendState},
        profile::SocialGraphListType,
        route::Route,
        search::SearchFilter,
        user::UserSummary,
    },
    models::{
        AppModels, PendingSend, QuickSwitcherLocalSearchOutput, SendDispatch,
        app_model::Connectivity,
        composer_model::{AutocompleteState, ComposerMode},
        compute_quick_switcher_local_results,
        emoji_picker_model::{EmojiPickerItem, recent_key_for_stock, selected_emoji_for_tone},
        file_upload_model::UploadTarget,
        navigation_model::RightPaneMode,
        notifications_model::ToastAction,
        profile_panel_model::SocialTab,
        quick_switcher_model::QuickSwitcherResultKind,
        sidebar_model::SidebarModel,
        timeline_model::TeamAuthorRole,
    },
    services::{
        backends::router::BackendRouter, local_store::LocalStore, settings_store::SettingsStore,
    },
    state::{
        AppStore, ConnectionState, DraftKey, UiAction,
        bindings::MessageBinding,
        effect::BackendCommand,
        event::{BackendEvent, TeamRoleKind},
    },
    util::{
        autocomplete_detect::{AutocompleteTriggerKind, TriggerMatch, detect_autocomplete_trigger},
        formatting::now_unix_ms,
        interactive_qos::mark_quick_switcher_input_activity,
        perf_harness::{PerfHarness, PerfTimer},
        video_decoder::decode_video_to_render_image,
    },
    views::{
        MAIN_PANEL_MIN_WIDTH_PX, RIGHT_PANE_RESIZE_HANDLE_WIDTH_PX, SHELL_GAP_PX,
        SHELL_HORIZONTAL_PADDING_PX, app_backdrop, badge, border,
        calls::MiniCallDock,
        composer::render_autocomplete_popup,
        glass_surface_dark,
        input::TextField,
        is_dark_theme,
        main_panel::MainPanelHost,
        overlays::OverlayHost,
        panel_alt_bg, panel_bg, panel_surface,
        right_pane::{RightPaneHost, RightPaneResizeDrag},
        selectable_text::SelectableText,
        shell_border_strong,
        sidebar::{Sidebar, SidebarHost},
        sidebar_bg, subtle_surface, text_primary, text_secondary, tint, with_theme,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, AnyView, App, AppContext, AsyncApp, ClickEvent, ClipboardItem, Context,
    CursorStyle, Entity, FocusHandle, Focusable, InteractiveElement, IntoElement, ListAlignment,
    ListOffset, ListState, MouseButton, MouseDownEvent, MouseMoveEvent, MouseUpEvent,
    ParentElement, PathPromptOptions, Render, RenderImage, ScrollDelta, ScrollHandle,
    ScrollWheelEvent, StatefulInteractiveElement, StyleRefinement, Styled, Subscription,
    WeakEntity, Window, div, px, rgb,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    path::{Path, PathBuf},
    sync::{
        Arc,
        mpsc::{self, Receiver, Sender},
    },
    thread,
    time::{Duration, Instant},
};

const ENV_BENCH_SKIP_BACKEND: &str = "ZBASE_BENCH_SKIP_BACKEND";
const ENV_BENCH_SCRIPT: &str = "ZBASE_BENCH_SCRIPT";
const ENV_BENCH_SCRIPT_TICK_MS: &str = "ZBASE_BENCH_SCRIPT_TICK_MS";
const ENV_BENCH_TIMELINE_MESSAGES: &str = "ZBASE_BENCH_TIMELINE_MESSAGES";
const ENV_BENCH_PROFILE_USER: &str = "ZBASE_BENCH_PROFILE_USER";
const ENV_BENCH_THREAD_CONVERSATION_ID: &str = "ZBASE_BENCH_THREAD_CONVERSATION_ID";
const ENV_BENCH_THREAD_ROOT_ID: &str = "ZBASE_BENCH_THREAD_ROOT_ID";
const ENV_BENCH_THREAD_ROOT_IDS: &str = "ZBASE_BENCH_THREAD_ROOT_IDS";
const ENV_BENCH_EXIT_ON_STOP: &str = "ZBASE_BENCH_EXIT_ON_STOP";
const ENV_THREAD_OPEN_PROFILE: &str = "ZBASE_THREAD_OPEN_PROFILE";
const BACKEND_POLL_BOOT_INTERVAL: Duration = Duration::from_millis(50);
const BACKEND_POLL_READY_INTERVAL: Duration = Duration::from_millis(200);
const QUICK_SWITCHER_REMOTE_MIN_QUERY_CHARS: usize = 2;
const QUICK_SWITCHER_CORPUS_REBUILD_COALESCE: Duration = Duration::from_millis(180);
const TEXT_INPUT_SYNC_DEFER_WINDOW: Duration = Duration::from_millis(280);
const MARK_READ_THROTTLE: Duration = Duration::from_millis(1200);
const MAX_VIDEO_RENDER_CACHE_ENTRIES: usize = 8;
const MAX_BACKEND_EVENTS_PER_DRAIN: usize = 48;
const NON_TEXT_PLACEHOLDER_BODY: &str = "<non-text message>";
const INLINE_AUTOCOMPLETE_MAX_RESULTS: usize = 8;
const AUTOCOMPLETE_NAV_DEBOUNCE: Duration = Duration::from_millis(35);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InputAutocompleteTarget {
    Composer,
    Thread,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AutocompleteDismissSignature {
    trigger: char,
    trigger_offset: usize,
    cursor: usize,
    query: String,
}

fn is_non_text_placeholder_message(message: &MessageRecord) -> bool {
    if message.event.is_some() {
        return false;
    }
    if !message.attachments.is_empty() || !message.link_previews.is_empty() {
        return false;
    }
    if message.fragments.len() != 1 {
        return false;
    }
    matches!(
        message.fragments.first(),
        Some(MessageFragment::Text(text)) if text.trim() == NON_TEXT_PLACEHOLDER_BODY
    )
}

fn quick_switcher_local_debounce(
    conversation_count: usize,
    prefix_extend: bool,
    short_query: bool,
) -> Duration {
    if prefix_extend {
        if conversation_count >= 10_000 {
            Duration::from_millis(16)
        } else if conversation_count >= 5_000 {
            Duration::from_millis(12)
        } else if conversation_count >= 2_000 {
            Duration::from_millis(8)
        } else {
            Duration::from_millis(0)
        }
    } else if short_query {
        if conversation_count >= 10_000 {
            Duration::from_millis(28)
        } else if conversation_count >= 5_000 {
            Duration::from_millis(22)
        } else if conversation_count >= 2_000 {
            Duration::from_millis(18)
        } else {
            Duration::from_millis(12)
        }
    } else if conversation_count >= 10_000 {
        Duration::from_millis(70)
    } else if conversation_count >= 5_000 {
        Duration::from_millis(56)
    } else if conversation_count >= 2_000 {
        Duration::from_millis(42)
    } else {
        Duration::from_millis(28)
    }
}

fn quick_switcher_remote_debounce(conversation_count: usize) -> Duration {
    if conversation_count >= 10_000 {
        Duration::from_millis(200)
    } else if conversation_count >= 5_000 {
        Duration::from_millis(180)
    } else if conversation_count >= 2_000 {
        Duration::from_millis(150)
    } else {
        Duration::from_millis(120)
    }
}

fn unique_copy_destination(downloads_dir: &Path, preferred_name: &str) -> PathBuf {
    let candidate = downloads_dir.join(preferred_name);
    if !candidate.exists() {
        return candidate;
    }
    let stem = std::path::Path::new(preferred_name)
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("attachment");
    let ext = std::path::Path::new(preferred_name)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    for index in 1..500 {
        let numbered = if ext.is_empty() {
            format!("{stem} ({index})")
        } else {
            format!("{stem} ({index}).{ext}")
        };
        let path = downloads_dir.join(numbered);
        if !path.exists() {
            return path;
        }
    }
    candidate
}

pub struct AppWindow {
    models: AppModels,
    app_store: AppStore,
    backend_router: BackendRouter,
    focus_handle: FocusHandle,
    quick_switcher_input: Entity<TextField>,
    new_chat_input: Entity<TextField>,
    emoji_picker_input: Entity<TextField>,
    file_upload_caption_input: Entity<TextField>,
    find_in_chat_input: Entity<TextField>,
    search_input: Entity<TextField>,
    composer_input: Entity<TextField>,
    thread_input: Entity<TextField>,
    sidebar_filter_input: Entity<TextField>,
    selectable_texts: HashMap<String, Entity<SelectableText>>,
    subscriptions: Vec<Subscription>,
    thread_resize_drag: Option<ThreadResizeDrag>,
    sidebar_resize_drag: Option<SidebarResizeState>,
    timeline_list_state: ListState,
    timeline_row_render_cache: crate::views::timeline::TimelineRowRenderCache,
    pub(crate) code_highlight_cache: crate::views::code_highlight::CodeHighlightCache,
    thread_scroll: ScrollHandle,
    profile_scroll: ScrollHandle,
    profile_social_scroll: ScrollHandle,
    timeline_unseen_count: usize,
    thread_unseen_count: usize,
    last_timeline_message_count: usize,
    last_timeline_latest_message_id: Option<MessageId>,
    last_timeline_loading_older: bool,
    timeline_scroll_seq: u64,
    pending_older_scroll_anchor: Option<MessageId>,
    pending_older_scroll_seq: Option<u64>,
    suppress_next_timeline_bottom_snap: bool,
    suppress_next_timeline_unseen_increment: bool,
    /// Set by navigate_to_message to prevent background timeline loads from overriding
    /// the jump-to-message scroll position. Cleared once the list has been laid out.
    jump_to_message_active: bool,
    last_thread_reply_count: usize,
    pending_thread_scroll_to_bottom: bool,
    sidebar_dm_avatar_assets: HashMap<String, String>,
    sidebar_view: Option<Entity<CachedSidebarView>>,
    keybase_inspector: KeybaseInspectorState,
    perf_harness: PerfHarness,
    perf_capture_generation: u64,
    bench_skip_backend: bool,
    bench_script: Option<BenchScriptConfig>,
    bench_script_step: u64,
    bench_profile_user_id: Option<UserId>,
    bench_profile_initialized: bool,
    bench_thread_conversation_id: Option<ConversationId>,
    bench_thread_root_id: Option<MessageId>,
    bench_thread_root_ids: Vec<MessageId>,
    perf_exit_on_stop: bool,
    sync_cache: SyncCache,
    pending_backend_events: VecDeque<BackendEvent>,
    video_render_cache: HashMap<String, Arc<RenderImage>>,
    video_cache_order: VecDeque<String>,
    video_pending_urls: HashSet<String>,
    failed_video_urls: HashSet<String>,
    video_result_sender: Sender<VideoDecodeOutcome>,
    video_result_receiver: Receiver<VideoDecodeOutcome>,
    preview_summaries: HashMap<ConversationId, (ConversationSummary, bool)>,
    og_service: OgService,
    og_result_sender: Sender<OgFetchResult>,
    og_result_receiver: Receiver<OgFetchResult>,
    last_mark_read_attempt: HashMap<String, (String, Instant)>,
    window_is_focused: bool,
    hover_settle_seq: u64,
    hover_clear_seq: u64,
    hover_clear_pending: bool,
    quick_switcher_query_seq: u64,
    quick_switcher_last_local_query: String,
    quick_switcher_last_local_matched_entry_indices: Arc<Vec<usize>>,
    quick_switcher_last_local_corpus_revision: u64,
    quick_switcher_remote_dispatch_started_at: HashMap<u64, Instant>,
    quick_switcher_corpus_rebuild_seq: u64,
    quick_switcher_indexing_active: bool,
    quick_switcher_indexing_total_conversations: Option<u64>,
    quick_switcher_indexing_completed_conversations: u64,
    quick_switcher_indexing_messages_indexed: u64,
    last_text_input_activity: Option<Instant>,
    dismissed_composer_autocomplete: Option<AutocompleteDismissSignature>,
    dismissed_thread_autocomplete: Option<AutocompleteDismissSignature>,
    last_autocomplete_nav: Option<(InputAutocompleteTarget, isize, Instant)>,
    resolved_theme: crate::app::theme::ThemeVariant,
    splash_open: bool,
    splash_shown_at: Instant,
    splash_boot_ready: bool,
    pending_file_upload_caption_focus: bool,
    sidebar_scroll_handle: ScrollHandle,
}

#[derive(Clone, Debug, Default)]
struct KeybaseInspectorEntry {
    method: String,
    count: u64,
    last_seen_seq: u64,
    payload_preview: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct KeybaseInspectorState {
    open: bool,
    paused: bool,
    unknown_only: bool,
    seq: u64,
    entries: HashMap<String, KeybaseInspectorEntry>,
}

struct ThreadResizeDrag {
    anchor_x: f32,
    starting_width: f32,
}

#[derive(Clone, Debug)]
struct SidebarResizeDrag;

#[derive(Default)]
struct SidebarResizeDragPreview;

impl Render for SidebarResizeDragPreview {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().w(px(0.)).h(px(0.))
    }
}

struct SidebarResizeState {
    anchor_x: f32,
    starting_width: f32,
}

struct ShellLayout {
    show_right_pane: bool,
    min_shell_width: f32,
}

#[derive(Default)]
struct SyncCache {
    workspace_sig: Option<u64>,
    sidebar_sections_sig: Option<u64>,
    dm_avatar_sig: Option<u64>,
    timeline_rows_sig: Option<u64>,
    timeline_link_previews_sig: Option<u64>,
    timeline_emoji_sig: Option<u64>,
    timeline_reactions_sig: Option<u64>,
    timeline_author_roles_sig: Option<u64>,
    deferred_model_sync: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct SidebarViewState {
    sidebar: SidebarModel,
    connectivity: Connectivity,
    current_user_display_name: String,
    current_user_avatar_asset: Option<String>,
    dm_avatar_assets: HashMap<String, String>,
    current_route: Route,
    theme: crate::app::theme::ThemeVariant,
}

struct CachedSidebarView {
    owner: WeakEntity<AppWindow>,
    state: SidebarViewState,
    hovered_row: Option<String>,
    scroll_handle: ScrollHandle,
}

#[derive(Clone)]
struct VideoDecodeOutcome {
    cache_key: String,
    render_image: Option<Arc<RenderImage>>,
}

#[derive(Clone, Copy, Debug)]
enum BenchScriptScenario {
    TimelineScroll,
    ProfileScroll,
    SidebarFilter,
    SyncHeavy,
    ComposerTyping,
    ComposerPaste,
    ThreadOpen,
}

impl BenchScriptScenario {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "timeline_scroll" | "timeline-scroll" | "timeline" | "scroll_chat" | "scroll-chat" => {
                Some(Self::TimelineScroll)
            }
            "profile_scroll" | "profile-scroll" | "profile" => Some(Self::ProfileScroll),
            "sidebar_filter" | "sidebar-filter" | "sidebar" => Some(Self::SidebarFilter),
            "sync_heavy" | "sync-heavy" | "sync" => Some(Self::SyncHeavy),
            "composer_typing" | "composer-typing" | "composer" | "typing" => {
                Some(Self::ComposerTyping)
            }
            "composer_paste" | "composer-paste" | "paste" => Some(Self::ComposerPaste),
            "thread_open" | "thread-open" | "thread" => Some(Self::ThreadOpen),
            _ => None,
        }
    }

    fn slug(self) -> &'static str {
        match self {
            Self::TimelineScroll => "timeline_scroll",
            Self::ProfileScroll => "profile_scroll",
            Self::SidebarFilter => "sidebar_filter",
            Self::SyncHeavy => "sync_heavy",
            Self::ComposerTyping => "composer_typing",
            Self::ComposerPaste => "composer_paste",
            Self::ThreadOpen => "thread_open",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct BenchScriptConfig {
    scenario: BenchScriptScenario,
    tick: Duration,
}

impl BenchScriptConfig {
    fn from_env() -> Option<Self> {
        let raw = env::var(ENV_BENCH_SCRIPT).ok()?;
        let scenario = BenchScriptScenario::parse(&raw)?;
        let tick_ms = env::var(ENV_BENCH_SCRIPT_TICK_MS)
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|ms| *ms > 0)
            .unwrap_or(16);
        Some(Self {
            scenario,
            tick: Duration::from_millis(tick_ms),
        })
    }
}

impl CachedSidebarView {
    fn new(owner: WeakEntity<AppWindow>, state: SidebarViewState) -> Self {
        Self {
            owner,
            state,
            hovered_row: None,
            scroll_handle: ScrollHandle::new(),
        }
    }

    fn update_state(&mut self, state: SidebarViewState, cx: &mut Context<Self>) {
        if self.state == state {
            return;
        }
        self.state = state;
        cx.notify();
    }
}

impl Render for CachedSidebarView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        with_theme(self.state.theme, || {
            Sidebar.render(
                &self.state.sidebar,
                &self.state.connectivity,
                &self.state.current_user_display_name,
                self.state.current_user_avatar_asset.as_deref(),
                &self.state.dm_avatar_assets,
                &self.state.current_route,
                self.hovered_row.as_deref(),
                &self.scroll_handle,
                cx,
            )
        })
    }
}

impl SidebarHost for CachedSidebarView {
    fn sidebar_toggle_quick_switcher(&mut self, cx: &mut Context<Self>) {
        let owner = self.owner.clone();
        cx.defer(move |cx| {
            let _ = owner.update(cx, |app_window, cx| app_window.toggle_quick_switcher(cx));
        });
    }

    fn sidebar_open_new_chat(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let owner = self.owner.clone();
        window.defer(cx, move |window, cx| {
            let _ = owner.update(cx, |app_window, cx| {
                app_window.open_new_chat(window, cx);
            });
        });
    }

    fn sidebar_open_preferences(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let owner = self.owner.clone();
        window.defer(cx, move |window, cx| {
            let _ = owner.update(cx, |app_window, cx| {
                app_window.navigate_to(Route::Preferences, window, cx);
            });
        });
    }

    fn sidebar_show_hover_tooltip(
        &mut self,
        text: String,
        anchor_x: f32,
        anchor_y: f32,
        cx: &mut Context<Self>,
    ) {
        let owner = self.owner.clone();
        cx.defer(move |cx| {
            let _ = owner.update(cx, |app_window, cx| {
                app_window.show_sidebar_hover_tooltip(text.clone(), anchor_x, anchor_y, cx);
            });
        });
    }

    fn sidebar_hide_hover_tooltip(&mut self, cx: &mut Context<Self>) {
        let owner = self.owner.clone();
        cx.defer(move |cx| {
            let _ = owner.update(cx, |app_window, cx| {
                app_window.clear_sidebar_hover_tooltip(cx);
            });
        });
    }

    fn sidebar_toggle_section(&mut self, section_id: SidebarSectionId, cx: &mut Context<Self>) {
        let owner = self.owner.clone();
        cx.defer(move |cx| {
            let _ = owner.update(cx, |app_window, cx| {
                app_window.toggle_sidebar_section_click(section_id, cx);
            });
        });
    }

    fn sidebar_reorder_section(
        &mut self,
        dragged_id: SidebarSectionId,
        target_id: SidebarSectionId,
        cx: &mut Context<Self>,
    ) {
        let owner = self.owner.clone();
        cx.defer(move |cx| {
            let _ = owner.update(cx, |app_window, cx| {
                app_window.reorder_sidebar_section(dragged_id, target_id, cx);
            });
        });
    }

    fn sidebar_navigate_to(&mut self, route: Route, window: &mut Window, cx: &mut Context<Self>) {
        let owner = self.owner.clone();
        window.defer(cx, move |window, cx| {
            let _ = owner.update(cx, |app_window, cx| {
                app_window.navigate_to(route, window, cx);
            });
        });
    }

    fn sidebar_set_hovered_row(&mut self, label: Option<String>, cx: &mut Context<Self>) {
        if self.hovered_row != label {
            self.hovered_row = label;
            cx.notify();
        }
    }

    fn sidebar_hovered_row(&self) -> Option<&str> {
        self.hovered_row.as_deref()
    }
}

impl SidebarHost for AppWindow {
    fn sidebar_toggle_quick_switcher(&mut self, cx: &mut Context<Self>) {
        self.toggle_quick_switcher(cx);
    }

    fn sidebar_open_new_chat(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.open_new_chat(window, cx);
    }

    fn sidebar_open_preferences(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.navigate_to(Route::Preferences, window, cx);
    }

    fn sidebar_show_hover_tooltip(
        &mut self,
        text: String,
        anchor_x: f32,
        anchor_y: f32,
        cx: &mut Context<Self>,
    ) {
        self.show_sidebar_hover_tooltip(text, anchor_x, anchor_y, cx);
    }

    fn sidebar_hide_hover_tooltip(&mut self, cx: &mut Context<Self>) {
        self.clear_sidebar_hover_tooltip(cx);
    }

    fn sidebar_toggle_section(&mut self, section_id: SidebarSectionId, cx: &mut Context<Self>) {
        self.toggle_sidebar_section_click(section_id, cx);
    }

    fn sidebar_reorder_section(
        &mut self,
        dragged_id: SidebarSectionId,
        target_id: SidebarSectionId,
        cx: &mut Context<Self>,
    ) {
        self.reorder_sidebar_section(dragged_id, target_id, cx);
    }

    fn sidebar_navigate_to(&mut self, route: Route, window: &mut Window, cx: &mut Context<Self>) {
        self.navigate_to(route, window, cx);
    }

    fn sidebar_set_hovered_row(&mut self, _label: Option<String>, _cx: &mut Context<Self>) {}

    fn sidebar_hovered_row(&self) -> Option<&str> {
        None
    }
}

impl AppWindow {
    pub fn new(
        models: AppModels,
        app_store: AppStore,
        backend_router: BackendRouter,
        local_store: Arc<LocalStore>,
        focus_handle: FocusHandle,
        quick_switcher_input: Entity<TextField>,
        new_chat_input: Entity<TextField>,
        emoji_picker_input: Entity<TextField>,
        file_upload_caption_input: Entity<TextField>,
        find_in_chat_input: Entity<TextField>,
        search_input: Entity<TextField>,
        composer_input: Entity<TextField>,
        thread_input: Entity<TextField>,
        sidebar_filter_input: Entity<TextField>,
    ) -> Self {
        let last_timeline_message_count = timeline_message_count(&models);
        let last_timeline_latest_message_id = latest_timeline_message_id(&models.timeline.rows);
        let last_thread_reply_count = models.thread_pane.replies.len();
        let initial_timeline_row_count = models.timeline.rows.len();
        let bench_script = BenchScriptConfig::from_env();
        let bench_profile_user_id = env_nonempty(ENV_BENCH_PROFILE_USER).map(UserId::new);
        let bench_thread_conversation_id =
            env_nonempty(ENV_BENCH_THREAD_CONVERSATION_ID).map(ConversationId::new);
        let bench_thread_root_id = env_nonempty(ENV_BENCH_THREAD_ROOT_ID).map(MessageId::new);
        let bench_thread_root_ids = env_nonempty(ENV_BENCH_THREAD_ROOT_IDS)
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(MessageId::new)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let (video_result_sender, video_result_receiver) = mpsc::channel();
        let (og_result_sender, og_result_receiver) = mpsc::channel();

        Self {
            models,
            app_store,
            backend_router,
            focus_handle,
            quick_switcher_input,
            new_chat_input,
            emoji_picker_input,
            file_upload_caption_input,
            find_in_chat_input,
            search_input,
            composer_input,
            thread_input,
            sidebar_filter_input,
            selectable_texts: HashMap::new(),
            subscriptions: Vec::new(),
            thread_resize_drag: None,
            sidebar_resize_drag: None,
            timeline_list_state: ListState::new(
                initial_timeline_row_count,
                ListAlignment::Top,
                px(1920.),
            ),
            timeline_row_render_cache: crate::views::timeline::TimelineRowRenderCache::default(),
            code_highlight_cache: crate::views::code_highlight::CodeHighlightCache::default(),
            thread_scroll: ScrollHandle::new(),
            profile_scroll: ScrollHandle::new(),
            profile_social_scroll: ScrollHandle::new(),
            timeline_unseen_count: 0,
            thread_unseen_count: 0,
            last_timeline_message_count,
            last_timeline_latest_message_id,
            last_timeline_loading_older: false,
            timeline_scroll_seq: 0,
            pending_older_scroll_anchor: None,
            pending_older_scroll_seq: None,
            suppress_next_timeline_bottom_snap: false,
            suppress_next_timeline_unseen_increment: false,
            jump_to_message_active: false,
            last_thread_reply_count,
            pending_thread_scroll_to_bottom: false,
            sidebar_dm_avatar_assets: HashMap::new(),
            sidebar_view: None,
            keybase_inspector: KeybaseInspectorState::default(),
            perf_harness: PerfHarness::from_env(),
            perf_capture_generation: 0,
            bench_skip_backend: env_flag(ENV_BENCH_SKIP_BACKEND),
            bench_script,
            bench_script_step: 0,
            bench_profile_user_id,
            bench_profile_initialized: false,
            bench_thread_conversation_id,
            bench_thread_root_id,
            bench_thread_root_ids,
            perf_exit_on_stop: env_flag(ENV_BENCH_EXIT_ON_STOP),
            sync_cache: SyncCache::default(),
            pending_backend_events: VecDeque::new(),
            video_render_cache: HashMap::new(),
            video_cache_order: VecDeque::new(),
            video_pending_urls: HashSet::new(),
            failed_video_urls: HashSet::new(),
            video_result_sender,
            video_result_receiver,
            preview_summaries: HashMap::new(),
            og_service: OgService::new(local_store),
            og_result_sender,
            og_result_receiver,
            last_mark_read_attempt: HashMap::new(),
            window_is_focused: false,
            hover_settle_seq: 0,
            hover_clear_seq: 0,
            hover_clear_pending: false,
            quick_switcher_query_seq: 0,
            quick_switcher_last_local_query: String::new(),
            quick_switcher_last_local_matched_entry_indices: Arc::new(Vec::new()),
            quick_switcher_last_local_corpus_revision: 0,
            quick_switcher_remote_dispatch_started_at: HashMap::new(),
            quick_switcher_corpus_rebuild_seq: 0,
            quick_switcher_indexing_active: false,
            quick_switcher_indexing_total_conversations: None,
            quick_switcher_indexing_completed_conversations: 0,
            quick_switcher_indexing_messages_indexed: 0,
            last_text_input_activity: None,
            dismissed_composer_autocomplete: None,
            dismissed_thread_autocomplete: None,
            last_autocomplete_nav: None,
            resolved_theme: crate::app::theme::ThemeVariant::Light,
            splash_open: true,
            splash_shown_at: Instant::now(),
            splash_boot_ready: false,
            pending_file_upload_caption_focus: false,
            sidebar_scroll_handle: ScrollHandle::new(),
        }
    }

    pub fn init(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.window_is_focused = window.is_window_active();
        if !self.bench_skip_backend {
            self.dispatch_ui_action(UiAction::StartApp);
            self.dispatch_ui_action(UiAction::Navigate(self.models.navigation.current.clone()));
        }

        self.sync_sidebar_view_state(cx);

        self.subscriptions = vec![
            cx.observe_window_activation(window, |this, window, cx| {
                this.window_is_focused = window.is_window_active();
                if this.window_is_focused && this.request_mark_conversation_read_if_needed() {
                    this.refresh(cx);
                }
            }),
            cx.observe_window_appearance(window, |this, _, cx| {
                this.refresh(cx);
            }),
            cx.observe(&self.quick_switcher_input, |this, input, cx| {
                let observer_started_at = Instant::now();
                let query = input.read(cx).text();
                if !this.models.overlay.quick_switcher_open {
                    return;
                }
                mark_quick_switcher_input_activity();
                if this.models.quick_switcher.query == query {
                    return;
                }
                if this.models.flush_quick_switcher_local_search_corpus_if_dirty() {
                    this.quick_switcher_last_local_query.clear();
                    this.quick_switcher_last_local_matched_entry_indices = Arc::new(Vec::new());
                    this.quick_switcher_last_local_corpus_revision = 0;
                }
                let query_trimmed = query.trim().to_string();
                let previous_query_trimmed = this.quick_switcher_last_local_query.trim().to_string();
                let prefix_extend = !previous_query_trimmed.is_empty()
                    && query_trimmed.starts_with(&previous_query_trimmed)
                    && query_trimmed.chars().count() > previous_query_trimmed.chars().count();
                let local_corpus = this.models.quick_switcher_local_search_snapshot();
                let quick_switcher_affinity =
                    Arc::new(this.models.settings.quick_switcher_affinity.clone());
                let conversation_count = local_corpus.entries.len();
                let local_debounce = quick_switcher_local_debounce(
                    conversation_count,
                    prefix_extend,
                    query_trimmed.chars().count() <= 2,
                );
                let remote_debounce = quick_switcher_remote_debounce(conversation_count);
                let query_for_local = query.clone();
                let query_for_remote = query.clone();
                let previous_local_query = this.quick_switcher_last_local_query.clone();
                let previous_local_matches =
                    this.quick_switcher_last_local_matched_entry_indices.clone();
                let previous_local_corpus_revision = this.quick_switcher_last_local_corpus_revision;
                this.quick_switcher_query_seq = this.quick_switcher_query_seq.wrapping_add(1);
                let seq = this.quick_switcher_query_seq;
                let query_observed_at = Instant::now();
                let query_observed_at_local = query_observed_at;
                let query_observed_at_remote = query_observed_at;

                // Keep the query in sync, but compute results off the keystroke path.
                this.models.quick_switcher.query = query.clone();

                if query.trim().is_empty() {
                    this.quick_switcher_last_local_query.clear();
                    this.quick_switcher_last_local_matched_entry_indices = Arc::new(Vec::new());
                    this.quick_switcher_last_local_corpus_revision = 0;
                    this.quick_switcher_remote_dispatch_started_at.clear();
                    this.models.quick_switcher.loading_messages = false;
                    this.models.update_quick_switcher_query(query);
                    tracing::debug!(
                        target: "zbase.quick_switcher.perf",
                        seq,
                        observer_ms = observer_started_at.elapsed().as_millis(),
                        "quick_switcher_input_observer empty_query"
                    );
                    cx.notify();
                    return;
                }
                let run_remote_search =
                    query.trim().chars().count() >= QUICK_SWITCHER_REMOTE_MIN_QUERY_CHARS;
                this.models.quick_switcher.loading_messages = run_remote_search;
                cx.notify();

                // Debounce local fuzzy results slightly so text input stays responsive.
                cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                    let background = cx.background_executor().clone();
                    let mut async_app = cx.clone();
                    let query = query_for_local.clone();
                    let corpus = local_corpus.clone();
                    let affinity_by_conversation_id = quick_switcher_affinity.clone();
                    let previous_query = previous_local_query.clone();
                    let previous_matches = previous_local_matches.clone();
                    let corpus_revision = corpus.revision;
                    let incremental_expected = !previous_query.trim().is_empty()
                        && query.trim().starts_with(previous_query.trim())
                        && previous_local_corpus_revision == corpus_revision
                        && !previous_matches.is_empty();
                    async move {
                        background.timer(local_debounce).await;
                        let local_compute_started_at = Instant::now();
                        let local_output: QuickSwitcherLocalSearchOutput =
                            compute_quick_switcher_local_results(
                            &query,
                            &corpus,
                            Some(&previous_query),
                            previous_matches.as_ref().as_slice(),
                            Some(previous_local_corpus_revision),
                            Some(affinity_by_conversation_id.as_ref()),
                            now_unix_ms(),
                        );
                        let local_compute_ms = local_compute_started_at.elapsed().as_millis();
                        let _ = this.update(&mut async_app, move |this, cx| {
                            if this.quick_switcher_query_seq != seq {
                                return;
                            }
                            if !this.models.overlay.quick_switcher_open {
                                return;
                            }
                            this.models.quick_switcher.query = query.clone();
                            this.models.quick_switcher.results = local_output.results;
                            this.models.quick_switcher.selected_index = this
                                .models
                                .quick_switcher
                                .selected_index
                                .min(this.models.quick_switcher.results.len().saturating_sub(1));
                            this.models.quick_switcher.loading_messages = run_remote_search;
                            this.quick_switcher_last_local_query = query.clone();
                            this.quick_switcher_last_local_matched_entry_indices =
                                Arc::new(local_output.matched_entry_indices);
                            this.quick_switcher_last_local_corpus_revision = corpus_revision;
                            tracing::debug!(
                                target: "zbase.quick_switcher.perf",
                                seq,
                                local_compute_ms,
                                input_to_local_ms = query_observed_at_local.elapsed().as_millis(),
                                incremental_expected,
                                scanned_entries = local_output.scanned_entries,
                                channel_scanned_entries = local_output.channel_scanned_entries,
                                dm_scanned_entries = local_output.dm_scanned_entries,
                                rejected_candidates = local_output.rejected_candidates,
                                fuzzy_evaluated = local_output.fuzzy_evaluated,
                                matched_pool = this.quick_switcher_last_local_matched_entry_indices.len(),
                                rendered_results = this.models.quick_switcher.results.len(),
                                "quick_switcher_local_results_applied"
                            );
                            cx.notify();
                        });
                    }
                })
                .detach();

                if run_remote_search {
                    cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                        let background = cx.background_executor().clone();
                        let mut async_app = cx.clone();
                        let query = query_for_remote.clone();
                        async move {
                            background.timer(remote_debounce).await;
                            let _ = this.update(&mut async_app, move |this, cx| {
                                if this.quick_switcher_query_seq != seq {
                                    return;
                                }
                                if !this.models.overlay.quick_switcher_open {
                                    this.models.quick_switcher.loading_messages = false;
                                    return;
                                }
                                this.quick_switcher_remote_dispatch_started_at
                                    .insert(seq, query_observed_at_remote);
                                this.quick_switcher_remote_dispatch_started_at.retain(|key, _| {
                                    key.saturating_add(48) >= this.quick_switcher_query_seq
                                });
                                this.dispatch_ui_action(UiAction::QuickSwitcherSearch {
                                    seq,
                                    query: query.clone(),
                                });
                                tracing::debug!(
                                    target: "zbase.quick_switcher.perf",
                                    seq,
                                    input_to_remote_dispatch_ms =
                                        query_observed_at_remote.elapsed().as_millis(),
                                    remote_debounce_ms = remote_debounce.as_millis(),
                                    "quick_switcher_remote_dispatched"
                                );
                                cx.notify();
                            });
                        }
                    })
                    .detach();
                }
                tracing::debug!(
                    target: "zbase.quick_switcher.perf",
                    seq,
                    observer_ms = observer_started_at.elapsed().as_millis(),
                    local_debounce_ms = local_debounce.as_millis(),
                    remote_debounce_ms = remote_debounce.as_millis(),
                    run_remote_search,
                    prefix_extend,
                    query_len = query.trim().chars().count(),
                    conversation_count,
                    "quick_switcher_input_observer"
                );
            }),
            cx.observe(&self.new_chat_input, |this, input, cx| {
                if !this.models.overlay.new_chat_open {
                    return;
                }
                let query = input.read(cx).text();
                if this.models.new_chat.search_query == query {
                    return;
                }
                this.models.new_chat.search_query = query.clone();
                this.dispatch_ui_action(UiAction::NewChatSearchUsers { query });
                cx.notify();
            }),
            cx.observe(&self.emoji_picker_input, |this, input, cx| {
                if !this.models.overlay.emoji_picker_open {
                    return;
                }
                let query = input.read(cx).text();
                if this.models.emoji_picker.query == query {
                    return;
                }
                this.models.set_emoji_picker_query(query);
                cx.notify();
            }),
            cx.observe(&self.file_upload_caption_input, |this, input, cx| {
                if this.models.overlay.file_upload_lightbox.is_none() {
                    return;
                }
                let caption = input.read(cx).text();
                this.models.file_upload_update_caption(caption);
            }),
            cx.observe(&self.find_in_chat_input, |this, input, cx| {
                if !this.models.find_in_chat.open {
                    return;
                }
                let query = input.read(cx).text();
                if this.models.find_in_chat.query == query {
                    return;
                }
                this.models.find_in_chat.query_seq = this.models.find_in_chat.query_seq.wrapping_add(1);
                let seq = this.models.find_in_chat.query_seq;
                this.models.find_in_chat.set_query(query.clone());
                if query.trim().is_empty() {
                    cx.notify();
                    return;
                }
                let Some(conversation_id) = this.models.find_in_chat.conversation_id.clone() else {
                    this.models.find_in_chat.loading = false;
                    cx.notify();
                    return;
                };
                this.dispatch_ui_action(UiAction::FindInChatSearch {
                    seq,
                    conversation_id,
                    query,
                });
                cx.notify();
            }),
            cx.observe(&self.search_input, |this, input, cx| {
                let query = input.read(cx).text();
                this.models.sync_live_search_query(query.clone());
                this.dispatch_ui_action(UiAction::SetSearchQuery(query));
            }),
            cx.observe(&self.sidebar_filter_input, |this, input, cx| {
                let filter = input.read(cx).text();
                this.models.set_sidebar_filter(filter.clone());
                this.dispatch_ui_action(UiAction::SetSidebarFilter(filter));
            }),
        ];

        if !self.bench_skip_backend {
            self.start_backend_poll_loop(cx);
        } else {
            tracing::warn!("bench.backend.skip enabled");
        }
        if let Some(message_count) =
            env_usize(ENV_BENCH_TIMELINE_MESSAGES).filter(|count| *count > 0)
        {
            self.inflate_bench_timeline_messages(message_count);
            tracing::warn!("bench.timeline.synthetic_messages={message_count}");
            self.refresh(cx);
        }
        if let Some(script) = self.bench_script {
            tracing::warn!(
                "bench.script.enabled scenario={} tick_ms={}",
                script.scenario.slug(),
                script.tick.as_millis()
            );
            self.start_bench_script_loop(cx);
        }
        if self.perf_harness.config().autostart {
            self.start_perf_capture(None, cx);
        }
    }

    fn start_backend_poll_loop(&mut self, cx: &mut Context<Self>) {
        cx.spawn(|this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let background = cx.background_executor().clone();
            let mut async_app = cx.clone();
            async move {
                let _ = this.update(&mut async_app, |this, cx| {
                    this.drain_backend_events(cx);
                });
                loop {
                    let sleep_for =
                        match this.update(&mut async_app, |this, _| this.backend_poll_interval()) {
                            Ok(duration) => duration,
                            Err(_) => break,
                        };
                    background.timer(sleep_for).await;
                    if this
                        .update(&mut async_app, |this, cx| {
                            this.drain_backend_events(cx);
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            }
        })
        .detach();
    }

    fn backend_poll_interval(&self) -> Duration {
        if !self.pending_backend_events.is_empty() {
            return BACKEND_POLL_BOOT_INTERVAL;
        }
        match self.app_store.snapshot().app.boot_phase {
            BootPhase::Ready | BootPhase::Degraded | BootPhase::FatalError => {
                BACKEND_POLL_READY_INTERVAL
            }
            BootPhase::Launching
            | BootPhase::HydratingLocalState
            | BootPhase::ConnectingBackend => BACKEND_POLL_BOOT_INTERVAL,
        }
    }

    fn drain_backend_events(&mut self, cx: &mut Context<Self>) {
        let drain_t0 = Instant::now();
        self.sync_drafts_from_inputs(cx);
        let polled_events = self.backend_router.poll_backends();
        self.perf_harness.record_backend_poll(polled_events.len());
        if !polled_events.is_empty() {
            self.pending_backend_events.extend(polled_events);
        }
        let mut events = Vec::with_capacity(MAX_BACKEND_EVENTS_PER_DRAIN);
        while events.len() < MAX_BACKEND_EVENTS_PER_DRAIN {
            let Some(event) = self.pending_backend_events.pop_front() else {
                break;
            };
            events.push(event);
        }
        let mut needs_refresh = self.drain_video_decode_results();
        needs_refresh |= self.drain_og_fetch_results();
        let quick_switcher_typing = self.models.overlay.quick_switcher_open
            && !self.models.quick_switcher.query.trim().is_empty();
        if events.is_empty() {
            if self.sync_cache.deferred_model_sync && !quick_switcher_typing {
                self.sync_cache.deferred_model_sync = false;
                self.sync_models_from_store();
                self.request_mark_conversation_read_if_needed();
                needs_refresh = true;
            } else if !quick_switcher_typing && self.request_mark_conversation_read_if_needed() {
                needs_refresh = true;
            }
            self.perf_harness
                .record_duration(PerfTimer::DrainBackendEvents, drain_t0.elapsed());
            if needs_refresh {
                self.refresh(cx);
            }
            return;
        }

        let mut pending_effects = Vec::new();
        let mut needs_model_sync = false;
        let defer_model_sync = quick_switcher_typing || self.text_input_activity_recent();
        let mut deferred_events_applied = false;
        for event in events {
            if defer_model_sync {
                match &event {
                    crate::state::event::BackendEvent::BootstrapLoaded { payload, .. } => {
                        for binding in &payload.workspace_bindings {
                            self.backend_router
                                .register_workspace_binding(binding.clone());
                        }
                        for binding in &payload.conversation_bindings {
                            self.backend_router
                                .register_conversation_binding(binding.clone());
                        }
                        for binding in &payload.message_bindings {
                            self.backend_router
                                .register_message_binding(binding.clone());
                        }
                    }
                    crate::state::event::BackendEvent::WorkspaceConversationsExtended {
                        conversation_bindings,
                        ..
                    } => {
                        for binding in conversation_bindings {
                            self.backend_router
                                .register_conversation_binding(binding.clone());
                        }
                    }
                    crate::state::event::BackendEvent::UserProfileUpserted {
                        user_id,
                        display_name,
                        ..
                    } => {
                        let updated = self
                            .models
                            .upsert_quick_switcher_profile_name(user_id, display_name);
                        if updated {
                            self.schedule_quick_switcher_corpus_rebuild(cx);
                        }
                    }
                    crate::state::event::BackendEvent::KeybaseNotifyStub {
                        method,
                        payload_preview,
                    } => {
                        if !self.keybase_inspector.open
                            && !self.models.overlay.quick_switcher_open
                            && !self.quick_switcher_indexing_active
                        {
                            continue;
                        }
                        let previous_seq = self.keybase_inspector.seq;
                        self.record_keybase_stub_event(method.clone(), payload_preview.clone());
                        let indexing_changed = self.update_quick_switcher_indexing_from_internal(
                            method,
                            payload_preview.as_deref(),
                        );
                        if indexing_changed && self.models.overlay.quick_switcher_open {
                            needs_refresh = true;
                        }
                        if self.keybase_inspector.open && self.keybase_inspector.seq != previous_seq
                        {
                            needs_refresh = true;
                        }
                        continue;
                    }
                    crate::state::event::BackendEvent::TypingUpdated {
                        conversation_id,
                        users,
                    } => {
                        let changed = self.apply_typing_indicator_update(conversation_id, users);
                        let _ = self.app_store.dispatch_backend(
                            crate::state::event::BackendEvent::TypingUpdated {
                                conversation_id: conversation_id.clone(),
                                users: users.clone(),
                            },
                        );
                        if changed {
                            needs_refresh = true;
                        }
                        continue;
                    }
                    crate::state::event::BackendEvent::SearchResults {
                        query_id,
                        results,
                        is_complete,
                    } if query_id.0.starts_with("quick-switcher-") => {
                        let expected_query =
                            format!("quick-switcher-{}", self.quick_switcher_query_seq);
                        if query_id.0 != expected_query {
                            continue;
                        }
                        let query_seq = quick_switcher_seq_from_query_id(&query_id.0);
                        if self.models.overlay.quick_switcher_open {
                            self.models.apply_quick_switcher_message_results(results);
                            self.models.quick_switcher.loading_messages = !*is_complete;
                            if let Some(query_seq) = query_seq {
                                if let Some(dispatched_at) = self
                                    .quick_switcher_remote_dispatch_started_at
                                    .remove(&query_seq)
                                {
                                    tracing::debug!(
                                        target: "zbase.quick_switcher.perf",
                                        seq = query_seq,
                                        remote_dispatch_to_apply_ms =
                                            dispatched_at.elapsed().as_millis(),
                                        is_complete = *is_complete,
                                        result_count = results.len(),
                                        "quick_switcher_remote_results_applied"
                                    );
                                } else {
                                    tracing::debug!(
                                        target: "zbase.quick_switcher.perf",
                                        seq = query_seq,
                                        is_complete = *is_complete,
                                        result_count = results.len(),
                                        "quick_switcher_remote_results_applied_without_dispatch_marker"
                                    );
                                }
                            }
                            needs_refresh = true;
                        }
                        continue;
                    }
                    crate::state::event::BackendEvent::SearchResults {
                        query_id,
                        results,
                        is_complete,
                    } if query_id.0.starts_with("find-in-chat-") => {
                        let expected_query =
                            format!("find-in-chat-{}", self.models.find_in_chat.query_seq);
                        if query_id.0 != expected_query {
                            continue;
                        }
                        if self.models.find_in_chat.open {
                            self.models
                                .find_in_chat
                                .apply_results(results.clone(), *is_complete);
                            needs_refresh = true;
                        }
                        continue;
                    }
                    _ => {}
                }
                pending_effects.extend(self.app_store.dispatch_backend(event));
                needs_model_sync = true;
                deferred_events_applied = true;
                continue;
            }

            match &event {
                crate::state::event::BackendEvent::BootstrapLoaded { payload, .. } => {
                    for binding in &payload.workspace_bindings {
                        self.backend_router
                            .register_workspace_binding(binding.clone());
                    }
                    for binding in &payload.conversation_bindings {
                        self.backend_router
                            .register_conversation_binding(binding.clone());
                    }
                    for binding in &payload.message_bindings {
                        self.backend_router
                            .register_message_binding(binding.clone());
                    }
                    needs_model_sync = true;
                    needs_refresh = true;
                }
                crate::state::event::BackendEvent::WorkspaceConversationsExtended {
                    conversation_bindings,
                    ..
                } => {
                    for binding in conversation_bindings {
                        self.backend_router
                            .register_conversation_binding(binding.clone());
                    }
                    needs_model_sync = true;
                    needs_refresh = true;
                }
                crate::state::event::BackendEvent::KeybaseNotifyStub {
                    method,
                    payload_preview,
                } => {
                    if !self.keybase_inspector.open
                        && !self.models.overlay.quick_switcher_open
                        && !self.quick_switcher_indexing_active
                    {
                        continue;
                    }
                    let previous_seq = self.keybase_inspector.seq;
                    self.record_keybase_stub_event(method.clone(), payload_preview.clone());
                    let indexing_changed = self.update_quick_switcher_indexing_from_internal(
                        method,
                        payload_preview.as_deref(),
                    );
                    if indexing_changed && self.models.overlay.quick_switcher_open {
                        needs_refresh = true;
                    }
                    if self.keybase_inspector.open && self.keybase_inspector.seq != previous_seq {
                        needs_refresh = true;
                    }
                    // Notify stubs do not change AppStore state; skip reducer work.
                    continue;
                }
                crate::state::event::BackendEvent::TypingUpdated {
                    conversation_id,
                    users,
                } => {
                    let changed = self.apply_typing_indicator_update(conversation_id, users);
                    let _ = self.app_store.dispatch_backend(
                        crate::state::event::BackendEvent::TypingUpdated {
                            conversation_id: conversation_id.clone(),
                            users: users.clone(),
                        },
                    );
                    if changed {
                        needs_refresh = true;
                    }
                    continue;
                }
                crate::state::event::BackendEvent::SearchResults {
                    query_id,
                    results,
                    is_complete,
                } if query_id.0.starts_with("quick-switcher-") => {
                    let expected_query =
                        format!("quick-switcher-{}", self.quick_switcher_query_seq);
                    if query_id.0 != expected_query {
                        continue;
                    }
                    let query_seq = quick_switcher_seq_from_query_id(&query_id.0);
                    if self.models.overlay.quick_switcher_open {
                        self.models.apply_quick_switcher_message_results(results);
                        self.models.quick_switcher.loading_messages = !*is_complete;
                        if let Some(query_seq) = query_seq {
                            if let Some(dispatched_at) = self
                                .quick_switcher_remote_dispatch_started_at
                                .remove(&query_seq)
                            {
                                tracing::debug!(
                                    target: "zbase.quick_switcher.perf",
                                    seq = query_seq,
                                    remote_dispatch_to_apply_ms =
                                        dispatched_at.elapsed().as_millis(),
                                    is_complete = *is_complete,
                                    result_count = results.len(),
                                    "quick_switcher_remote_results_applied"
                                );
                            } else {
                                tracing::debug!(
                                    target: "zbase.quick_switcher.perf",
                                    seq = query_seq,
                                    is_complete = *is_complete,
                                    result_count = results.len(),
                                    "quick_switcher_remote_results_applied_without_dispatch_marker"
                                );
                            }
                        }
                        needs_refresh = true;
                    }
                    continue;
                }
                crate::state::event::BackendEvent::SearchResults {
                    query_id,
                    results,
                    is_complete,
                } if query_id.0.starts_with("find-in-chat-") => {
                    let expected_query =
                        format!("find-in-chat-{}", self.models.find_in_chat.query_seq);
                    if query_id.0 != expected_query {
                        continue;
                    }
                    if self.models.find_in_chat.open {
                        self.models
                            .find_in_chat
                            .apply_results(results.clone(), *is_complete);
                        needs_refresh = true;
                    }
                    continue;
                }
                crate::state::event::BackendEvent::WorkspaceSynced { .. }
                | crate::state::event::BackendEvent::ConversationLoaded { .. }
                | crate::state::event::BackendEvent::PresenceUpdated { .. }
                | crate::state::event::BackendEvent::CallUpdated(_) => {
                    // These backend events are currently reducer no-ops for this UI tree.
                    continue;
                }
                _ => {
                    needs_model_sync = true;
                    needs_refresh = true;
                }
            }
            pending_effects.extend(self.app_store.dispatch_backend(event));
        }

        if deferred_events_applied {
            self.sync_cache.deferred_model_sync = true;
        } else if self.sync_cache.deferred_model_sync && !defer_model_sync {
            self.sync_cache.deferred_model_sync = false;
            needs_model_sync = true;
            needs_refresh = true;
        }

        if needs_model_sync {
            loop {
                if pending_effects.is_empty() {
                    break;
                }

                let Ok(events) = self.backend_router.apply_effects(&pending_effects) else {
                    break;
                };
                if events.is_empty() {
                    break;
                }

                pending_effects.clear();
                for event in events {
                    pending_effects.extend(self.app_store.dispatch_backend(event));
                }
            }
        }

        self.perf_harness
            .record_duration(PerfTimer::DrainBackendEvents, drain_t0.elapsed());
        if !needs_refresh {
            return;
        }
        let should_sync_models_now =
            needs_model_sync && !(defer_model_sync && deferred_events_applied);
        if should_sync_models_now {
            self.sync_models_from_store();
            let _ = self.request_mark_conversation_read_if_needed();
        }
        self.check_splash_dismiss(cx);
        self.refresh(cx);
    }

    fn drain_video_decode_results(&mut self) -> bool {
        let mut changed = false;
        while let Ok(outcome) = self.video_result_receiver.try_recv() {
            self.video_pending_urls.remove(&outcome.cache_key);
            if let Some(render_image) = outcome.render_image {
                self.insert_video_render_cache_entry(outcome.cache_key, render_image);
                changed = true;
            } else {
                self.failed_video_urls.insert(outcome.cache_key);
            }
        }
        changed
    }

    fn insert_video_render_cache_entry(
        &mut self,
        cache_key: String,
        render_image: Arc<RenderImage>,
    ) {
        let is_new_key = !self.video_render_cache.contains_key(&cache_key);
        self.video_render_cache
            .insert(cache_key.clone(), render_image);
        if is_new_key {
            self.video_cache_order.push_back(cache_key);
        }
        while self.video_cache_order.len() > MAX_VIDEO_RENDER_CACHE_ENTRIES {
            if let Some(evicted_key) = self.video_cache_order.pop_front() {
                self.video_render_cache.remove(&evicted_key);
                self.video_pending_urls.remove(&evicted_key);
            }
        }
    }

    fn schedule_video_preview_decodes_for_messages(&mut self, messages: &[MessageRecord]) {
        for message in messages {
            self.schedule_video_preview_decodes_for_previews(&message.link_previews);
        }
    }

    fn schedule_video_preview_decodes_for_search_results(&mut self) {
        let previews = self
            .models
            .search
            .results
            .iter()
            .flat_map(|result| result.message.link_previews.iter())
            .cloned()
            .collect::<Vec<_>>();
        self.schedule_video_preview_decodes_for_previews(&previews);
    }

    fn drain_og_fetch_results(&mut self) -> bool {
        let mut changed = false;
        while let Ok(result) = self.og_result_receiver.try_recv() {
            if result.preview.is_some() {
                changed = true;
            }
            self.og_service.apply_result(result);
        }
        if changed {
            self.apply_og_previews_to_timeline();
            self.timeline_row_render_cache.invalidate();
        }
        changed
    }

    fn apply_og_previews_to_timeline(&mut self) {
        apply_og_previews_to_messages(
            &self.og_service,
            self.models
                .timeline
                .rows
                .iter_mut()
                .filter_map(|row| {
                    if let crate::models::timeline_model::TimelineRow::Message(message_row) = row {
                        Some(&mut message_row.message)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
        );
        apply_og_previews_to_messages(
            &self.og_service,
            self.models.thread_pane.replies.iter_mut().collect(),
        );
    }

    fn schedule_og_fetches_for_messages(&mut self, messages: &[MessageRecord]) {
        for message in messages {
            let urls = extract_link_urls_from_fragments(&message.fragments);
            for url in &urls {
                self.og_service.schedule_fetch(url, &self.og_result_sender);
            }
        }
    }

    fn schedule_video_preview_decodes_for_previews(&mut self, previews: &[LinkPreview]) {
        for preview in previews {
            if !preview.is_video {
                continue;
            }
            let Some(video_url) = link_preview_video_url(preview) else {
                continue;
            };
            let cache_key = video_preview_cache_key(&video_url);
            if self.video_render_cache.contains_key(&cache_key)
                || self.failed_video_urls.contains(&cache_key)
                || !self.video_pending_urls.insert(cache_key.clone())
            {
                continue;
            }
            spawn_video_preview_decode(cache_key, video_url, self.video_result_sender.clone());
        }
    }

    fn active_workspace_route(&self) -> Route {
        Route::WorkspaceHome {
            workspace_id: self.models.app.active_workspace_id.clone(),
        }
    }

    fn activity_route(&self) -> Route {
        Route::Activity {
            workspace_id: self.models.app.active_workspace_id.clone(),
        }
    }

    fn current_conversation_route(&self) -> Route {
        route_for_conversation_id(
            &self.models.app.active_workspace_id,
            &self.models.conversation.summary.id,
        )
    }

    fn sync_inputs_from_models(&mut self, cx: &mut Context<Self>) {
        let quick_switcher_text = self.models.quick_switcher.query.clone();
        if self.quick_switcher_input.read(cx).text() != quick_switcher_text {
            self.quick_switcher_input.update(cx, |input, cx| {
                input.set_text(quick_switcher_text.clone(), cx)
            });
        }

        let new_chat_text = self.models.new_chat.search_query.clone();
        if self.new_chat_input.read(cx).text() != new_chat_text {
            self.new_chat_input
                .update(cx, |input, cx| input.set_text(new_chat_text.clone(), cx));
        }

        let emoji_picker_text = self.models.emoji_picker.query.clone();
        if self.emoji_picker_input.read(cx).text() != emoji_picker_text {
            self.emoji_picker_input.update(cx, |input, cx| {
                input.set_text(emoji_picker_text.clone(), cx)
            });
        }

        let file_upload_caption = self
            .models
            .overlay
            .file_upload_lightbox
            .as_ref()
            .and_then(|lightbox| lightbox.current_candidate())
            .map(|candidate| candidate.caption.clone())
            .unwrap_or_default();
        if self.file_upload_caption_input.read(cx).text() != file_upload_caption {
            self.file_upload_caption_input.update(cx, |input, cx| {
                input.set_text(file_upload_caption.clone(), cx)
            });
        }

        let find_in_chat_text = self.models.find_in_chat.query.clone();
        if self.find_in_chat_input.read(cx).text() != find_in_chat_text {
            self.find_in_chat_input.update(cx, |input, cx| {
                input.set_text(find_in_chat_text.clone(), cx)
            });
        }

        let search_text = self.models.search.query.clone();
        if self.search_input.read(cx).text() != search_text {
            self.search_input
                .update(cx, |input, cx| input.set_text(search_text.clone(), cx));
        }

        let composer_text = self.models.composer.draft_text.clone();
        if self.composer_input.read(cx).text() != composer_text {
            self.composer_input
                .update(cx, |input, cx| input.set_text(composer_text.clone(), cx));
        }

        let thread_text = self.models.thread_pane.reply_draft.clone();
        if self.thread_input.read(cx).text() != thread_text {
            self.thread_input
                .update(cx, |input, cx| input.set_text(thread_text.clone(), cx));
        }

        let sidebar_filter = self.models.sidebar.filter.clone();
        if self.sidebar_filter_input.read(cx).text() != sidebar_filter {
            self.sidebar_filter_input
                .update(cx, |input, cx| input.set_text(sidebar_filter.clone(), cx));
        }
    }

    fn capture_live_inputs(&mut self, cx: &mut Context<Self>) {
        self.models
            .update_quick_switcher_query(self.quick_switcher_input.read(cx).text());
        self.models.new_chat.search_query = self.new_chat_input.read(cx).text();
        self.models
            .set_emoji_picker_query(self.emoji_picker_input.read(cx).text());
        self.models.find_in_chat.query = self.find_in_chat_input.read(cx).text();
        self.models
            .sync_live_search_query(self.search_input.read(cx).text());
        self.models
            .update_composer_draft_text(self.composer_input.read(cx).text());
        self.models
            .update_thread_reply_draft(self.thread_input.read(cx).text());
        self.models
            .set_sidebar_filter(self.sidebar_filter_input.read(cx).text());
    }

    fn take_pasted_image(input: &Entity<TextField>, cx: &mut Context<Self>) -> Option<Vec<u8>> {
        let has_image = input.read(cx).pasted_image.is_some();
        if has_image {
            input.update(cx, |input, _| input.pasted_image.take())
        } else {
            None
        }
    }

    fn handle_pasted_image(
        &mut self,
        bytes: Vec<u8>,
        target: UploadTarget,
        cx: &mut Context<Self>,
    ) {
        use std::time::SystemTime;
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let path =
            std::env::temp_dir().join(format!("zbase-paste-{}-{ts}.png", std::process::id(),));
        if std::fs::write(&path, &bytes).is_ok() {
            self.open_file_upload_lightbox_with_paths(vec![path], target, cx);
        }
    }

    fn sync_drafts_from_inputs(&mut self, cx: &mut Context<Self>) {
        let started_at = Instant::now();

        if let Some(bytes) = Self::take_pasted_image(&self.composer_input, cx) {
            self.handle_pasted_image(bytes, UploadTarget::Composer, cx);
        }
        if let Some(bytes) = Self::take_pasted_image(&self.thread_input, cx) {
            self.handle_pasted_image(bytes, UploadTarget::Thread, cx);
        }

        if self.composer_input.read(cx).up_at_top_triggered {
            self.composer_input.update(cx, |input, _| {
                input.up_at_top_triggered = false;
            });
            self.edit_last_own_message(cx);
        }

        let composer_text = self
            .composer_input
            .read(cx)
            .text_if_different(&self.models.composer.draft_text);
        if let Some(composer_text) = composer_text {
            let was_near_bottom = self.timeline_is_near_bottom();
            let key = DraftKey::Conversation(self.models.composer.conversation_id.clone());
            self.models
                .update_composer_draft_text(composer_text.clone());
            self.dispatch_ui_action(UiAction::UpdateDraft {
                key,
                text: composer_text,
            });
            self.last_text_input_activity = Some(Instant::now());
            if was_near_bottom {
                scroll_list_to_bottom(&self.timeline_list_state);
            }
        }

        let thread_text = self
            .thread_input
            .read(cx)
            .text_if_different(&self.models.thread_pane.reply_draft);
        if let Some(thread_text) = thread_text {
            self.models.update_thread_reply_draft(thread_text.clone());
            self.last_text_input_activity = Some(Instant::now());
            if let Some(root_id) = self.models.thread_pane.root_message_id.clone() {
                self.dispatch_ui_action(UiAction::UpdateDraft {
                    key: DraftKey::Thread(root_id),
                    text: thread_text,
                });
            }
        }
        self.sync_inline_autocomplete_state(cx);

        let elapsed = started_at.elapsed();
        self.perf_harness
            .record_duration(PerfTimer::ComposerInputObserver, elapsed);
        if self.perf_harness.is_capturing() && elapsed.as_millis() > 2 {
            tracing::warn!("composer_input_observer took {elapsed:?}");
        }
    }

    fn sync_inline_autocomplete_state(&mut self, cx: &mut Context<Self>) {
        self.sync_autocomplete_for_target(InputAutocompleteTarget::Composer, cx);
        self.sync_autocomplete_for_target(InputAutocompleteTarget::Thread, cx);
    }

    fn set_autocomplete_navigation_enabled_for_target(
        &mut self,
        target: InputAutocompleteTarget,
        enabled: bool,
        cx: &mut Context<Self>,
    ) {
        match target {
            InputAutocompleteTarget::Composer => {
                self.composer_input.update(cx, |field, cx| {
                    field.set_autocomplete_navigation_enabled(enabled, cx);
                });
            }
            InputAutocompleteTarget::Thread => {
                self.thread_input.update(cx, |field, cx| {
                    field.set_autocomplete_navigation_enabled(enabled, cx);
                });
            }
        }
    }

    fn sync_autocomplete_for_target(
        &mut self,
        target: InputAutocompleteTarget,
        cx: &mut Context<Self>,
    ) {
        let (text, cursor, current_state) = match target {
            InputAutocompleteTarget::Composer => (
                self.composer_input.read(cx).text(),
                self.composer_input.read(cx).cursor_offset(),
                self.models.composer.autocomplete.clone(),
            ),
            InputAutocompleteTarget::Thread => (
                self.thread_input.read(cx).text(),
                self.thread_input.read(cx).cursor_offset(),
                self.models.thread_pane.reply_autocomplete.clone(),
            ),
        };

        let detected = detect_autocomplete_trigger(&text, cursor);
        let detected_signature = detected
            .as_ref()
            .map(|matched| AutocompleteDismissSignature {
                trigger: match matched.kind {
                    AutocompleteTriggerKind::Mention => '@',
                    AutocompleteTriggerKind::Emoji => ':',
                },
                trigger_offset: matched.trigger_offset,
                cursor,
                query: matched.query.clone(),
            });

        // If the user dismissed autocomplete (Escape), keep it closed until the input context
        // (trigger/query/cursor) changes.
        let dismissed = match target {
            InputAutocompleteTarget::Composer => self.dismissed_composer_autocomplete.clone(),
            InputAutocompleteTarget::Thread => self.dismissed_thread_autocomplete.clone(),
        };
        if let Some(dismissed) = dismissed {
            if detected_signature.as_ref() == Some(&dismissed) {
                if current_state.is_some() {
                    match target {
                        InputAutocompleteTarget::Composer => {
                            self.models.set_composer_autocomplete(None)
                        }
                        InputAutocompleteTarget::Thread => {
                            self.models.set_thread_autocomplete(None)
                        }
                    }
                }
                self.set_autocomplete_navigation_enabled_for_target(target, false, cx);
                return;
            }
            // Context changed; allow autocomplete to show again.
            match target {
                InputAutocompleteTarget::Composer => self.dismissed_composer_autocomplete = None,
                InputAutocompleteTarget::Thread => self.dismissed_thread_autocomplete = None,
            }
        }

        let mut next_state = detected
            .and_then(|matched| self.build_autocomplete_state(matched))
            .filter(|state| !state.candidates.is_empty());
        if let (Some(next), Some(current)) = (next_state.as_mut(), current_state.as_ref()) {
            // Preserve selection when only the highlighted row changed (e.g. Up/Down navigation).
            // The input observer can re-run this sync even without text edits.
            if next.trigger == current.trigger
                && next.trigger_offset == current.trigger_offset
                && next.query == current.query
                && next.candidates == current.candidates
            {
                let max_ix = next.candidates.len().saturating_sub(1);
                next.selected_index = current.selected_index.min(max_ix);
            }
        }
        if next_state == current_state {
            return;
        }
        match target {
            InputAutocompleteTarget::Composer => self.models.set_composer_autocomplete(next_state),
            InputAutocompleteTarget::Thread => self.models.set_thread_autocomplete(next_state),
        }
        let enabled = match target {
            InputAutocompleteTarget::Composer => self
                .models
                .composer
                .autocomplete
                .as_ref()
                .is_some_and(|state| !state.candidates.is_empty()),
            InputAutocompleteTarget::Thread => self
                .models
                .thread_pane
                .reply_autocomplete
                .as_ref()
                .is_some_and(|state| !state.candidates.is_empty()),
        };
        self.set_autocomplete_navigation_enabled_for_target(target, enabled, cx);
    }

    fn current_autocomplete_dismiss_signature(
        &self,
        target: InputAutocompleteTarget,
        cx: &mut Context<Self>,
    ) -> Option<AutocompleteDismissSignature> {
        match target {
            InputAutocompleteTarget::Composer => {
                self.models.composer.autocomplete.as_ref().map(|state| {
                    AutocompleteDismissSignature {
                        trigger: state.trigger,
                        trigger_offset: state.trigger_offset,
                        cursor: self.composer_input.read(cx).cursor_offset(),
                        query: state.query.clone(),
                    }
                })
            }
            InputAutocompleteTarget::Thread => self
                .models
                .thread_pane
                .reply_autocomplete
                .as_ref()
                .map(|state| AutocompleteDismissSignature {
                    trigger: state.trigger,
                    trigger_offset: state.trigger_offset,
                    cursor: self.thread_input.read(cx).cursor_offset(),
                    query: state.query.clone(),
                }),
        }
    }

    fn build_autocomplete_state(&self, matched: TriggerMatch) -> Option<AutocompleteState> {
        let candidates = match matched.kind {
            AutocompleteTriggerKind::Mention => self
                .models
                .mention_autocomplete_candidates(&matched.query, INLINE_AUTOCOMPLETE_MAX_RESULTS),
            AutocompleteTriggerKind::Emoji => {
                let custom_items = self.current_conversation_custom_emoji_items();
                AppModels::emoji_autocomplete_candidates(
                    &matched.query,
                    &custom_items,
                    INLINE_AUTOCOMPLETE_MAX_RESULTS,
                )
            }
        };
        if candidates.is_empty() {
            return None;
        }
        Some(AutocompleteState {
            trigger: match matched.kind {
                AutocompleteTriggerKind::Mention => '@',
                AutocompleteTriggerKind::Emoji => ':',
            },
            query: matched.query,
            trigger_offset: matched.trigger_offset,
            selected_index: 0,
            candidates,
        })
    }

    fn current_conversation_custom_emoji_items(&self) -> Vec<EmojiPickerItem> {
        let snapshot = self.app_store.snapshot();
        let supports_custom_emoji = snapshot
            .backend
            .accounts
            .values()
            .find(|account| matches!(account.connection_state, ConnectionState::Connected))
            .or_else(|| snapshot.backend.accounts.values().next())
            .map(|account| account.capabilities.supports_custom_emoji)
            .unwrap_or(false);
        if !supports_custom_emoji {
            return Vec::new();
        }
        let Some(conversation_id) = snapshot.timeline.conversation_id.as_ref() else {
            return Vec::new();
        };
        let mut custom_items = snapshot
            .backend
            .conversation_emojis
            .get(conversation_id)
            .into_iter()
            .flat_map(|index| index.values())
            .map(|emoji| EmojiPickerItem::Custom {
                alias: emoji.alias.clone(),
                unicode: emoji.unicode.clone(),
                asset_path: emoji.asset_path.clone(),
            })
            .collect::<Vec<_>>();
        custom_items.sort_by_key(|item| item.key());
        custom_items
    }

    fn start_perf_capture(&mut self, label_override: Option<String>, cx: &mut Context<Self>) {
        if !self.perf_harness.start_capture(label_override) {
            return;
        }

        self.perf_capture_generation = self.perf_capture_generation.wrapping_add(1);
        if let Some(duration) = self.perf_harness.config().duration {
            self.schedule_perf_capture_stop(self.perf_capture_generation, duration, cx);
        }
    }

    fn stop_perf_capture(&mut self) {
        if !self.perf_harness.stop_capture() {
            return;
        }
        self.perf_capture_generation = self.perf_capture_generation.wrapping_add(1);
        if self.perf_exit_on_stop {
            tracing::warn!("bench.capture.exit_on_stop enabled");
            std::process::exit(0);
        }
    }

    fn toggle_perf_capture(&mut self, cx: &mut Context<Self>) {
        if self.perf_harness.is_capturing() {
            self.stop_perf_capture();
        } else {
            self.start_perf_capture(None, cx);
        }
    }

    fn schedule_perf_capture_stop(
        &mut self,
        generation: u64,
        duration: Duration,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let background = cx.background_executor().clone();
            let mut async_app = cx.clone();
            async move {
                background.timer(duration).await;
                let _ = this.update(&mut async_app, move |this, _cx| {
                    if this.perf_capture_generation == generation
                        && this.perf_harness.is_capturing()
                    {
                        this.stop_perf_capture();
                    }
                });
            }
        })
        .detach();
    }

    fn start_bench_script_loop(&mut self, cx: &mut Context<Self>) {
        let Some(script) = self.bench_script else {
            return;
        };
        let tick = script.tick;
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let background = cx.background_executor().clone();
            let mut async_app = cx.clone();
            async move {
                loop {
                    background.timer(tick).await;
                    let _ = this.update(&mut async_app, |this, cx| {
                        this.run_bench_script_tick(cx);
                    });
                }
            }
        })
        .detach();
    }

    fn run_bench_script_tick(&mut self, cx: &mut Context<Self>) {
        if !self.perf_harness.is_capturing() {
            return;
        }
        let Some(script) = self.bench_script else {
            return;
        };

        self.ensure_bench_profile_initialized();
        self.bench_script_step = self.bench_script_step.wrapping_add(1);
        match script.scenario {
            BenchScriptScenario::TimelineScroll => {
                let direction = if (self.bench_script_step / 180).is_multiple_of(2) {
                    1.0
                } else {
                    -1.0
                };
                self.timeline_list_state.scroll_by(px(96. * direction));
                self.models
                    .move_timeline_highlight(if direction > 0.0 { 1 } else { -1 });
                if self.bench_script_step.is_multiple_of(3) {
                    self.models.move_sidebar_highlight(1);
                }
                if self.bench_script_step.is_multiple_of(420) {
                    self.scroll_timeline_list_to_bottom();
                }
                let _ = self.sync_scroll_indicators();
                self.refresh(cx);
            }
            BenchScriptScenario::ProfileScroll => {
                self.run_bench_profile_scroll_tick(cx);
            }
            BenchScriptScenario::SidebarFilter => {
                const FILTERS: [&str; 10] =
                    ["", "g", "ge", "gen", "d", "de", "des", "a", "al", "ali"];
                let filter = FILTERS[(self.bench_script_step as usize) % FILTERS.len()].to_string();
                self.models.set_sidebar_filter(filter.clone());
                self.dispatch_ui_action(UiAction::SetSidebarFilter(filter));
                self.refresh(cx);
            }
            BenchScriptScenario::SyncHeavy => {
                self.sync_models_from_store();
                self.refresh(cx);
            }
            BenchScriptScenario::ComposerTyping => {
                const SAMPLE: &str = "typing benchmark sentence with wraps and spaces for realistic composer workload ";
                let mut next_text = self.composer_input.read(cx).text();
                if self.bench_script_step.is_multiple_of(420) {
                    next_text.clear();
                } else if self.bench_script_step.is_multiple_of(61) {
                    next_text.push('\n');
                } else if self.bench_script_step.is_multiple_of(97) {
                    let _ = next_text.pop();
                } else {
                    let bytes = SAMPLE.as_bytes();
                    let idx = (self.bench_script_step as usize) % bytes.len();
                    next_text.push(bytes[idx] as char);
                }

                if next_text.len() > 512 {
                    let keep_from = next_text.len().saturating_sub(384);
                    next_text = next_text[keep_from..].to_string();
                }

                self.composer_input
                    .update(cx, |input, cx| input.set_text(next_text, cx));
                self.sync_drafts_from_inputs(cx);
                self.refresh(cx);
            }
            BenchScriptScenario::ComposerPaste => {
                if self.bench_script_step.is_multiple_of(96) {
                    self.composer_input
                        .update(cx, |input, cx| input.set_text(String::new(), cx));
                } else if self.bench_script_step.is_multiple_of(48) {
                    let payload = Self::bench_composer_paste_payload(self.bench_script_step);
                    self.composer_input.update(cx, |input, cx| {
                        input.set_text(String::new(), cx);
                        input.insert_text(&payload, cx);
                    });
                }
                self.sync_drafts_from_inputs(cx);
                self.refresh(cx);
            }
            BenchScriptScenario::ThreadOpen => {
                self.run_bench_thread_open_tick(cx);
            }
        }
    }

    fn run_bench_profile_scroll_tick(&mut self, cx: &mut Context<Self>) {
        let direction = if (self.bench_script_step / 180).is_multiple_of(2) {
            -96.0
        } else {
            96.0
        };
        let mut moved = false;
        moved |= Self::scroll_handle_by(&self.profile_social_scroll, direction);
        moved |= Self::scroll_handle_by(&self.profile_scroll, direction);
        if self.bench_script_step.is_multiple_of(420) || !moved {
            Self::scroll_handle_to_top(&self.profile_social_scroll);
            Self::scroll_handle_to_top(&self.profile_scroll);
        }
        self.refresh(cx);
    }

    fn scroll_handle_by(handle: &ScrollHandle, delta_y_px: f32) -> bool {
        let max = handle.max_offset();
        if max.height <= px(0.) {
            return false;
        }
        let mut offset = handle.offset();
        let next_y = (offset.y + px(delta_y_px)).clamp(-max.height, px(0.));
        if next_y == offset.y {
            return false;
        }
        offset.y = next_y;
        handle.set_offset(offset);
        true
    }

    fn scroll_handle_to_top(handle: &ScrollHandle) {
        let mut offset = handle.offset();
        if offset.y == px(0.) {
            return;
        }
        offset.y = px(0.);
        handle.set_offset(offset);
    }

    fn ensure_bench_thread_conversation(&mut self, cx: &mut Context<Self>) -> bool {
        let Some(conversation_id) = self.bench_thread_conversation_id.clone() else {
            return true;
        };
        if self.app_store.snapshot().timeline.conversation_id.as_ref() == Some(&conversation_id) {
            return true;
        }

        let route =
            route_for_conversation_id(&self.models.app.active_workspace_id, &conversation_id);
        self.models.navigate_to(route.clone());
        self.dispatch_ui_action(UiAction::Navigate(route.clone()));
        self.models.expand_section_for_route(&route);
        self.reset_timeline_scroll_state();
        self.reset_thread_scroll_state();
        self.sync_inputs_from_models(cx);
        self.refresh(cx);
        false
    }

    fn bench_thread_root_candidate(&self) -> Option<MessageId> {
        let mut best: Option<(MessageId, u32)> = None;
        for row in &self.models.timeline.rows {
            let crate::models::timeline_model::TimelineRow::Message(message_row) = row else {
                continue;
            };
            let replies = message_row.message.thread_reply_count;
            if replies == 0 {
                continue;
            }
            let message_id = message_row.message.id.clone();
            best = match best {
                Some((best_id, best_replies))
                    if best_replies > replies
                        || (best_replies == replies
                            && message_id_is_after(&best_id, &message_id)) =>
                {
                    Some((best_id, best_replies))
                }
                _ => Some((message_id, replies)),
            };
        }
        best.map(|(message_id, _)| message_id)
    }

    fn run_bench_thread_open_tick(&mut self, cx: &mut Context<Self>) {
        if !self.ensure_bench_thread_conversation(cx) {
            return;
        }
        if !self.bench_script_step.is_multiple_of(24) {
            return;
        }
        let root_id = if self.bench_thread_root_ids.is_empty() {
            self.bench_thread_root_id
                .clone()
                .or_else(|| self.bench_thread_root_candidate())
        } else {
            let open_ix = (self.bench_script_step / 24) as usize;
            self.bench_thread_root_ids
                .get(open_ix % self.bench_thread_root_ids.len())
                .cloned()
        };
        let Some(root_id) = root_id else {
            return;
        };

        let started_at = Instant::now();
        self.ensure_message_binding_for_active_conversation(&root_id);
        self.models.open_thread(root_id.clone());
        self.dispatch_ui_action(UiAction::OpenThread {
            root_id: root_id.clone(),
        });
        self.reset_thread_scroll_state();
        self.pending_thread_scroll_to_bottom = true;
        self.sync_inputs_from_models(cx);
        self.refresh(cx);

        if env_flag(ENV_THREAD_OPEN_PROFILE) {
            let elapsed = started_at.elapsed();
            tracing::warn!(
                target: "zbase.thread.open.perf",
                phase = "bench_open_thread_tick",
                root_id = %root_id.0,
                elapsed_ms = elapsed.as_millis() as i64,
                elapsed_us = elapsed.as_micros() as i64,
                replies_after_open = self.models.thread_pane.replies.len() as i64,
                loading = self.models.thread_pane.loading,
                "thread_open_measurement"
            );
        }
    }

    fn ensure_bench_profile_initialized(&mut self) {
        if self.bench_profile_initialized {
            return;
        }
        let Some(user_id) = self.bench_profile_user_id.clone() else {
            return;
        };
        self.models.profile_panel.active_social_tab = SocialTab::Followers;
        self.models.profile_panel.user_id = Some(user_id.clone());
        self.models
            .set_right_pane(RightPaneMode::Profile(user_id.clone()));
        self.dispatch_ui_action(UiAction::ShowUserProfilePanel {
            user_id: user_id.clone(),
        });
        self.dispatch_ui_action(UiAction::LoadSocialGraphList {
            user_id: user_id.clone(),
            list_type: SocialGraphListType::Followers,
        });
        self.dispatch_ui_action(UiAction::RefreshProfilePresence {
            user_id,
            conversation_id: self.app_store.snapshot().timeline.conversation_id.clone(),
        });
        self.bench_profile_initialized = true;
    }

    fn inflate_bench_timeline_messages(&mut self, message_count: usize) {
        if message_count <= timeline_message_count(&self.models) {
            return;
        }

        let authors: Vec<crate::domain::user::UserSummary> = self
            .models
            .timeline
            .rows
            .iter()
            .filter_map(|row| match row {
                crate::models::timeline_model::TimelineRow::Message(message) => {
                    Some(message.author.clone())
                }
                _ => None,
            })
            .collect();
        if authors.is_empty() {
            return;
        }

        let conversation_id = self.models.timeline.conversation_id.clone();
        let mut rows = Vec::with_capacity(message_count + message_count / 180 + 4);
        let mut newest_message_id: Option<MessageId> = None;

        for idx in 0..message_count {
            if idx % 180 == 0 {
                rows.push(crate::models::timeline_model::TimelineRow::DateDivider(
                    format!("Bench Day {}", (idx / 180) + 1),
                ));
            }

            let author = authors[idx % authors.len()].clone();
            let text = if idx % 17 == 0 {
                format!(
                    "Synthetic benchmark message {idx}: this entry is intentionally longer to exercise multiline wrapping and heavy scroll rendering."
                )
            } else if idx % 5 == 0 {
                format!(
                    "Synthetic benchmark message {idx} with emoji aliases :rocket: :sparkles: and a short status update."
                )
            } else {
                format!("Synthetic benchmark message {idx}.")
            };

            let mut fragments = vec![MessageFragment::Text(text)];
            if idx % 29 == 0 {
                fragments.push(MessageFragment::Code {
                    text: "cargo check -q".to_string(),
                    lang: None,
                });
            }

            let message_id = MessageId::new(format!("bench-msg-{idx:06}"));
            newest_message_id = Some(message_id.clone());

            let message = crate::domain::message::MessageRecord {
                id: message_id,
                conversation_id: conversation_id.clone(),
                author_id: author.id.clone(),
                reply_to: None,
                thread_root_id: None,
                timestamp_ms: Some(now_unix_ms().saturating_sub((idx as i64) * 60_000)),
                event: None,
                link_previews: Vec::new(),
                permalink: format!("zbase://bench/{idx}"),
                fragments,
                source_text: None,
                attachments: Vec::new(),
                reactions: Vec::new(),
                thread_reply_count: if idx % 11 == 0 {
                    ((idx % 4) + 1) as u32
                } else {
                    0
                },
                send_state: MessageSendState::Sent,
                edited: None,
            };

            rows.push(crate::models::timeline_model::TimelineRow::Message(
                crate::models::timeline_model::MessageRow {
                    author,
                    message,
                    show_header: true,
                },
            ));
        }

        if rows.len() > 12 {
            rows.insert(
                rows.len() / 2,
                crate::models::timeline_model::TimelineRow::UnreadDivider(
                    "Unread messages".to_string(),
                ),
            );
        }

        self.models.timeline.rows = rows;
        self.models.timeline.highlighted_message_id = newest_message_id;
        self.models.timeline.unread_marker = None;
        self.timeline_row_render_cache.invalidate();
        self.sync_timeline_list_state_len(self.models.timeline.rows.len());
        self.scroll_timeline_list_to_bottom();
        self.reset_timeline_scroll_state();
    }

    fn bench_composer_paste_payload(step: u64) -> String {
        const BLOCK: &str = "Release checklist:\n- Verify migration status for all regions.\n- Confirm roll-forward and rollback scripts are in the runbook.\n- Coordinate with QA on smoke-test coverage before cutover.\n- Update incident channel with timestamps and owners.\n\nNotes:\nThe previous deployment hit a warmup bottleneck on cold cache reads.\nWe should stage traffic in three waves and validate query latency after each wave.\n";
        let repeat = 4 + ((step / 48) as usize % 4);
        let mut text = String::with_capacity((BLOCK.len() + 32) * repeat);
        for ix in 0..repeat {
            text.push_str(BLOCK);
            text.push_str(&format!("Chunk {} / {}\n\n", ix + 1, repeat));
        }
        text
    }

    fn sidebar_view_state(&self) -> SidebarViewState {
        SidebarViewState {
            sidebar: self.models.sidebar.clone(),
            connectivity: self.models.app.connectivity.clone(),
            current_user_display_name: self.models.app.current_user_display_name.clone(),
            current_user_avatar_asset: self.models.app.current_user_avatar_asset.clone(),
            dm_avatar_assets: self.sidebar_dm_avatar_assets.clone(),
            current_route: self.models.navigation.current.clone(),
            theme: self.resolved_theme,
        }
    }

    fn sync_sidebar_view_state(&mut self, cx: &mut Context<Self>) {
        let sidebar_state = self.sidebar_view_state();
        if self.sidebar_view.is_none() {
            let owner = cx.entity().downgrade();
            self.sidebar_view =
                Some(cx.new(|_| CachedSidebarView::new(owner, sidebar_state.clone())));
        }
        if let Some(sidebar_view) = self.sidebar_view.as_ref() {
            sidebar_view.update(cx, |view, cx| {
                view.update_state(sidebar_state, cx);
            });
        }
    }

    fn refresh(&mut self, cx: &mut Context<Self>) {
        self.sync_sidebar_view_state(cx);
        self.sync_timeline_list_state_len(self.models.timeline.rows.len());
        self.perf_harness.record_refresh();
        cx.notify();
    }

    fn reset_timeline_scroll_state(&mut self) {
        self.timeline_unseen_count = 0;
        self.last_timeline_message_count = timeline_message_count(&self.models);
        self.last_timeline_latest_message_id =
            latest_timeline_message_id(&self.models.timeline.rows);
        self.pending_older_scroll_anchor = None;
        self.suppress_next_timeline_bottom_snap = false;
        self.suppress_next_timeline_unseen_increment = false;
    }

    fn reset_thread_scroll_state(&mut self) {
        self.thread_unseen_count = 0;
        self.last_thread_reply_count = self.models.thread_pane.replies.len();
    }

    fn timeline_is_near_bottom(&self) -> bool {
        is_list_near_bottom(&self.timeline_list_state)
    }

    fn thread_is_near_bottom(&self) -> bool {
        is_near_bottom(&self.thread_scroll)
    }

    fn text_input_activity_recent(&self) -> bool {
        self.last_text_input_activity
            .is_some_and(|last| last.elapsed() <= TEXT_INPUT_SYNC_DEFER_WINDOW)
    }

    fn apply_typing_indicator_update(
        &mut self,
        conversation_id: &ConversationId,
        users: &[UserId],
    ) -> bool {
        if &self.models.timeline.conversation_id != conversation_id {
            return false;
        }

        let next_label = typing_indicator_label(users);
        if self.models.timeline.typing_text == next_label {
            return false;
        }

        self.models.timeline.typing_text = next_label;
        true
    }

    fn timeline_is_near_top(&self) -> bool {
        is_list_near_top(&self.timeline_list_state)
    }

    fn scroll_timeline_list_to_bottom(&self) {
        scroll_list_to_bottom(&self.timeline_list_state);
    }

    fn sync_timeline_list_state_len(&self, target_len: usize) {
        if self.timeline_list_state.item_count() == target_len {
            return;
        }
        let was_near_bottom = self.timeline_is_near_bottom() && !self.jump_to_message_active;
        let prev = self.timeline_list_state.logical_scroll_top();
        self.timeline_list_state.reset(target_len);
        if target_len == 0 {
            return;
        }
        if was_near_bottom {
            self.scroll_timeline_list_to_bottom();
            return;
        }
        self.timeline_list_state.scroll_to(ListOffset {
            item_ix: prev.item_ix.min(target_len.saturating_sub(1)),
            offset_in_item: prev.offset_in_item,
        });
    }

    fn sync_scroll_indicators(&mut self) -> bool {
        let prev_timeline_unseen = self.timeline_unseen_count;
        let prev_thread_unseen = self.thread_unseen_count;

        let timeline_count = timeline_message_count(&self.models);
        let timeline_latest_message_id = latest_timeline_message_id(&self.models.timeline.rows);
        if timeline_count < self.last_timeline_message_count {
            self.reset_timeline_scroll_state();
        } else if timeline_count > self.last_timeline_message_count {
            let delta = timeline_count - self.last_timeline_message_count;
            let has_new_latest_message = match (
                timeline_latest_message_id.as_ref(),
                self.last_timeline_latest_message_id.as_ref(),
            ) {
                (Some(latest), Some(previous)) => message_id_is_after(latest, previous),
                (Some(_), None) => true,
                _ => false,
            };
            if self.suppress_next_timeline_bottom_snap {
                self.suppress_next_timeline_bottom_snap = false;
            } else if !has_new_latest_message {
                if self.timeline_is_near_bottom() {
                    self.timeline_unseen_count = 0;
                }
            } else if self.timeline_is_near_bottom() {
                self.scroll_timeline_list_to_bottom();
                self.timeline_unseen_count = 0;
                self.suppress_next_timeline_unseen_increment = false;
            } else if self.suppress_next_timeline_unseen_increment {
                self.timeline_unseen_count = 0;
                self.suppress_next_timeline_unseen_increment = false;
            } else {
                self.timeline_unseen_count = self.timeline_unseen_count.saturating_add(delta);
            }
            self.last_timeline_message_count = timeline_count;
            self.last_timeline_latest_message_id = timeline_latest_message_id;
        } else if self.timeline_is_near_bottom() {
            self.timeline_unseen_count = 0;
            self.suppress_next_timeline_unseen_increment = false;
            self.last_timeline_latest_message_id = timeline_latest_message_id;
        } else {
            self.last_timeline_latest_message_id = timeline_latest_message_id;
        }

        if self.models.navigation.right_pane != RightPaneMode::Thread
            || self.models.thread_pane.root_message_id.is_none()
        {
            self.reset_thread_scroll_state();
            return self.timeline_unseen_count != prev_timeline_unseen
                || self.thread_unseen_count != prev_thread_unseen;
        }

        let thread_count = self.models.thread_pane.replies.len();
        if thread_count < self.last_thread_reply_count {
            self.reset_thread_scroll_state();
        } else if thread_count > self.last_thread_reply_count {
            self.thread_scroll.scroll_to_bottom();
            self.thread_unseen_count = 0;
            self.last_thread_reply_count = thread_count;
        } else if self.thread_is_near_bottom() {
            self.thread_unseen_count = 0;
        }

        self.timeline_unseen_count != prev_timeline_unseen
            || self.thread_unseen_count != prev_thread_unseen
    }

    fn sync_timeline_row_link_previews(
        &mut self,
        source_messages: &[crate::domain::message::MessageRecord],
    ) {
        let previews_by_message_id = source_messages
            .iter()
            .map(|message| (&message.id, &message.link_previews))
            .collect::<HashMap<_, _>>();
        for row in &mut self.models.timeline.rows {
            let crate::models::timeline_model::TimelineRow::Message(message_row) = row else {
                continue;
            };
            let message = &mut message_row.message;
            if let Some(next_previews) = previews_by_message_id.get(&message.id) {
                if message.link_previews != **next_previews {
                    message.link_previews = (*next_previews).clone();
                }
            } else if !message.link_previews.is_empty() {
                message.link_previews.clear();
            }
        }
        self.apply_og_previews_to_timeline();
    }

    fn dispatch_ui_action(&mut self, action: UiAction) {
        let t0 = Instant::now();
        let is_update_draft = matches!(&action, UiAction::UpdateDraft { .. });
        let open_thread_root = match &action {
            UiAction::OpenThread { root_id } => Some(root_id.clone()),
            _ => None,
        };
        let profile_open_thread = open_thread_root.is_some() && env_flag(ENV_THREAD_OPEN_PROFILE);
        let mut backend_apply_elapsed = Duration::default();
        let mut backend_reduce_elapsed = Duration::default();
        let mut backend_apply_calls = 0usize;
        let mut backend_event_count = 0usize;
        if is_update_draft {
            let _ = self.app_store.dispatch_ui(action);
            self.perf_harness
                .record_duration(PerfTimer::DispatchUiAction, t0.elapsed());
            return;
        }
        let is_quick_switcher_search = matches!(&action, UiAction::QuickSwitcherSearch { .. });
        let is_find_in_chat_search = matches!(&action, UiAction::FindInChatSearch { .. });
        let is_open_thread = open_thread_root.is_some();
        let should_sync_models = !matches!(
            &action,
            UiAction::SetSearchQuery(_)
                | UiAction::SetSidebarFilter(_)
                | UiAction::UpdateDraft { .. }
                | UiAction::OpenThread { .. }
                | UiAction::QuickSwitcherSearch { .. }
                | UiAction::FindInChatSearch { .. }
        );
        let mut pending_effects = self.app_store.dispatch_ui(action);

        loop {
            let apply_started_at = profile_open_thread.then(Instant::now);
            let Ok(events) = self.backend_router.apply_effects(&pending_effects) else {
                break;
            };
            if let Some(apply_started_at) = apply_started_at {
                backend_apply_elapsed += apply_started_at.elapsed();
                backend_apply_calls = backend_apply_calls.saturating_add(1);
                backend_event_count = backend_event_count.saturating_add(events.len());
            }

            if events.is_empty() {
                break;
            }

            pending_effects.clear();
            for event in events {
                if is_quick_switcher_search || is_find_in_chat_search {
                    let maybe_handled = if let crate::state::event::BackendEvent::SearchResults {
                        query_id,
                        results,
                        is_complete,
                    } = &event
                    {
                        if is_quick_switcher_search {
                            if query_id.0.starts_with("quick-switcher-")
                                && query_id.0
                                    == format!("quick-switcher-{}", self.quick_switcher_query_seq)
                            {
                                let query_seq = quick_switcher_seq_from_query_id(&query_id.0);
                                self.models.apply_quick_switcher_message_results(results);
                                self.models.quick_switcher.loading_messages = !*is_complete;
                                if let Some(query_seq) = query_seq {
                                    if let Some(dispatched_at) = self
                                        .quick_switcher_remote_dispatch_started_at
                                        .remove(&query_seq)
                                    {
                                        tracing::debug!(
                                            target: "zbase.quick_switcher.perf",
                                            seq = query_seq,
                                            remote_dispatch_to_apply_ms =
                                                dispatched_at.elapsed().as_millis(),
                                            is_complete = *is_complete,
                                            result_count = results.len(),
                                            "quick_switcher_remote_results_applied_fast_path"
                                        );
                                    } else {
                                        tracing::debug!(
                                            target: "zbase.quick_switcher.perf",
                                            seq = query_seq,
                                            is_complete = *is_complete,
                                            result_count = results.len(),
                                            "quick_switcher_remote_results_applied_fast_path_without_dispatch_marker"
                                        );
                                    }
                                }
                            }
                            true
                        } else if is_find_in_chat_search {
                            if query_id.0.starts_with("find-in-chat-")
                                && query_id.0
                                    == format!(
                                        "find-in-chat-{}",
                                        self.models.find_in_chat.query_seq
                                    )
                            {
                                self.models
                                    .find_in_chat
                                    .apply_results(results.clone(), *is_complete);
                            }
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if maybe_handled {
                        continue;
                    }
                }
                let reduce_started_at = profile_open_thread.then(Instant::now);
                pending_effects.extend(self.app_store.dispatch_backend(event));
                if let Some(reduce_started_at) = reduce_started_at {
                    backend_reduce_elapsed += reduce_started_at.elapsed();
                }
            }

            if pending_effects.is_empty() {
                break;
            }
        }

        let sync_started_at = profile_open_thread.then(Instant::now);
        if is_open_thread {
            self.sync_thread_pane_from_store();
        } else if should_sync_models {
            self.sync_models_from_store();
        }
        let sync_elapsed = sync_started_at.map(|started_at| started_at.elapsed());
        let elapsed = t0.elapsed();
        self.perf_harness
            .record_duration(PerfTimer::DispatchUiAction, elapsed);
        if let Some(root_id) = open_thread_root.as_ref()
            && env_flag(ENV_THREAD_OPEN_PROFILE)
        {
            tracing::warn!(
                target: "zbase.thread.open.perf",
                phase = "dispatch_ui_open_thread",
                root_id = %root_id.0,
                elapsed_ms = elapsed.as_millis() as i64,
                elapsed_us = elapsed.as_micros() as i64,
                loading = self.models.thread_pane.loading,
                replies = self.models.thread_pane.replies.len() as i64,
                "thread_open_measurement"
            );
            if let Some(sync_elapsed) = sync_elapsed {
                tracing::warn!(
                    target: "zbase.thread.open.perf",
                    phase = "dispatch_ui_open_thread_breakdown",
                    root_id = %root_id.0,
                    total_us = elapsed.as_micros() as i64,
                    backend_apply_us = backend_apply_elapsed.as_micros() as i64,
                    backend_reduce_us = backend_reduce_elapsed.as_micros() as i64,
                    sync_models_us = sync_elapsed.as_micros() as i64,
                    backend_apply_calls = backend_apply_calls as i64,
                    backend_event_count = backend_event_count as i64,
                    should_sync_models = should_sync_models,
                    "thread_open_measurement"
                );
            }
        }
        if self.perf_harness.is_capturing() && elapsed.as_millis() > 1 {
            let action_label = if is_quick_switcher_search {
                "QuickSwitcherSearch".to_string()
            } else if is_find_in_chat_search {
                "FindInChatSearch".to_string()
            } else {
                "UiAction".to_string()
            };
            tracing::warn!("dispatch_ui_action({action_label}) took {elapsed:?}");
        }
    }

    fn sync_models_from_store(&mut self) {
        let t0 = Instant::now();
        let snapshot = self.app_store.snapshot();

        self.models.sidebar.filter = snapshot.sidebar.filter.clone();
        self.models.search.query = snapshot.search.query.clone();
        self.models.search.filters = snapshot.search.filters.clone();
        self.models.search.results = snapshot.search.results.clone();
        self.models.search.highlighted_index = snapshot.search.highlighted_index;
        self.models.search.is_loading = snapshot.search.is_loading;
        self.models.new_chat.open = snapshot.new_chat.open;
        self.models.new_chat.search_query = snapshot.new_chat.search_query.clone();
        self.models.new_chat.search_results = snapshot.new_chat.search_results.clone();
        self.models.new_chat.selected_participants =
            snapshot.new_chat.selected_participants.clone();
        self.models.new_chat.creating = snapshot.new_chat.creating;
        self.models.new_chat.error = snapshot.new_chat.error.clone();
        self.models.overlay.new_chat_open = snapshot.new_chat.open;

        let workspace_sig = workspace_state_signature(&snapshot.workspace);
        let workspace_changed = self.sync_cache.workspace_sig != Some(workspace_sig);
        if workspace_changed {
            self.models.workspace.workspace_name = snapshot.workspace.workspace_name.clone();
            self.models.workspace.channels = snapshot.workspace.channels.clone();
            self.models.workspace.direct_messages = snapshot.workspace.direct_messages.clone();
            self.sync_cache.workspace_sig = Some(workspace_sig);
        }

        let mut current_user_id: Option<UserId> = None;
        let previous_active_workspace_id = self.models.app.active_workspace_id.clone();
        if let Some(workspace_id) = snapshot.workspace.active_workspace_id.clone() {
            self.models.app.active_workspace_id = workspace_id.clone();
            self.models.workspace.workspace_id = workspace_id;
        }
        let active_workspace_changed =
            self.models.app.active_workspace_id != previous_active_workspace_id;
        if let Some(account) = snapshot
            .backend
            .accounts
            .values()
            .find(|account| matches!(account.connection_state, ConnectionState::Connected))
            .or_else(|| snapshot.backend.accounts.values().next())
        {
            let account_user = UserId::new(account.display_name.clone());
            current_user_id = Some(account_user.clone());
            let profile = snapshot
                .backend
                .user_profiles
                .get(&account_user)
                .or_else(|| {
                    let lower = account_user.0.to_ascii_lowercase();
                    if lower == account_user.0 {
                        None
                    } else {
                        snapshot.backend.user_profiles.get(&UserId::new(lower))
                    }
                });
            self.models.app.current_user_id = Some(account_user);
            self.models.app.current_user_display_name = profile
                .map(|value| value.display_name.clone())
                .unwrap_or_else(|| account.display_name.clone());
            self.models.app.current_user_avatar_asset = profile
                .and_then(|value| value.avatar_asset.clone())
                .or_else(|| account.avatar.clone());
        } else {
            self.models.app.current_user_id = None;
            self.models.app.current_user_display_name = "You".to_string();
            self.models.app.current_user_avatar_asset = None;
        }
        let quick_switcher_profiles_changed = self.models.update_quick_switcher_profile_names(
            snapshot
                .backend
                .user_profiles
                .iter()
                .filter_map(|(user_id, profile)| {
                    let display_name = profile.display_name.trim();
                    if display_name.is_empty() {
                        None
                    } else {
                        Some((user_id.0.to_ascii_lowercase(), display_name.to_string()))
                    }
                })
                .collect(),
        );
        if workspace_changed || active_workspace_changed {
            self.models.rebuild_quick_switcher_local_search_corpus();
            self.quick_switcher_last_local_query.clear();
            self.quick_switcher_last_local_matched_entry_indices = Arc::new(Vec::new());
            self.quick_switcher_last_local_corpus_revision = 0;
        } else if quick_switcher_profiles_changed {
            self.quick_switcher_last_local_query.clear();
            self.quick_switcher_last_local_matched_entry_indices = Arc::new(Vec::new());
            self.quick_switcher_last_local_corpus_revision = 0;
        }
        if quick_switcher_profiles_changed
            && self.models.overlay.quick_switcher_open
            && self.models.quick_switcher.query.trim().is_empty()
        {
            self.models.update_quick_switcher_query(String::new());
        }
        let current_user_avatar = self.models.app.current_user_avatar_asset.clone();

        let next_route = snapshot
            .navigation
            .current_route
            .clone()
            .unwrap_or_else(|| self.models.navigation.current.clone());
        let route_changed = self.models.navigation.current != next_route;
        if route_changed {
            self.models.navigation.current = next_route.clone();
        }

        if let Some(user_id) = self.models.profile_panel.user_id.clone() {
            self.models.profile_panel.profile = snapshot
                .backend
                .profile_panel
                .profiles
                .get(&user_id)
                .cloned();
            self.models.profile_panel.loading =
                snapshot.backend.profile_panel.loading.contains(&user_id);
            self.models.profile_panel.loading_social_list = snapshot
                .backend
                .profile_panel
                .loading_social_list
                .contains(&user_id);
        } else {
            self.models.profile_panel.profile = None;
            self.models.profile_panel.loading = false;
            self.models.profile_panel.loading_social_list = false;
        }

        let sidebar_sections_sig = sidebar_sections_state_signature(&snapshot.sidebar.sections);
        let sidebar_sections_changed =
            self.sync_cache.sidebar_sections_sig != Some(sidebar_sections_sig);
        if sidebar_sections_changed {
            self.models.sidebar.sections = snapshot
                .sidebar
                .sections
                .iter()
                .map(|section| crate::models::sidebar_model::SidebarSection {
                    id: section
                        .id
                        .clone()
                        .unwrap_or_else(|| SidebarSectionId::new("section")),
                    title: section.title.clone(),
                    rows: section
                        .rows
                        .iter()
                        .filter_map(|row| {
                            row.route.clone().map(|route| {
                                crate::models::sidebar_model::SidebarRow {
                                    label: row.label.clone(),
                                    unread_count: row.unread_count,
                                    mention_count: row.mention_count,
                                    route,
                                }
                            })
                        })
                        .collect(),
                    collapsed: section.collapsed,
                })
                .collect();
            self.sync_cache.sidebar_sections_sig = Some(sidebar_sections_sig);
        }
        self.models.sidebar.highlighted_route = snapshot.sidebar.highlighted_route.clone();
        if sidebar_sections_changed || workspace_changed {
            self.models.apply_saved_sidebar_order();
        }
        if route_changed {
            self.models.expand_section_for_route(&next_route);
        }

        let dm_avatar_sig = dm_avatar_inputs_signature(
            &self.models.sidebar.sections,
            &snapshot.workspace.direct_messages,
            &snapshot.backend.user_profiles,
        );
        if self.sync_cache.dm_avatar_sig != Some(dm_avatar_sig) {
            let dm_conversation_avatar_assets = snapshot
                .workspace
                .direct_messages
                .iter()
                .filter_map(|summary| {
                    if !matches!(
                        summary.kind,
                        crate::domain::conversation::ConversationKind::DirectMessage
                    ) {
                        return None;
                    }

                    let username = first_dm_username(&summary.title)?;
                    let avatar_asset = snapshot
                        .backend
                        .user_profiles
                        .get(&UserId::new(username.to_string()))
                        .and_then(|profile| profile.avatar_asset.clone())?;
                    Some((summary.id.0.clone(), avatar_asset))
                })
                .collect::<HashMap<_, _>>();

            let mut dm_avatar_assets = HashMap::new();
            for row in self
                .models
                .sidebar
                .sections
                .iter()
                .flat_map(|section| section.rows.iter())
            {
                if let Route::DirectMessage { dm_id, .. } = &row.route
                    && let Some(avatar_asset) = dm_conversation_avatar_assets.get(&dm_id.0)
                {
                    dm_avatar_assets.insert(row.route.label(), avatar_asset.clone());
                }
            }
            self.sidebar_dm_avatar_assets = dm_avatar_assets;
            self.sync_cache.dm_avatar_sig = Some(dm_avatar_sig);
        }

        let mut conversation_changed = false;
        if let Some(conversation_id) = snapshot.timeline.conversation_id.clone() {
            conversation_changed = self.models.timeline.conversation_id != conversation_id;
            self.models.timeline.conversation_id = conversation_id;
        }
        if conversation_changed {
            self.pending_older_scroll_anchor = None;
            self.pending_older_scroll_seq = None;
            self.suppress_next_timeline_bottom_snap = false;
        }
        let was_near_bottom_before_sync =
            self.timeline_is_near_bottom() && !self.jump_to_message_active;
        let previous_has_pinned_banner = self.models.conversation.pinned_message.is_some();
        let timeline_anchor_message_id = if !conversation_changed
            && self.pending_older_scroll_anchor.is_none()
            && self.models.timeline.pending_scroll_target.is_none()
            && (!self.timeline_is_near_bottom() || self.jump_to_message_active)
        {
            self.first_visible_timeline_message_id()
        } else {
            None
        };

        let timeline_emoji_sig = timeline_emoji_signature(
            Some(&self.models.timeline.conversation_id),
            &snapshot.backend.conversation_emojis,
            &snapshot.backend.emoji_sources,
        );
        let timeline_emoji_changed = self.sync_cache.timeline_emoji_sig != Some(timeline_emoji_sig);
        if timeline_emoji_changed {
            self.models.timeline.emoji_index = snapshot
                .timeline
                .conversation_id
                .as_ref()
                .and_then(|conversation_id| {
                    snapshot.backend.conversation_emojis.get(conversation_id)
                })
                .map(|emoji_index| {
                    emoji_index
                        .iter()
                        .map(|(alias, value)| {
                            (
                                alias.clone(),
                                crate::models::timeline_model::InlineEmojiRender {
                                    alias: value.alias.clone(),
                                    unicode: value.unicode.clone(),
                                    asset_path: value.asset_path.clone(),
                                },
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            self.models.timeline.emoji_source_index = snapshot
                .backend
                .emoji_sources
                .iter()
                .map(|(source_key, value)| {
                    (
                        source_key.clone(),
                        crate::models::timeline_model::InlineEmojiRender {
                            alias: value.alias.clone(),
                            unicode: value.unicode.clone(),
                            asset_path: value.asset_path.clone(),
                        },
                    )
                })
                .collect();
            self.sync_cache.timeline_emoji_sig = Some(timeline_emoji_sig);
            self.models.refresh_quick_react_recent();
        }
        let timeline_reactions_sig = timeline_reactions_signature(
            Some(&self.models.timeline.conversation_id),
            &snapshot.backend.message_reactions,
            &snapshot.backend.user_profiles,
        );
        let timeline_reactions_changed =
            self.sync_cache.timeline_reactions_sig != Some(timeline_reactions_sig);
        if timeline_reactions_changed {
            let current_user_id = snapshot
                .backend
                .accounts
                .values()
                .find(|account| matches!(account.connection_state, ConnectionState::Connected))
                .map(|account| account.display_name.to_ascii_lowercase());
            self.models.timeline.reaction_index = snapshot
                .timeline
                .conversation_id
                .as_ref()
                .and_then(|conversation_id| snapshot.backend.message_reactions.get(conversation_id))
                .map(|reactions_by_message| {
                    reactions_by_message
                        .iter()
                        .map(|(message_id, reactions)| {
                            (
                                message_id.clone(),
                                reactions
                                    .iter()
                                    .map(|reaction| {
                                        let mut actors = reaction
                                            .actor_ids
                                            .iter()
                                            .map(|actor_id| {
                                                let display_name = snapshot
                                                    .backend
                                                    .user_profiles
                                                    .get(actor_id)
                                                    .map(|profile| profile.display_name.trim())
                                                    .filter(|name| !name.is_empty())
                                                    .map(str::to_string)
                                                    .unwrap_or_else(|| actor_id.0.clone());
                                                crate::models::timeline_model::ReactionActorRender {
                                                    user_id: actor_id.0.clone(),
                                                    display_name,
                                                }
                                            })
                                            .collect::<Vec<_>>();
                                        actors.sort_by(|left, right| {
                                            left.display_name
                                                .cmp(&right.display_name)
                                                .then_with(|| left.user_id.cmp(&right.user_id))
                                        });
                                        let reacted_by_me =
                                            current_user_id.as_ref().is_some_and(|current_user| {
                                                reaction.actor_ids.iter().any(|actor_id| {
                                                    actor_id.0.eq_ignore_ascii_case(current_user)
                                                })
                                            });
                                        crate::models::timeline_model::MessageReactionRender {
                                            emoji: reaction.emoji.clone(),
                                            source_ref: reaction.source_ref.clone(),
                                            count: actors.len(),
                                            actors,
                                            reacted_by_me,
                                        }
                                    })
                                    .collect(),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            self.sync_cache.timeline_reactions_sig = Some(timeline_reactions_sig);
        }
        let timeline_author_roles_sig = timeline_author_roles_signature(
            snapshot.timeline.conversation_id.as_ref(),
            &snapshot.backend.conversation_team_ids,
            &snapshot.backend.team_roles,
        );
        let timeline_author_roles_changed =
            self.sync_cache.timeline_author_roles_sig != Some(timeline_author_roles_sig);
        if timeline_author_roles_changed {
            let timeline_is_dm =
                snapshot
                    .timeline
                    .conversation_id
                    .as_ref()
                    .is_some_and(|conversation_id| {
                        snapshot
                            .workspace
                            .direct_messages
                            .iter()
                            .any(|summary| summary.id == *conversation_id)
                    });
            self.models.timeline.author_role_index = if timeline_is_dm {
                HashMap::new()
            } else {
                snapshot
                    .timeline
                    .conversation_id
                    .as_ref()
                    .and_then(|conversation_id| {
                        snapshot.backend.conversation_team_ids.get(conversation_id)
                    })
                    .and_then(|team_id| snapshot.backend.team_roles.get(team_id))
                    .map(|roles_by_user| {
                        roles_by_user
                            .iter()
                            .filter_map(|(user_id, role)| {
                                let mapped = match role {
                                    crate::state::event::TeamRoleKind::Admin => {
                                        TeamAuthorRole::Admin
                                    }
                                    crate::state::event::TeamRoleKind::Owner => {
                                        TeamAuthorRole::Owner
                                    }
                                    crate::state::event::TeamRoleKind::Member => return None,
                                };
                                Some((
                                    crate::domain::ids::UserId::new(user_id.0.to_ascii_lowercase()),
                                    mapped,
                                ))
                            })
                            .collect::<HashMap<_, _>>()
                    })
                    .unwrap_or_default()
            };
            self.sync_cache.timeline_author_roles_sig = Some(timeline_author_roles_sig);
        }
        self.models.timeline.highlighted_message_id =
            snapshot.timeline.highlighted_message_id.clone();
        self.models.timeline.unread_marker = snapshot.timeline.unread_marker.clone();
        self.models.timeline.current_user_id = current_user_id.clone();
        self.models.timeline.affinity_index = snapshot.backend.user_affinities.clone();
        self.models.timeline.older_cursor = snapshot.timeline.older_cursor.clone();
        self.models.timeline.newer_cursor = snapshot.timeline.newer_cursor.clone();
        self.models.timeline.loading_older = snapshot.timeline.loading_older;
        self.models.thread_pane.open = snapshot.thread.open;
        self.models.thread_pane.root_message_id = snapshot.thread.root_message_id.clone();
        self.models.thread_pane.replies = snapshot.thread.replies.clone();
        self.models.thread_pane.reply_draft = snapshot.thread.reply_draft.clone();
        self.models.thread_pane.loading = snapshot.thread.loading;
        if self.pending_thread_scroll_to_bottom
            && self.models.thread_pane.open
            && !self.models.thread_pane.loading
        {
            self.thread_scroll.scroll_to_bottom();
            self.pending_thread_scroll_to_bottom = false;
        }

        if workspace_changed || conversation_changed {
            self.models.conversation.can_post = false;
            if let Some(active_conversation) = self
                .models
                .workspace
                .channels
                .iter()
                .chain(self.models.workspace.direct_messages.iter())
                .find(|summary| Some(summary.id.clone()) == snapshot.timeline.conversation_id)
                .cloned()
            {
                self.models.conversation.summary = active_conversation.clone();
                self.models.composer.conversation_id = active_conversation.id;
                self.models.conversation.can_post = true;
                self.models.conversation.is_archived = false;
                self.models.conversation.avatar_asset = match active_conversation.kind {
                    crate::domain::conversation::ConversationKind::Channel => active_conversation
                        .group
                        .as_ref()
                        .and_then(|group| {
                            let team_name = group.id.trim();
                            if team_name.is_empty() {
                                None
                            } else {
                                snapshot
                                    .backend
                                    .user_profiles
                                    .get(&UserId::new(format!("team:{team_name}")))
                            }
                        })
                        .and_then(|profile| profile.avatar_asset.clone()),
                    crate::domain::conversation::ConversationKind::DirectMessage => {
                        let username = first_dm_username(&active_conversation.title)
                            .unwrap_or("")
                            .trim();
                        if username.is_empty() {
                            None
                        } else {
                            let lower_id = UserId::new(username.to_ascii_lowercase());
                            let profile =
                                snapshot.backend.user_profiles.get(&lower_id).or_else(|| {
                                    snapshot
                                        .backend
                                        .user_profiles
                                        .get(&UserId::new(username.to_string()))
                                });

                            if let Some(profile) = profile {
                                let display_name = profile.display_name.trim();
                                if !display_name.is_empty()
                                    && !display_name.eq_ignore_ascii_case(username)
                                {
                                    self.models.conversation.summary.title =
                                        display_name.to_string();
                                } else {
                                    self.models.conversation.summary.title = username.to_string();
                                }
                                profile.avatar_asset.clone()
                            } else {
                                self.models.conversation.summary.title = username.to_string();
                                None
                            }
                        }
                    }
                    crate::domain::conversation::ConversationKind::GroupDirectMessage => {
                        let mut participants = Vec::new();
                        for username in active_conversation
                            .title
                            .split(',')
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                        {
                            let lower_id = UserId::new(username.to_ascii_lowercase());
                            let profile =
                                snapshot.backend.user_profiles.get(&lower_id).or_else(|| {
                                    snapshot
                                        .backend
                                        .user_profiles
                                        .get(&UserId::new(username.to_string()))
                                });
                            let display_name = profile
                                .map(|profile| profile.display_name.trim())
                                .filter(|name| !name.is_empty())
                                .filter(|name| !name.eq_ignore_ascii_case(username))
                                .unwrap_or(username);
                            participants.push(display_name.to_string());
                        }
                        if !participants.is_empty() {
                            self.models.conversation.summary.title = participants.join(", ");
                        }
                        None
                    }
                };
            } else if let Some(conversation_id) = snapshot.timeline.conversation_id.clone() {
                if let Some((cached_summary, cached_can_post)) =
                    self.preview_summaries.get(&conversation_id).cloned()
                {
                    self.models.conversation.summary = cached_summary;
                    self.models.conversation.can_post = cached_can_post;
                    self.models.conversation.avatar_asset = None;
                } else {
                    self.models.conversation.avatar_asset = None;
                    if self.models.conversation.summary.id != conversation_id {
                        self.models.conversation.summary.id = conversation_id.clone();
                    }
                }
                self.models.composer.conversation_id = conversation_id;
            }
        }
        self.models.conversation.pinned_message = snapshot
            .timeline
            .conversation_id
            .as_ref()
            .and_then(|conversation_id| {
                snapshot
                    .backend
                    .conversation_pins
                    .get(conversation_id)
                    .and_then(|pinned| pinned.items.first().cloned())
                    .filter(|pinned_item| {
                        self.models
                            .settings
                            .dismissed_pinned_items
                            .get(&conversation_id.0)
                            .is_none_or(|dismissed_id| dismissed_id != &pinned_item.id)
                    })
            });
        self.models.conversation.details = snapshot
            .timeline
            .conversation_id
            .as_ref()
            .filter(|conversation_id| **conversation_id == self.models.conversation.summary.id)
            .map(|_| {
                build_channel_details(
                    &self.models.conversation.summary,
                    self.models.conversation.member_count,
                    self.models.conversation.can_post,
                    self.models.conversation.is_archived,
                    &snapshot.backend,
                    current_user_id.as_ref(),
                )
            });
        let has_pinned_banner = self.models.conversation.pinned_message.is_some();
        let pinned_banner_visibility_changed = previous_has_pinned_banner != has_pinned_banner;

        let timeline_messages = snapshot.timeline.messages.clone();
        let loading_older = snapshot.timeline.loading_older;
        let loading_older_just_finished = self.last_timeline_loading_older && !loading_older;
        self.last_timeline_loading_older = loading_older;
        let timeline_link_previews_sig = timeline_link_previews_signature(
            snapshot.timeline.conversation_id.as_ref(),
            &timeline_messages,
        );
        let timeline_link_previews_changed =
            self.sync_cache.timeline_link_previews_sig != Some(timeline_link_previews_sig);
        let timeline_rows_sig = timeline_rows_input_signature(
            snapshot.timeline.conversation_id.as_ref(),
            &timeline_messages,
            snapshot.timeline.unread_marker.as_ref(),
            loading_older,
            &snapshot.backend.user_profiles,
            &snapshot.backend.user_affinities,
            &snapshot.backend.user_presences,
            current_user_id.as_ref(),
            current_user_avatar.as_deref(),
        );
        let timeline_rows_changed = self.sync_cache.timeline_rows_sig != Some(timeline_rows_sig);
        if timeline_rows_changed {
            let mut rows: Vec<crate::models::timeline_model::TimelineRow> = Vec::new();
            let unread_marker = snapshot.timeline.unread_marker.clone();
            let mut unread_divider_inserted = unread_marker.is_none();
            let mut previous_message: Option<(UserId, Option<i64>, bool)> = None;

            for message in &timeline_messages {
                if !unread_divider_inserted
                    && unread_marker
                        .as_ref()
                        .is_some_and(|marker| message_id_is_after(&message.id, marker))
                {
                    rows.push(crate::models::timeline_model::TimelineRow::UnreadDivider(
                        "Unread messages".to_string(),
                    ));
                    unread_divider_inserted = true;
                    previous_message = None;
                }

                if is_non_text_placeholder_message(message) {
                    continue;
                }

                if let Some(event) = &message.event {
                    if matches!(event, ChatEvent::MessageDeleted { .. }) {
                        continue;
                    }
                    rows.push(crate::models::timeline_model::TimelineRow::SystemEvent(
                        format_chat_event(
                            event,
                            &message.author_id,
                            &snapshot.backend.user_profiles,
                            team_name_for_conversation(
                                &message.conversation_id,
                                &snapshot.backend.conversation_team_ids,
                                &self.models.workspace.channels,
                            ),
                        ),
                    ));
                    previous_message = None;
                    continue;
                }

                let author_profile = snapshot
                    .backend
                    .user_profiles
                    .get(&message.author_id)
                    .or_else(|| {
                        let lower = message.author_id.0.to_ascii_lowercase();
                        if lower == message.author_id.0 {
                            None
                        } else {
                            snapshot.backend.user_profiles.get(&UserId::new(lower))
                        }
                    });
                let display_name = author_profile
                    .map(|profile| profile.display_name.clone())
                    .unwrap_or_else(|| message.author_id.0.clone());
                let avatar_asset = author_profile
                    .and_then(|profile| profile.avatar_asset.clone())
                    .or_else(|| {
                        if current_user_id.as_ref() == Some(&message.author_id) {
                            current_user_avatar.clone()
                        } else {
                            None
                        }
                    });
                let author_presence = snapshot
                    .backend
                    .user_presences
                    .get(&message.author_id)
                    .cloned()
                    .or_else(|| {
                        let lower = message.author_id.0.to_ascii_lowercase();
                        if lower == message.author_id.0 {
                            None
                        } else {
                            snapshot
                                .backend
                                .user_presences
                                .get(&UserId::new(lower))
                                .cloned()
                        }
                    })
                    .unwrap_or(crate::domain::presence::Presence {
                        availability: crate::domain::presence::Availability::Offline,
                        status_text: None,
                    });
                let author_affinity = snapshot
                    .backend
                    .user_affinities
                    .get(&message.author_id)
                    .copied()
                    .or_else(|| {
                        let lower = message.author_id.0.to_ascii_lowercase();
                        if lower == message.author_id.0 {
                            None
                        } else {
                            snapshot
                                .backend
                                .user_affinities
                                .get(&UserId::new(lower))
                                .copied()
                        }
                    })
                    .unwrap_or_default();

                let show_header = previous_message.as_ref().is_none_or(
                    |(previous_author, previous_timestamp_ms, previous_was_thread_reply_stub)| {
                        if previous_author != &message.author_id {
                            return true;
                        }
                        // Don't group a main-chat message immediately after a thread-reply stub
                        // ("Replied in thread") from the same author.
                        if *previous_was_thread_reply_stub && message.reply_to.is_none() {
                            return true;
                        }
                        let (Some(previous_timestamp_ms), Some(current_timestamp_ms)) =
                            (*previous_timestamp_ms, message.timestamp_ms)
                        else {
                            return true;
                        };
                        if current_timestamp_ms <= previous_timestamp_ms {
                            return true;
                        }
                        const GROUP_WINDOW_MS: i64 = 15 * 60 * 1000;
                        current_timestamp_ms.saturating_sub(previous_timestamp_ms) > GROUP_WINDOW_MS
                    },
                );

                rows.push(crate::models::timeline_model::TimelineRow::Message(
                    crate::models::timeline_model::MessageRow {
                        author: crate::domain::user::UserSummary {
                            id: message.author_id.clone(),
                            display_name,
                            title: String::new(),
                            avatar_asset,
                            presence: author_presence,
                            affinity: author_affinity,
                        },
                        message: message.clone(),
                        show_header,
                    },
                ));
                previous_message = Some((
                    message.author_id.clone(),
                    message.timestamp_ms,
                    message.reply_to.is_some(),
                ));
            }

            self.models.timeline.typing_text = snapshot.timeline.typing_text.clone();
            if loading_older {
                rows.insert(
                    0,
                    crate::models::timeline_model::TimelineRow::LoadingIndicator(
                        "Loading older messages…".to_string(),
                    ),
                );
            }

            self.models.timeline.rows = rows;
            self.sync_timeline_list_state_len(self.models.timeline.rows.len());
            if let Some(anchor_message_id) = timeline_anchor_message_id.as_ref()
                && self.models.timeline.pending_scroll_target.is_none()
                && let Some(row_index) =
                    timeline_row_index_for_message(&self.models.timeline.rows, anchor_message_id)
            {
                self.timeline_list_state.scroll_to(ListOffset {
                    item_ix: row_index,
                    offset_in_item: px(0.),
                });
            }
            self.apply_og_previews_to_timeline();
            self.sync_cache.timeline_rows_sig = Some(timeline_rows_sig);
            self.sync_cache.timeline_link_previews_sig = Some(timeline_link_previews_sig);
        } else if timeline_link_previews_changed {
            self.sync_timeline_row_link_previews(&timeline_messages);
            self.sync_cache.timeline_link_previews_sig = Some(timeline_link_previews_sig);
        }
        self.restore_pending_older_scroll_anchor(loading_older, loading_older_just_finished);

        if conversation_changed
            || timeline_emoji_changed
            || timeline_reactions_changed
            || timeline_author_roles_changed
            || timeline_rows_changed
        {
            self.timeline_row_render_cache.invalidate();
        }
        self.schedule_video_preview_decodes_for_messages(&timeline_messages);
        self.schedule_video_preview_decodes_for_search_results();
        self.schedule_og_fetches_for_messages(&timeline_messages);
        self.schedule_og_fetches_for_messages(&self.models.thread_pane.replies.clone());
        self.apply_pending_timeline_scroll_target();
        if (timeline_reactions_changed || timeline_rows_changed)
            && was_near_bottom_before_sync
            && self.models.timeline.pending_scroll_target.is_none()
        {
            // Row-content changes (e.g. reactions or "(edited)" badges) can increase the
            // last row height without changing list length. Keep the viewport pinned when
            // the user was already at the latest message.
            self.scroll_timeline_list_to_bottom();
        }
        if pinned_banner_visibility_changed
            && was_near_bottom_before_sync
            && self.models.timeline.pending_scroll_target.is_none()
        {
            // Keep the newest message in view when top chrome changes height.
            const PINNED_BANNER_HEIGHT_PX: f32 = 36.0;
            let delta = if has_pinned_banner {
                PINNED_BANNER_HEIGHT_PX
            } else {
                -PINNED_BANNER_HEIGHT_PX
            };
            self.timeline_list_state.scroll_by(px(delta));
            self.scroll_timeline_list_to_bottom();
        }

        // Clear jump_to_message_active once the list layout is settled and the pixel-based
        // near-bottom check is reliable (max_offset > 0 means the list has been painted).
        if self.jump_to_message_active
            && (conversation_changed
                || (self.models.timeline.pending_scroll_target.is_none()
                    && self.timeline_list_state.max_offset_for_scrollbar().height > px(0.)))
        {
            self.jump_to_message_active = false;
        }

        let elapsed = t0.elapsed();
        self.perf_harness
            .record_duration(PerfTimer::SyncModelsFromStore, elapsed);
        if self.perf_harness.is_capturing() && elapsed.as_millis() > 1 {
            tracing::warn!(
                "sync_models_from_store took {elapsed:?} (sidebar={} sections, timeline={} rows)",
                self.models.sidebar.sections.len(),
                self.models.timeline.rows.len(),
            );
        }
    }

    fn sync_thread_pane_from_store(&mut self) {
        let snapshot = self.app_store.snapshot();
        self.models.thread_pane.open = snapshot.thread.open;
        self.models.thread_pane.root_message_id = snapshot.thread.root_message_id.clone();
        self.models.thread_pane.replies = snapshot.thread.replies.clone();
        self.models.thread_pane.reply_draft = snapshot.thread.reply_draft.clone();
        self.models.thread_pane.loading = snapshot.thread.loading;
    }

    fn persist_settings(&mut self) {
        let _ = SettingsStore::save_to_disk(&self.models.settings);
    }

    fn apply_pending_timeline_scroll_target(&mut self) {
        let Some(target_message_id) = self.models.timeline.pending_scroll_target.clone() else {
            return;
        };
        let Some(row_index) =
            timeline_row_index_for_message(&self.models.timeline.rows, &target_message_id)
        else {
            return;
        };
        self.timeline_list_state.scroll_to(ListOffset {
            item_ix: row_index,
            offset_in_item: px(0.),
        });
        self.models.timeline.pending_scroll_target = None;
    }

    fn first_visible_timeline_message_id(&self) -> Option<MessageId> {
        let top = self.timeline_list_state.logical_scroll_top();
        self.models
            .timeline
            .rows
            .iter()
            .skip(top.item_ix)
            .find_map(|row| match row {
                crate::models::timeline_model::TimelineRow::Message(message_row) => {
                    Some(message_row.message.id.clone())
                }
                _ => None,
            })
    }

    fn restore_pending_older_scroll_anchor(&mut self, loading_older: bool, just_finished: bool) {
        if loading_older {
            return;
        }
        if !just_finished {
            // Prevent stale "restore" from firing on unrelated syncs (e.g. new messages).
            self.pending_older_scroll_anchor = None;
            self.pending_older_scroll_seq = None;
            return;
        }
        if let Some(pending_seq) = self.pending_older_scroll_seq
            && pending_seq != self.timeline_scroll_seq
        {
            self.pending_older_scroll_anchor = None;
            self.pending_older_scroll_seq = None;
            return;
        }
        if let Some(anchor_message_id) = self.pending_older_scroll_anchor.clone()
            && let Some(row_index) =
                timeline_row_index_for_message(&self.models.timeline.rows, &anchor_message_id)
        {
            self.timeline_list_state.scroll_to(ListOffset {
                item_ix: row_index,
                offset_in_item: px(0.),
            });
        }
        self.pending_older_scroll_anchor = None;
        self.pending_older_scroll_seq = None;
        if timeline_message_count(&self.models) <= self.last_timeline_message_count {
            self.suppress_next_timeline_bottom_snap = false;
        }
    }

    pub(crate) fn navigate_to_channel_link(
        &mut self,
        channel_name: &str,
        team_hint: Option<&str>,
        conv_id: Option<&ConversationId>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(conv_id) = conv_id {
            tracing::info!(
                "navigate_to_channel_link: using conv_id {} for #{channel_name}",
                conv_id.0
            );
            self.navigate_to_channel_by_conv_id(conv_id.clone(), window, cx);
        } else {
            tracing::info!(
                "navigate_to_channel_link: no conv_id, falling back to name resolution for #{channel_name}"
            );
            self.navigate_to_channel_by_name(channel_name, team_hint, window, cx);
        }
    }

    fn navigate_to_channel_by_conv_id(
        &mut self,
        conversation_id: ConversationId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let workspace_id = self.models.app.active_workspace_id.clone();

        // Check if already in the sidebar (user is a member)
        let sidebar_route = self
            .models
            .sidebar
            .sections
            .iter()
            .flat_map(|s| &s.rows)
            .find(|row| {
                matches!(&row.route, Route::Channel { channel_id: cid, .. } if cid.0 == conversation_id.0)
            })
            .map(|row| row.route.clone());

        if let Some(route) = sidebar_route {
            self.navigate_to(route, window, cx);
            return;
        }

        // Not in sidebar - resolve via backend (handles non-member preview)
        tracing::info!(
            "navigate_to_channel_by_conv_id: resolving {} via backend",
            conversation_id.0
        );
        let Ok(events) = self
            .backend_router
            .route_command(BackendCommand::ResolveChannelById {
                workspace_id: workspace_id.clone(),
                conversation_id: conversation_id.clone(),
            })
        else {
            tracing::warn!(
                "navigate_to_channel_by_conv_id: route_command returned Err for {}",
                conversation_id.0
            );
            cx.notify();
            return;
        };

        for event in events {
            match event {
                BackendEvent::ChannelResolved {
                    workspace_id,
                    conversation,
                    conversation_binding,
                    can_post,
                } => {
                    self.backend_router
                        .register_conversation_binding(conversation_binding.clone());
                    let _ = self
                        .app_store
                        .dispatch_backend(BackendEvent::ChannelResolved {
                            workspace_id: workspace_id.clone(),
                            conversation: conversation.clone(),
                            conversation_binding,
                            can_post,
                        });
                    self.preview_summaries
                        .insert(conversation.id.clone(), (conversation.clone(), can_post));
                    let conv_id_str = conversation.id.0.clone();
                    self.navigate_to(
                        Route::Channel {
                            workspace_id,
                            channel_id: crate::domain::ids::ChannelId::new(conv_id_str),
                        },
                        window,
                        cx,
                    );
                    self.models.conversation.summary = conversation.clone();
                    self.models.composer.conversation_id = conversation.id.clone();
                    self.models.conversation.can_post = can_post;
                    return;
                }
                BackendEvent::ChannelResolveFailed { error, .. } => {
                    tracing::warn!("navigate_to_channel_by_conv_id: resolve failed: {error}");
                    cx.notify();
                    return;
                }
                other => {
                    self.apply_backend_events_inline(vec![other]);
                }
            }
        }

        tracing::warn!(
            "navigate_to_channel_by_conv_id: no ChannelResolved event for {}",
            conversation_id.0
        );
        cx.notify();
    }

    pub(crate) fn navigate_to_channel_by_name(
        &mut self,
        channel_name: &str,
        team_hint: Option<&str>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let normalized_channel_name = channel_name
            .trim()
            .trim_start_matches('#')
            .trim()
            .to_string();
        if normalized_channel_name.is_empty() {
            self.models.push_toast("Channel name is empty", None);
            cx.notify();
            return;
        }

        let current_conv_id = conversation_id_from_navigated_route(&self.models.navigation.current);
        let workspace_team_name = current_conv_id.as_ref().and_then(|cid| {
            self.models
                .workspace
                .channels
                .iter()
                .find(|c| c.id == *cid)
                .and_then(|c| c.group.as_ref())
                .map(|g| g.id.clone())
        });
        let summary_team_name = self
            .models
            .conversation
            .summary
            .group
            .as_ref()
            .map(|group| group.id.clone());
        let backend_team_name = current_conv_id.as_ref().and_then(|cid| {
            self.app_store
                .snapshot()
                .backend
                .conversation_team_ids
                .get(cid)
                .cloned()
                .and_then(|value| {
                    let trimmed = value.trim();
                    if trimmed.is_empty() || looks_like_keybase_team_id(trimmed) {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
        });
        let hinted_team_name = team_hint.and_then(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then_some(trimmed.to_string())
        });
        let current_team_name = hinted_team_name
            .or(workspace_team_name)
            .or(summary_team_name)
            .or(backend_team_name)
            .and_then(|name| {
                let trimmed = name.trim();
                (!trimmed.is_empty()).then_some(trimmed.to_string())
            });

        let route = self
            .models
            .sidebar
            .sections
            .iter()
            .flat_map(|s| &s.rows)
            .find(|row| {
                if !row.label.eq_ignore_ascii_case(&normalized_channel_name) {
                    return false;
                }
                match (&row.route, &current_team_name) {
                    (
                        Route::Channel {
                            channel_id,
                            workspace_id: _,
                        },
                        Some(gid),
                    ) => {
                        let conv_id = ConversationId::new(channel_id.0.clone());
                        self.models
                            .workspace
                            .channels
                            .iter()
                            .find(|c| c.id == conv_id)
                            .and_then(|c| c.group.as_ref())
                            .is_some_and(|g| g.id == *gid)
                    }
                    (Route::Channel { .. }, None) => false,
                    _ => false,
                }
            })
            .map(|row| row.route.clone());

        if let Some(route) = route {
            tracing::info!(
                "navigate_to_channel_by_name: found in sidebar, navigating to {normalized_channel_name}"
            );
            self.navigate_to(route, window, cx);
        } else {
            let Some(team_name) = current_team_name else {
                tracing::warn!(
                    "navigate_to_channel_by_name: no team context for #{normalized_channel_name}"
                );
                cx.notify();
                return;
            };
            let workspace_id = self.models.app.active_workspace_id.clone();
            self.resolve_channel_and_navigate(
                workspace_id,
                team_name,
                normalized_channel_name,
                window,
                cx,
            );
        }
    }

    pub(crate) fn join_current_channel(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(conversation_id) =
            conversation_id_from_navigated_route(&self.models.navigation.current)
        else {
            tracing::warn!("join_current_channel: no conversation_id from route");
            return;
        };
        tracing::info!("join_current_channel: joining {}", conversation_id.0);
        let workspace_id = self.models.app.active_workspace_id.clone();
        let Ok(events) = self
            .backend_router
            .route_command(BackendCommand::JoinChannel {
                workspace_id: workspace_id.clone(),
                conversation_id: conversation_id.clone(),
            })
        else {
            tracing::warn!(
                "join_current_channel: route_command failed for {}",
                conversation_id.0
            );
            self.models.push_toast("Failed to join channel", None);
            cx.notify();
            return;
        };
        let joined = events
            .iter()
            .any(|event| matches!(event, BackendEvent::ChannelJoined { .. }));
        if !joined {
            tracing::warn!(
                "join_current_channel: no ChannelJoined event for {}",
                conversation_id.0
            );
            self.models.push_toast("Failed to join channel", None);
            cx.notify();
            return;
        }
        tracing::info!(
            "join_current_channel: successfully joined {}",
            conversation_id.0
        );
        self.preview_summaries.remove(&conversation_id);

        if let Ok(workspace_events) = self
            .backend_router
            .route_command(BackendCommand::LoadWorkspace { workspace_id })
        {
            self.apply_backend_events_inline(workspace_events);
        }
        self.models.conversation.can_post = true;
        self.sync_inputs_from_models(cx);
        self.restore_chat_focus(window, cx);
        self.refresh(cx);
    }

    pub(crate) fn navigate_to(
        &mut self,
        route: Route,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.capture_live_inputs(cx);
        self.models.dismiss_overlays();
        self.jump_to_message_active = false;
        let route_changed = self.models.navigation.current != route;
        // If the user switches to a different conversation, don't keep any right pane open.
        // Panes like Details/Thread/Profile are scoped to the current conversation/user context
        // and should be explicitly opened.
        if route_changed
            && conversation_id_from_navigated_route(&route).is_some()
            && conversation_id_from_navigated_route(&self.models.navigation.current)
                != conversation_id_from_navigated_route(&route)
            && self.models.navigation.right_pane != RightPaneMode::Hidden
        {
            self.dispatch_ui_action(UiAction::CloseRightPane);
            self.models.set_right_pane(RightPaneMode::Hidden);
            self.models.profile_panel.user_id = None;
            self.models.profile_panel.profile = None;
            self.reset_thread_scroll_state();
        }
        if route_changed {
            self.models.navigate_to(route.clone());
            self.sync_models_from_store();
        }
        self.dispatch_ui_action(UiAction::Navigate(route.clone()));
        if let Some(conversation_id) = conversation_id_from_navigated_route(&route) {
            self.dispatch_ui_action(UiAction::MarkConversationRead {
                conversation_id,
                message_id: None,
            });
        }
        self.models.expand_section_for_route(&route);
        self.reset_timeline_scroll_state();
        self.reset_thread_scroll_state();
        if matches!(route, Route::Channel { .. } | Route::DirectMessage { .. }) {
            self.scroll_timeline_list_to_bottom();
        }
        self.sync_inputs_from_models(cx);
        self.restore_chat_focus(window, cx);
        self.refresh(cx);
    }

    fn resolve_channel_and_navigate(
        &mut self,
        workspace_id: WorkspaceId,
        team_name: String,
        channel_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let team_for_error = team_name.clone();
        tracing::info!(
            "resolve_channel_and_navigate: resolving #{channel_name} in team={team_name}"
        );
        let Ok(events) = self
            .backend_router
            .route_command(BackendCommand::ResolveChannel {
                workspace_id: workspace_id.clone(),
                team_name,
                channel_name: channel_name.clone(),
            })
        else {
            tracing::warn!(
                "resolve_channel_and_navigate: route_command returned Err for #{channel_name} in {team_for_error}"
            );
            cx.notify();
            return;
        };

        for event in events {
            match event {
                BackendEvent::ChannelResolved {
                    workspace_id,
                    conversation,
                    conversation_binding,
                    can_post,
                } => {
                    self.backend_router
                        .register_conversation_binding(conversation_binding.clone());
                    let _ = self
                        .app_store
                        .dispatch_backend(BackendEvent::ChannelResolved {
                            workspace_id: workspace_id.clone(),
                            conversation: conversation.clone(),
                            conversation_binding,
                            can_post,
                        });
                    self.preview_summaries
                        .insert(conversation.id.clone(), (conversation.clone(), can_post));
                    let conv_id_str = conversation.id.0.clone();
                    self.navigate_to(
                        Route::Channel {
                            workspace_id,
                            channel_id: crate::domain::ids::ChannelId::new(conv_id_str),
                        },
                        window,
                        cx,
                    );
                    self.models.conversation.summary = conversation.clone();
                    self.models.composer.conversation_id = conversation.id.clone();
                    self.models.conversation.can_post = can_post;
                    return;
                }
                BackendEvent::ChannelResolveFailed { error, .. } => {
                    tracing::warn!(
                        "resolve_channel_and_navigate: failed for #{channel_name} in {team_for_error}: {error}"
                    );
                    cx.notify();
                    return;
                }
                other => {
                    self.apply_backend_events_inline(vec![other]);
                }
            }
        }

        self.models.push_toast(
            format!("Unable to open #{channel_name} in {team_for_error}"),
            None,
        );
        cx.notify();
    }

    fn apply_backend_events_inline(&mut self, events: Vec<BackendEvent>) {
        if events.is_empty() {
            return;
        }
        let mut pending_effects = Vec::new();
        for event in events {
            self.register_bindings_from_backend_event(&event);
            pending_effects.extend(self.app_store.dispatch_backend(event));
        }
        loop {
            if pending_effects.is_empty() {
                break;
            }
            let Ok(events) = self.backend_router.apply_effects(&pending_effects) else {
                break;
            };
            if events.is_empty() {
                break;
            }
            pending_effects.clear();
            for event in events {
                self.register_bindings_from_backend_event(&event);
                pending_effects.extend(self.app_store.dispatch_backend(event));
            }
        }
        self.sync_models_from_store();
    }

    fn register_bindings_from_backend_event(&mut self, event: &BackendEvent) {
        match event {
            BackendEvent::BootstrapLoaded { payload, .. } => {
                for binding in &payload.workspace_bindings {
                    self.backend_router
                        .register_workspace_binding(binding.clone());
                }
                for binding in &payload.conversation_bindings {
                    self.backend_router
                        .register_conversation_binding(binding.clone());
                }
                for binding in &payload.message_bindings {
                    self.backend_router
                        .register_message_binding(binding.clone());
                }
            }
            BackendEvent::WorkspaceConversationsExtended {
                conversation_bindings,
                ..
            } => {
                for binding in conversation_bindings {
                    self.backend_router
                        .register_conversation_binding(binding.clone());
                }
            }
            BackendEvent::ConversationCreated {
                conversation_binding,
                ..
            } => {
                self.backend_router
                    .register_conversation_binding(conversation_binding.clone());
            }
            BackendEvent::ChannelResolved {
                conversation_binding,
                ..
            } => {
                self.backend_router
                    .register_conversation_binding(conversation_binding.clone());
            }
            _ => {}
        }
    }

    pub(crate) fn navigate_to_message(
        &mut self,
        route: Route,
        message_id: MessageId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.capture_live_inputs(cx);
        self.models.dismiss_overlays();
        let route_changed = self.models.navigation.current != route;
        // Same as navigate_to: switching conversations should close any right pane.
        if route_changed
            && conversation_id_from_navigated_route(&route).is_some()
            && conversation_id_from_navigated_route(&self.models.navigation.current)
                != conversation_id_from_navigated_route(&route)
            && self.models.navigation.right_pane != RightPaneMode::Hidden
        {
            self.dispatch_ui_action(UiAction::CloseRightPane);
            self.models.set_right_pane(RightPaneMode::Hidden);
            self.models.profile_panel.user_id = None;
            self.models.profile_panel.profile = None;
            self.reset_thread_scroll_state();
        }
        if route_changed {
            self.models.navigate_to(route.clone());
        }
        // Use NavigateQuiet to set navigation state without triggering LoadConversation.
        // LoadConversation spawns a background task that sends TimelineReplaced with recent
        // messages, which can race with JumpToMessage and override the scroll position.
        self.dispatch_ui_action(UiAction::NavigateQuiet(route.clone()));
        self.models.expand_section_for_route(&route);
        self.models.timeline.pending_scroll_target = Some(message_id.clone());
        self.models.timeline.highlighted_message_id = Some(message_id.clone());
        self.jump_to_message_active = true;
        if let Some(conversation_id) = self.app_store.snapshot().timeline.conversation_id.clone() {
            self.dispatch_ui_action(UiAction::JumpToMessage {
                conversation_id,
                message_id,
            });
        }
        self.reset_timeline_scroll_state();
        self.suppress_next_timeline_unseen_increment = true;
        self.reset_thread_scroll_state();
        self.sync_inputs_from_models(cx);
        if route_changed {
            // Trigger the full conversation load (emojis, profiles, team roles) now that
            // JumpToMessage has settled the scroll position. The background TimelineReplaced
            // from this load will merge with the jump-centered messages; the
            // jump_to_message_active flag prevents scroll-to-bottom overrides.
            self.dispatch_ui_action(UiAction::Navigate(route.clone()));
        }
        self.restore_chat_focus(window, cx);
        self.refresh(cx);
    }

    pub(crate) fn jump_to_pinned_message(
        &mut self,
        _: &ClickEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(message_id) = self
            .models
            .conversation
            .pinned_message
            .as_ref()
            .and_then(|pinned| pinned.message_id().cloned())
        else {
            return;
        };
        self.models.timeline.pending_scroll_target = Some(message_id.clone());
        self.models.timeline.highlighted_message_id = Some(message_id.clone());
        self.apply_pending_timeline_scroll_target();
        if self.models.timeline.pending_scroll_target.is_some()
            && let Some(conversation_id) =
                self.app_store.snapshot().timeline.conversation_id.clone()
        {
            self.dispatch_ui_action(UiAction::JumpToMessage {
                conversation_id,
                message_id,
            });
        }
        self.suppress_next_timeline_unseen_increment = true;
        self.refresh(cx);
    }

    pub(crate) fn dismiss_pinned_banner(
        &mut self,
        _: &ClickEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.models.dismiss_current_pinned_banner() {
            self.persist_settings();
            self.refresh(cx);
        }
    }

    pub(crate) fn navigate_back(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.capture_live_inputs(cx);
        if self.models.navigate_back() {
            let route = self.models.navigation.current.clone();
            self.dispatch_ui_action(UiAction::Navigate(route.clone()));
            self.models.expand_section_for_route(&route);
            self.reset_timeline_scroll_state();
            self.reset_thread_scroll_state();
            if matches!(route, Route::Channel { .. } | Route::DirectMessage { .. }) {
                self.scroll_timeline_list_to_bottom();
            }
            self.sync_inputs_from_models(cx);
            self.restore_chat_focus(window, cx);
            self.refresh(cx);
        }
    }

    pub(crate) fn navigate_forward(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.capture_live_inputs(cx);
        if self.models.navigate_forward() {
            let route = self.models.navigation.current.clone();
            self.dispatch_ui_action(UiAction::Navigate(route.clone()));
            self.models.expand_section_for_route(&route);
            self.reset_timeline_scroll_state();
            self.reset_thread_scroll_state();
            if matches!(route, Route::Channel { .. } | Route::DirectMessage { .. }) {
                self.scroll_timeline_list_to_bottom();
            }
            self.sync_inputs_from_models(cx);
            self.restore_chat_focus(window, cx);
            self.refresh(cx);
        }
    }

    pub(crate) fn toggle_pane(
        &mut self,
        pane: RightPaneMode,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if pane != RightPaneMode::Hidden && !self.models.settings.show_right_pane {
            self.models.push_toast(
                "Right pane is disabled in preferences",
                Some(ToastAction::OpenPreferences),
            );
            self.refresh(cx);
            return;
        }

        let next = if self.models.navigation.right_pane == pane {
            RightPaneMode::Hidden
        } else {
            pane
        };
        self.models.set_right_pane(next);
        self.refresh(cx);
    }

    pub(crate) fn open_dm_header_profile(&mut self, cx: &mut Context<Self>) {
        let conv_id = &self.models.conversation.summary.id;
        let username = self
            .models
            .workspace
            .direct_messages
            .iter()
            .find(|dm| dm.id == *conv_id)
            .and_then(|dm| {
                dm.title
                    .split(',')
                    .map(str::trim)
                    .find(|s| !s.is_empty())
                    .map(|s| s.to_string())
            });
        if let Some(username) = username {
            self.open_user_profile_card(UserId::new(username), cx);
        }
    }

    pub(crate) fn open_user_profile_card(&mut self, user_id: UserId, cx: &mut Context<Self>) {
        if !self.models.settings.show_right_pane {
            self.models.push_toast(
                "Right pane is disabled in preferences",
                Some(ToastAction::OpenPreferences),
            );
            self.refresh(cx);
            return;
        }
        self.models.hide_profile_card();
        self.models.profile_panel.user_id = Some(user_id.clone());
        self.models
            .set_right_pane(RightPaneMode::Profile(user_id.clone()));
        self.dispatch_ui_action(UiAction::ShowUserProfilePanel {
            user_id: user_id.clone(),
        });
        self.dispatch_ui_action(UiAction::RefreshProfilePresence {
            user_id: user_id.clone(),
            conversation_id: self.app_store.snapshot().timeline.conversation_id.clone(),
        });
        self.maybe_load_profile_social_tab(&user_id);
        self.refresh(cx);
    }

    pub(crate) fn open_user_profile_panel(
        &mut self,
        user_id: UserId,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.models.settings.show_right_pane {
            self.models.push_toast(
                "Right pane is disabled in preferences",
                Some(ToastAction::OpenPreferences),
            );
            self.refresh(cx);
            return;
        }

        self.models.hide_profile_card();
        self.models.profile_panel.user_id = Some(user_id.clone());
        self.models
            .set_right_pane(RightPaneMode::Profile(user_id.clone()));
        self.dispatch_ui_action(UiAction::ShowUserProfilePanel {
            user_id: user_id.clone(),
        });
        self.dispatch_ui_action(UiAction::RefreshProfilePresence {
            user_id: user_id.clone(),
            conversation_id: self.app_store.snapshot().timeline.conversation_id.clone(),
        });
        self.maybe_load_profile_social_tab(&user_id);
        self.refresh(cx);
    }

    pub(crate) fn profile_open_message(&mut self, user_id: UserId, cx: &mut Context<Self>) {
        self.dispatch_ui_action(UiAction::OpenOrCreateDirectMessage { user_id });
        self.refresh(cx);
    }

    pub(crate) fn profile_select_social_tab(
        &mut self,
        list_type: SocialGraphListType,
        cx: &mut Context<Self>,
    ) {
        self.models.profile_panel.active_social_tab = match list_type {
            SocialGraphListType::Followers => SocialTab::Followers,
            SocialGraphListType::Following => SocialTab::Following,
        };
        let Some(user_id) = self.models.profile_panel.user_id.clone() else {
            return;
        };
        self.dispatch_ui_action(UiAction::LoadSocialGraphList { user_id, list_type });
        self.refresh(cx);
    }

    pub(crate) fn profile_follow_user(&mut self, user_id: UserId, cx: &mut Context<Self>) {
        self.dispatch_ui_action(UiAction::FollowUser { user_id });
        self.refresh(cx);
    }

    pub(crate) fn profile_unfollow_user(&mut self, user_id: UserId, cx: &mut Context<Self>) {
        self.dispatch_ui_action(UiAction::UnfollowUser { user_id });
        self.refresh(cx);
    }

    fn maybe_load_profile_social_tab(&mut self, user_id: &UserId) {
        let Some(profile) = self.models.profile_panel.profile.as_ref() else {
            return;
        };
        let active_tab = self.models.profile_panel.active_social_tab.as_list_type();
        let list_loaded = profile
            .sections
            .iter()
            .find_map(|section| match section {
                crate::domain::profile::ProfileSection::SocialGraph(graph) => {
                    Some(match active_tab {
                        SocialGraphListType::Followers => graph.followers.is_some(),
                        SocialGraphListType::Following => graph.following.is_some(),
                    })
                }
                _ => None,
            })
            .unwrap_or(false);
        if !list_loaded {
            self.dispatch_ui_action(UiAction::LoadSocialGraphList {
                user_id: user_id.clone(),
                list_type: active_tab,
            });
        }
    }

    pub(crate) fn open_thread(
        &mut self,
        root_id: MessageId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.capture_live_inputs(cx);
        if !self.models.settings.show_right_pane {
            self.models.push_toast(
                "Enable the right pane before opening threads",
                Some(ToastAction::OpenPreferences),
            );
            self.refresh(cx);
            return;
        }

        self.ensure_message_binding_for_active_conversation(&root_id);
        self.models.open_thread(root_id.clone());
        self.dispatch_ui_action(UiAction::OpenThread {
            root_id: root_id.clone(),
        });
        self.reset_thread_scroll_state();
        self.pending_thread_scroll_to_bottom = true;
        self.sync_inputs_from_models(cx);
        window.focus(&self.thread_input.focus_handle(cx));
        self.refresh(cx);
    }

    fn ensure_message_binding_for_active_conversation(&mut self, message_id: &MessageId) {
        if self
            .app_store
            .snapshot()
            .backend
            .message_bindings
            .contains_key(message_id)
        {
            return;
        }
        let Some(conversation_id) = self.app_store.snapshot().timeline.conversation_id.clone()
        else {
            return;
        };
        let Some(conversation_binding) = self
            .app_store
            .snapshot()
            .backend
            .conversation_bindings
            .get(&conversation_id)
            .cloned()
        else {
            return;
        };
        let binding = MessageBinding {
            message_id: message_id.clone(),
            backend_id: conversation_binding.backend_id,
            account_id: conversation_binding.account_id,
            provider_message_ref: ProviderMessageRef::new(message_id.0.clone()),
        };
        self.backend_router
            .register_message_binding(binding.clone());
        self.app_store.register_message_binding(binding);
    }

    pub(crate) fn close_right_pane(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.capture_live_inputs(cx);
        self.dispatch_ui_action(UiAction::CloseRightPane);
        self.models.set_right_pane(RightPaneMode::Hidden);
        self.models.profile_panel.user_id = None;
        self.models.profile_panel.profile = None;
        self.reset_thread_scroll_state();
        self.refresh(cx);
    }

    fn timeline_interactions_blocked_by_overlay(&self) -> bool {
        timeline_interactions_blocked_by_overlay_state(
            &self.models.overlay,
            self.keybase_inspector.open,
        )
    }

    pub(crate) fn timeline_scrolled(
        &mut self,
        event: &ScrollWheelEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.timeline_interactions_blocked_by_overlay() {
            return;
        }
        let boost = match event.delta {
            ScrollDelta::Pixels(delta) => -delta.y,
            ScrollDelta::Lines(delta) => px(-delta.y * 20.),
        };
        if boost != px(0.) {
            self.timeline_scroll_seq = self.timeline_scroll_seq.wrapping_add(1);
            self.timeline_list_state.scroll_by(boost);
        }
        let mut should_refresh = self.sync_scroll_indicators();
        if self.timeline_is_near_top() && self.request_timeline_older_page_if_needed() {
            should_refresh = true;
        }
        if self.request_mark_conversation_read_if_needed() {
            should_refresh = true;
        }
        if should_refresh {
            self.refresh(cx);
        }
    }

    pub(crate) fn render_timeline_row_virtualized(
        &mut self,
        ix: usize,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = self.resolved_theme;
        with_theme(theme, || {
            let timeline = &self.models.timeline;
            let Some(row) = timeline.rows.get(ix) else {
                return div().into_any_element();
            };
            let find_query = if self.models.find_in_chat.open {
                Some(self.models.find_in_chat.query.as_str())
            } else {
                None
            };
            crate::views::timeline::TimelineList::render_row_cached(
                timeline,
                ix,
                row,
                &mut self.timeline_row_render_cache,
                find_query,
                &self.video_render_cache,
                &self.failed_video_urls,
                &mut self.code_highlight_cache,
                &mut self.selectable_texts,
                cx,
            )
        })
    }

    pub(crate) fn thread_scrolled(
        &mut self,
        _: &ScrollWheelEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.timeline_interactions_blocked_by_overlay() {
            return;
        }
        if self.sync_scroll_indicators() {
            self.refresh(cx);
        }
    }

    pub(crate) fn profile_scrolled(
        &mut self,
        _: &ScrollWheelEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.perf_harness.record_refresh();
        cx.notify();
    }

    fn request_timeline_older_page_if_needed(&mut self) -> bool {
        let snapshot = self.app_store.snapshot();
        if snapshot.timeline.loading_older {
            return false;
        }
        if self.models.timeline.rows.is_empty() {
            return false;
        }
        let Some(cursor) = snapshot.timeline.older_cursor.clone() else {
            return false;
        };
        let cursor = cursor.trim().to_string();
        if cursor.is_empty() {
            return false;
        }
        self.pending_older_scroll_anchor = self.first_visible_timeline_message_id();
        self.pending_older_scroll_seq = Some(self.timeline_scroll_seq);
        self.suppress_next_timeline_bottom_snap = true;
        let conversation_id = self.models.timeline.conversation_id.clone();
        self.dispatch_ui_action(UiAction::LoadOlderMessages {
            key: TimelineKey::Conversation(conversation_id),
            cursor,
        });
        true
    }

    fn request_mark_conversation_read_if_needed(&mut self) -> bool {
        if !self.window_is_focused {
            return false;
        }
        let has_visible_message_rows = self
            .models
            .timeline
            .rows
            .iter()
            .any(|row| matches!(row, crate::models::timeline_model::TimelineRow::Message(_)));
        if !self.timeline_is_near_bottom() && has_visible_message_rows {
            return false;
        }
        let conversation_id = self.models.timeline.conversation_id.clone();
        if conversation_id.0.is_empty() {
            return false;
        }
        let latest_message_id = self
            .models
            .timeline
            .newer_cursor
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| MessageId::new(value.to_string()))
            .or_else(|| {
                let snapshot = self.app_store.snapshot();
                latest_timeline_snapshot_message_id(&snapshot.timeline.messages)
                    .or_else(|| latest_timeline_message_id(&self.models.timeline.rows))
            });
        let has_unread_marker = self
            .models
            .timeline
            .unread_marker
            .as_ref()
            .is_some_and(|marker| {
                latest_message_id
                    .as_ref()
                    .is_some_and(|latest| message_id_is_after(latest, marker))
            });
        let has_unread_sidebar_dot = sidebar_route_unread(
            &self.models.sidebar.sections,
            &self.models.navigation.current,
        );
        if !has_unread_marker && !has_unread_sidebar_dot {
            return false;
        }
        let current_message_key = latest_message_id
            .as_ref()
            .map(|value| value.0.clone())
            .unwrap_or_default();
        if let Some((last_message_key, last_attempt_at)) =
            self.last_mark_read_attempt.get(&conversation_id.0)
            && last_message_key == &current_message_key
            && last_attempt_at.elapsed() < MARK_READ_THROTTLE
        {
            return false;
        }
        self.dispatch_ui_action(UiAction::MarkConversationRead {
            conversation_id: conversation_id.clone(),
            message_id: None,
        });
        self.last_mark_read_attempt.insert(
            conversation_id.0.clone(),
            (current_message_key, Instant::now()),
        );
        true
    }

    fn mark_all_conversations_read(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let unread: Vec<_> = self
            .models
            .workspace
            .channels
            .iter()
            .chain(self.models.workspace.direct_messages.iter())
            .filter(|c| c.unread_count > 0)
            .map(|c| c.id.clone())
            .collect();
        let count = unread.len();
        for conversation_id in unread {
            self.dispatch_ui_action(UiAction::MarkConversationRead {
                conversation_id,
                message_id: None,
            });
        }
        if count > 0 {
            self.models.push_toast(
                format!(
                    "Marked {} conversation{} as read",
                    count,
                    if count == 1 { "" } else { "s" }
                ),
                None,
            );
        }
        self.restore_chat_focus(window, cx);
        self.refresh(cx);
    }

    pub(crate) fn scroll_timeline_to_bottom(
        &mut self,
        _: &ClickEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.timeline_unseen_count = 0;
        self.suppress_next_timeline_unseen_increment = false;
        self.scroll_timeline_list_to_bottom();
        let _ = self.request_mark_conversation_read_if_needed();
        self.refresh(cx);
    }

    pub(crate) fn scroll_thread_to_bottom(
        &mut self,
        _: &ClickEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.thread_unseen_count = 0;
        self.thread_scroll.scroll_to_bottom();
        self.refresh(cx);
    }

    pub(crate) fn open_search_query(
        &mut self,
        query: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.search_input
            .update(cx, |input, cx| input.set_text(query, cx));
        self.submit_search_input(window, cx);
    }

    pub(crate) fn submit_search_input(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.models.dismiss_overlays();
        self.dispatch_ui_action(UiAction::SubmitSearch);
        let query = self.search_input.read(cx).text();
        self.navigate_to(
            Route::Search {
                workspace_id: self.models.app.active_workspace_id.clone(),
                query,
            },
            window,
            cx,
        );
    }

    pub(crate) fn open_search_result_at(
        &mut self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(result) = self.models.search.results.get(index).cloned() else {
            return;
        };
        self.navigate_to_message(result.route, result.message.id, window, cx);
    }

    pub(crate) fn open_activity_item_at(
        &mut self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some((route, message_id)) = self.models.open_activity_item(index) else {
            return;
        };
        if let Some(message_id) = message_id {
            self.navigate_to_message(route, message_id, window, cx);
        } else {
            self.navigate_to(route, window, cx);
        }
    }

    pub(crate) fn send_composer_message(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let composer_text = self.composer_input.read(cx).text();
        self.models
            .update_composer_draft_text(composer_text.clone());
        if let ComposerMode::Edit { message_id } = self.models.composer.mode.clone() {
            let edited_text = self.models.composer.draft_text.trim().to_string();
            if edited_text.is_empty() {
                self.delete_message(message_id, cx);
                window.focus(&self.composer_input.focus_handle(cx));
                return;
            }
            let was_near_bottom = self.timeline_is_near_bottom();
            self.ensure_message_binding_for_active_conversation(&message_id);
            self.dispatch_ui_action(UiAction::EditMessage {
                conversation_id: self.models.composer.conversation_id.clone(),
                message_id,
                text: edited_text,
            });
            let (dispatch, pending) = self.models.send_composer_message();
            if matches!(dispatch, SendDispatch::NotSent) {
                return;
            }
            self.sync_inputs_from_models(cx);
            self.refresh(cx);
            if was_near_bottom && self.models.timeline.pending_scroll_target.is_none() {
                self.scroll_timeline_list_to_bottom();
            }
            window.focus(&self.composer_input.focus_handle(cx));
            if let Some(pending) = pending {
                self.schedule_pending_send(pending, cx);
            }
            return;
        }
        let draft_key = DraftKey::Conversation(self.models.composer.conversation_id.clone());
        let store_draft_matches = self
            .app_store
            .snapshot()
            .drafts
            .get(&draft_key)
            .map(|draft| draft.text.as_str())
            == Some(composer_text.as_str());
        if !store_draft_matches {
            self.dispatch_ui_action(UiAction::UpdateDraft {
                key: draft_key.clone(),
                text: composer_text,
            });
        }
        self.dispatch_ui_action(UiAction::SendMessage { key: draft_key });
        let (dispatch, pending) = self.models.send_composer_message();
        if matches!(dispatch, SendDispatch::NotSent) {
            return;
        }

        self.sync_inputs_from_models(cx);
        self.refresh(cx);
        window.focus(&self.composer_input.focus_handle(cx));

        if let Some(pending) = pending {
            self.schedule_pending_send(pending, cx);
        }
    }

    pub(crate) fn toggle_thread_following(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if self.models.toggle_thread_following() {
            self.refresh(cx);
        }
    }

    pub(crate) fn send_thread_reply(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let thread_text = self.thread_input.read(cx).text();
        self.models.update_thread_reply_draft(thread_text.clone());
        let Some(root_id) = self.models.thread_pane.root_message_id.clone() else {
            return;
        };
        let draft_key = DraftKey::Thread(root_id);
        let store_draft_matches = self
            .app_store
            .snapshot()
            .drafts
            .get(&draft_key)
            .map(|draft| draft.text.as_str())
            == Some(thread_text.as_str());
        if !store_draft_matches {
            self.dispatch_ui_action(UiAction::UpdateDraft {
                key: draft_key.clone(),
                text: thread_text,
            });
        }
        self.dispatch_ui_action(UiAction::SendMessage { key: draft_key });
        let (dispatch, pending) = self.models.send_thread_reply();
        if matches!(dispatch, SendDispatch::NotSent) {
            return;
        }

        self.sync_inputs_from_models(cx);
        self.refresh(cx);
        window.focus(&self.thread_input.focus_handle(cx));

        if let Some(pending) = pending {
            self.schedule_pending_send(pending, cx);
        }
    }

    fn autocomplete_target_for_focused_input(
        &self,
        window: &Window,
        cx: &mut Context<Self>,
    ) -> Option<InputAutocompleteTarget> {
        if self.composer_input.focus_handle(cx).is_focused(window) {
            return Some(InputAutocompleteTarget::Composer);
        }
        if self.thread_input.focus_handle(cx).is_focused(window) {
            return Some(InputAutocompleteTarget::Thread);
        }
        None
    }

    fn move_autocomplete_selection(
        &mut self,
        target: InputAutocompleteTarget,
        direction: isize,
    ) -> bool {
        let now = Instant::now();
        if let Some((last_target, last_direction, last_at)) = self.last_autocomplete_nav
            && last_target == target
            && last_direction == direction
            && now.duration_since(last_at) < AUTOCOMPLETE_NAV_DEBOUNCE
        {
            return false;
        }
        self.last_autocomplete_nav = Some((target, direction, now));

        let autocomplete = match target {
            InputAutocompleteTarget::Composer => &mut self.models.composer.autocomplete,
            InputAutocompleteTarget::Thread => &mut self.models.thread_pane.reply_autocomplete,
        };
        let Some(state) = autocomplete.as_mut() else {
            return false;
        };
        let len = state.candidates.len();
        if len == 0 {
            return false;
        }
        let current = state.selected_index.min(len.saturating_sub(1));
        let next = (current as isize + direction).rem_euclid(len as isize) as usize;
        if next == state.selected_index {
            return false;
        }
        state.selected_index = next;
        true
    }

    fn clear_autocomplete_for_target(
        &mut self,
        target: InputAutocompleteTarget,
        _cx: &mut Context<Self>,
    ) -> bool {
        match target {
            InputAutocompleteTarget::Composer => {
                if self.models.composer.autocomplete.is_none() {
                    return false;
                }
                self.models.set_composer_autocomplete(None);
                true
            }
            InputAutocompleteTarget::Thread => {
                if self.models.thread_pane.reply_autocomplete.is_none() {
                    return false;
                }
                self.models.set_thread_autocomplete(None);
                true
            }
        }
    }

    fn accept_autocomplete_selection(
        &mut self,
        target: InputAutocompleteTarget,
        selected_index_override: Option<usize>,
        cx: &mut Context<Self>,
    ) -> bool {
        let state = match target {
            InputAutocompleteTarget::Composer => self.models.composer.autocomplete.clone(),
            InputAutocompleteTarget::Thread => self.models.thread_pane.reply_autocomplete.clone(),
        };
        let Some(state) = state else {
            return false;
        };
        if state.candidates.is_empty() {
            return false;
        }
        let selected_index = selected_index_override
            .unwrap_or(state.selected_index)
            .min(state.candidates.len().saturating_sub(1));
        let Some(selected) = state.candidates.get(selected_index) else {
            return false;
        };
        let completion = selected.completion_text();
        match target {
            InputAutocompleteTarget::Composer => {
                let cursor = self.composer_input.read(cx).cursor_offset();
                if state.trigger_offset > cursor {
                    return false;
                }
                self.composer_input.update(cx, |input, cx| {
                    input.replace_range(state.trigger_offset..cursor, &completion, cx);
                });
                self.models.set_composer_autocomplete(None);
                self.models
                    .update_composer_draft_text(self.composer_input.read(cx).text());
            }
            InputAutocompleteTarget::Thread => {
                let cursor = self.thread_input.read(cx).cursor_offset();
                if state.trigger_offset > cursor {
                    return false;
                }
                self.thread_input.update(cx, |input, cx| {
                    input.replace_range(state.trigger_offset..cursor, &completion, cx);
                });
                self.models.set_thread_autocomplete(None);
                self.models
                    .update_thread_reply_draft(self.thread_input.read(cx).text());
            }
        }
        true
    }

    pub(crate) fn select_composer_autocomplete_index(
        &mut self,
        index: usize,
        cx: &mut Context<Self>,
    ) {
        if self.accept_autocomplete_selection(InputAutocompleteTarget::Composer, Some(index), cx) {
            self.refresh(cx);
        }
    }

    pub(crate) fn select_thread_autocomplete_index(
        &mut self,
        index: usize,
        cx: &mut Context<Self>,
    ) {
        if self.accept_autocomplete_selection(InputAutocompleteTarget::Thread, Some(index), cx) {
            self.refresh(cx);
        }
    }

    fn schedule_pending_send(&mut self, pending: PendingSend, cx: &mut Context<Self>) {
        let delay = match pending {
            PendingSend::TimelineMessage(_) => Duration::from_millis(850),
        };
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let background = cx.background_executor().clone();
            let mut async_app = cx.clone();
            async move {
                background.timer(delay).await;
                let _ = this.update(&mut async_app, |this, cx| {
                    this.complete_pending_send(pending.clone(), cx);
                });
            }
        })
        .detach();
    }

    fn complete_pending_send(&mut self, pending: PendingSend, cx: &mut Context<Self>) {
        if self.models.complete_pending_send(&pending).is_some() {
            self.sync_inputs_from_models(cx);
            self.refresh(cx);
        }
    }

    pub(crate) fn cycle_theme(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.models.cycle_theme();
        self.persist_settings();
        self.refresh(cx);
    }

    pub(crate) fn focus_search_input(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        window.focus(&self.search_input.focus_handle(cx));
    }

    pub(crate) fn focus_find_in_chat_input(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        window.focus(&self.find_in_chat_input.focus_handle(cx));
    }

    pub(crate) fn focus_quick_switcher_input(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        window.focus(&self.quick_switcher_input.focus_handle(cx));
    }

    pub(crate) fn focus_new_chat_input(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        window.focus(&self.new_chat_input.focus_handle(cx));
    }

    pub(crate) fn focus_emoji_picker_input(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        window.focus(&self.emoji_picker_input.focus_handle(cx));
    }

    pub(crate) fn focus_file_upload_caption_input(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        window.focus(&self.file_upload_caption_input.focus_handle(cx));
    }

    pub(crate) fn focus_composer_input(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        cx.stop_propagation();
        window.focus(&self.composer_input.focus_handle(cx));
    }

    pub(crate) fn restore_chat_focus(&self, window: &mut Window, cx: &mut Context<Self>) {
        let o = &self.models.overlay;
        if o.quick_switcher_open
            || o.command_palette_open
            || o.emoji_picker_open
            || o.new_chat_open
            || o.fullscreen_image.is_some()
            || o.file_upload_lightbox.is_some()
            || o.profile_card_user_id.is_some()
            || self.keybase_inspector.open
            || self.splash_open
        {
            return;
        }
        if !matches!(
            self.models.navigation.current,
            Route::Channel { .. } | Route::DirectMessage { .. }
        ) {
            window.focus(&self.focus_handle);
            return;
        }
        if !self.models.conversation.can_post {
            window.focus(&self.focus_handle);
            return;
        }
        if self.models.navigation.right_pane == RightPaneMode::Thread {
            let composer_focused = self.composer_input.focus_handle(cx).is_focused(window);
            let thread_focused = self.thread_input.focus_handle(cx).is_focused(window);
            if composer_focused && !thread_focused {
                window.focus(&self.composer_input.focus_handle(cx));
            } else {
                window.focus(&self.thread_input.focus_handle(cx));
            }
        } else {
            window.focus(&self.composer_input.focus_handle(cx));
        }
    }

    pub(crate) fn focus_thread_input(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        cx.stop_propagation();
        window.focus(&self.thread_input.focus_handle(cx));
    }

    pub(crate) fn focus_sidebar_filter_input(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        window.focus(&self.sidebar_filter_input.focus_handle(cx));
    }

    pub(crate) fn back_click(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_back(window, cx);
    }

    pub(crate) fn forward_click(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_forward(window, cx);
    }

    pub(crate) fn home_click(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_to(self.active_workspace_route(), window, cx);
    }

    pub(crate) fn activity_click(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_to(self.activity_route(), window, cx);
    }

    pub(crate) fn preferences_click(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_to(Route::Preferences, window, cx);
    }

    pub(crate) fn search_click(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.submit_search_input(window, cx);
    }

    pub(crate) fn toggle_search_filter_click(
        &mut self,
        filter: SearchFilter,
        cx: &mut Context<Self>,
    ) {
        self.dispatch_ui_action(UiAction::ToggleSearchFilter(filter));
        self.refresh(cx);
    }

    pub(crate) fn toggle_sidebar_section_click(
        &mut self,
        section_id: SidebarSectionId,
        cx: &mut Context<Self>,
    ) {
        self.models.toggle_sidebar_section(&section_id);
        let account_key = self.models.app.active_workspace_id.0.clone();
        let collapsed_set = self
            .models
            .sidebar
            .sections
            .iter()
            .filter(|s| s.collapsed && s.id.0 != "unread")
            .map(|s| s.id.0.clone())
            .collect();
        self.models
            .settings
            .sidebar_collapsed_sections
            .insert(account_key, collapsed_set);
        self.persist_settings();
        self.refresh(cx);
    }

    pub(crate) fn reorder_sidebar_section(
        &mut self,
        dragged_id: SidebarSectionId,
        target_id: SidebarSectionId,
        cx: &mut Context<Self>,
    ) {
        self.models.reorder_sidebar_section(&dragged_id, &target_id);
        let account_key = self.models.app.active_workspace_id.0.clone();
        let order = self
            .models
            .sidebar
            .sections
            .iter()
            .filter(|section| section.id.0 != "unread")
            .map(|s| s.id.0.clone())
            .collect();
        self.models
            .settings
            .sidebar_section_order
            .insert(account_key, order);
        self.persist_settings();
        self.refresh(cx);
    }

    pub(crate) fn highlight_timeline_message(
        &mut self,
        message_id: MessageId,
        cx: &mut Context<Self>,
    ) {
        self.models.highlight_timeline_message(&message_id);
        self.refresh(cx);
    }

    pub(crate) fn increase_thread_width(&mut self, cx: &mut Context<Self>) {
        let next = self.models.thread_pane.width_px + 32.0;
        self.models.set_thread_width(next);
        self.refresh(cx);
    }

    pub(crate) fn decrease_thread_width(&mut self, cx: &mut Context<Self>) {
        let next = self.models.thread_pane.width_px - 32.0;
        self.models.set_thread_width(next);
        self.refresh(cx);
    }

    pub(crate) fn begin_thread_resize(
        &mut self,
        event: &MouseDownEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.thread_resize_drag = Some(ThreadResizeDrag {
            anchor_x: f32::from(event.position.x),
            starting_width: self.models.thread_pane.width_px,
        });
        self.refresh(cx);
    }

    pub(crate) fn begin_sidebar_resize(
        &mut self,
        event: &MouseDownEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.sidebar_resize_drag = Some(SidebarResizeState {
            anchor_x: f32::from(event.position.x),
            starting_width: self.models.sidebar.width_px,
        });
        self.refresh(cx);
    }

    fn update_thread_resize_drag(
        &mut self,
        event: &MouseMoveEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        for view in self.selectable_texts.values() {
            let is_selecting = view.read(cx).is_selecting();
            if is_selecting {
                let position = event.position;
                view.update(cx, |text, cx| {
                    let index = text.index_for_mouse_position(position);
                    text.select_to(index, cx);
                });
                return;
            }
        }
    }

    fn update_thread_resize_drag_on_drag(
        &mut self,
        event: &gpui::DragMoveEvent<RightPaneResizeDrag>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(drag) = self.thread_resize_drag.as_ref() else {
            return;
        };

        let next_width = drag.starting_width + (drag.anchor_x - f32::from(event.event.position.x));
        self.models.set_thread_width(next_width);
        self.refresh(cx);
    }

    fn update_sidebar_resize_drag_on_drag(
        &mut self,
        event: &gpui::DragMoveEvent<SidebarResizeDrag>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(drag) = self.sidebar_resize_drag.as_ref() else {
            return;
        };

        // Sidebar handle sits on the right edge of the sidebar: drag right = wider.
        let next_width = drag.starting_width + (f32::from(event.event.position.x) - drag.anchor_x);
        self.models.set_sidebar_width(next_width);
        self.refresh(cx);
    }

    fn finish_thread_resize_drag(
        &mut self,
        _: &MouseUpEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let mut changed = false;
        if self.thread_resize_drag.take().is_some() {
            changed = true;
        }
        if self.sidebar_resize_drag.take().is_some() {
            changed = true;
        }
        if changed {
            self.refresh(cx);
        }
    }

    pub(crate) fn composer_remove_attachment(&mut self, index: usize, cx: &mut Context<Self>) {
        self.models.remove_composer_attachment(index);
        self.refresh(cx);
    }

    pub(crate) fn thread_remove_attachment(&mut self, index: usize, cx: &mut Context<Self>) {
        self.models.remove_thread_attachment(index);
        self.refresh(cx);
    }

    pub(crate) fn open_composer_file_upload_picker(&mut self, cx: &mut Context<Self>) {
        self.open_file_upload_lightbox_via_picker(UploadTarget::Composer, cx);
    }

    pub(crate) fn open_thread_file_upload_picker(&mut self, cx: &mut Context<Self>) {
        self.open_file_upload_lightbox_via_picker(UploadTarget::Thread, cx);
    }

    pub(crate) fn open_composer_file_upload_with_paths(
        &mut self,
        paths: Vec<PathBuf>,
        cx: &mut Context<Self>,
    ) {
        self.open_file_upload_lightbox_with_paths(paths, UploadTarget::Composer, cx);
    }

    pub(crate) fn open_thread_file_upload_with_paths(
        &mut self,
        paths: Vec<PathBuf>,
        cx: &mut Context<Self>,
    ) {
        self.open_file_upload_lightbox_with_paths(paths, UploadTarget::Thread, cx);
    }

    fn open_file_upload_lightbox_via_picker(
        &mut self,
        target: UploadTarget,
        cx: &mut Context<Self>,
    ) {
        let picker = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: true,
            prompt: None,
        });
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut async_app = cx.clone();
            async move {
                let Ok(result) = picker.await else {
                    return;
                };
                let Ok(paths) = result else {
                    return;
                };
                let Some(paths) = paths else {
                    return;
                };
                if paths.is_empty() {
                    return;
                }
                let _ = this.update(&mut async_app, move |this, cx| {
                    this.open_file_upload_lightbox_with_paths(paths, target, cx);
                });
            }
        })
        .detach();
    }

    fn open_file_upload_lightbox_with_paths(
        &mut self,
        paths: Vec<PathBuf>,
        target: UploadTarget,
        cx: &mut Context<Self>,
    ) {
        self.clear_hovered_message_immediate(cx);
        if !self.models.open_file_upload_lightbox(paths, target) {
            return;
        }
        self.pending_file_upload_caption_focus = true;
        self.sync_inputs_from_models(cx);
        self.refresh(cx);
    }

    pub(crate) fn file_upload_next(&mut self, cx: &mut Context<Self>) {
        if self.models.file_upload_next() {
            self.sync_inputs_from_models(cx);
            self.refresh(cx);
        }
    }

    fn file_upload_draft_key(&self, target: UploadTarget) -> Option<DraftKey> {
        match target {
            UploadTarget::Composer => Some(DraftKey::Conversation(
                self.models.composer.conversation_id.clone(),
            )),
            UploadTarget::Thread => self
                .models
                .thread_pane
                .root_message_id
                .clone()
                .map(DraftKey::Thread),
        }
    }

    pub(crate) fn file_upload_send_current(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let payload = self
            .models
            .overlay
            .file_upload_lightbox
            .as_ref()
            .and_then(|lightbox| {
                lightbox.current_candidate().map(|candidate| {
                    (
                        lightbox.target,
                        candidate.path.to_string_lossy().to_string(),
                        candidate.filename.clone(),
                        candidate.caption.clone(),
                    )
                })
            });
        let target = payload.as_ref().map(|(target, _, _, _)| *target);
        if !self.models.file_upload_send_current() {
            return;
        }
        if let Some((target, local_path, filename, caption)) = payload
            && let Some(key) = self.file_upload_draft_key(target)
        {
            self.dispatch_ui_action(UiAction::SendAttachment {
                key,
                local_path,
                filename,
                caption,
            });
        }
        self.sync_inputs_from_models(cx);
        self.refresh(cx);
        if let Some(target) = target {
            match target {
                UploadTarget::Composer => window.focus(&self.composer_input.focus_handle(cx)),
                UploadTarget::Thread => window.focus(&self.thread_input.focus_handle(cx)),
            }
        }
    }

    pub(crate) fn file_upload_send_all(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let payload = self
            .models
            .overlay
            .file_upload_lightbox
            .as_ref()
            .map(|lightbox| {
                let items = lightbox
                    .candidates
                    .iter()
                    .map(|candidate| {
                        (
                            candidate.path.to_string_lossy().to_string(),
                            candidate.filename.clone(),
                            candidate.caption.clone(),
                        )
                    })
                    .collect::<Vec<_>>();
                (lightbox.target, items)
            });
        let target = payload.as_ref().map(|(target, _)| *target);
        if !self.models.file_upload_send_all() {
            return;
        }
        if let Some((target, items)) = payload
            && let Some(key) = self.file_upload_draft_key(target)
        {
            for (local_path, filename, caption) in items {
                self.dispatch_ui_action(UiAction::SendAttachment {
                    key: key.clone(),
                    local_path,
                    filename,
                    caption,
                });
            }
        }
        self.sync_inputs_from_models(cx);
        self.refresh(cx);
        if let Some(target) = target {
            match target {
                UploadTarget::Composer => window.focus(&self.composer_input.focus_handle(cx)),
                UploadTarget::Thread => window.focus(&self.thread_input.focus_handle(cx)),
            }
        }
    }

    pub(crate) fn file_upload_cancel(&mut self, cx: &mut Context<Self>) {
        if self.models.cancel_file_upload_lightbox() {
            self.sync_inputs_from_models(cx);
            self.refresh(cx);
        }
    }

    pub(crate) fn composer_insert_emoji(&mut self, emoji: &str, cx: &mut Context<Self>) {
        if self.apply_picker_reaction_if_target(emoji.trim(), cx) {
            return;
        }
        self.composer_input
            .update(cx, |input, cx| input.insert_text(emoji, cx));
        self.models.set_composer_autocomplete(None);
        self.models.dismiss_overlays();
        self.refresh(cx);
    }

    pub(crate) fn thread_insert_emoji(&mut self, emoji: &str, cx: &mut Context<Self>) {
        if self.apply_picker_reaction_if_target(emoji.trim(), cx) {
            return;
        }
        self.thread_input
            .update(cx, |input, cx| input.insert_text(emoji, cx));
        self.models.set_thread_autocomplete(None);
        self.models.dismiss_overlays();
        self.refresh(cx);
    }

    fn emoji_picker_insert_text(&self, item: &EmojiPickerItem) -> (String, String) {
        match item {
            EmojiPickerItem::Stock(emoji) => {
                let selected =
                    selected_emoji_for_tone(emoji, self.models.emoji_picker.selected_skin_tone);
                (
                    format!("{} ", selected.as_str()),
                    recent_key_for_stock(emoji),
                )
            }
            EmojiPickerItem::Custom { alias, .. } => {
                (format!(":{alias}: "), alias.to_ascii_lowercase())
            }
        }
    }

    pub(crate) fn composer_insert_emoji_item(
        &mut self,
        item: EmojiPickerItem,
        cx: &mut Context<Self>,
    ) {
        let (emoji_text, recent_alias) = self.emoji_picker_insert_text(&item);
        self.models.add_recent_emoji_alias(recent_alias);
        self.persist_settings();
        self.composer_insert_emoji(&emoji_text, cx);
    }

    pub(crate) fn thread_insert_emoji_item(
        &mut self,
        item: EmojiPickerItem,
        cx: &mut Context<Self>,
    ) {
        let (emoji_text, recent_alias) = self.emoji_picker_insert_text(&item);
        self.models.add_recent_emoji_alias(recent_alias);
        self.persist_settings();
        self.thread_insert_emoji(&emoji_text, cx);
    }

    pub(crate) fn emoji_picker_pick_item(
        &mut self,
        item: EmojiPickerItem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(message_id) = self.models.overlay.reaction_target_message_id.clone() {
            let (emoji_text, recent_alias) = self.emoji_picker_insert_text(&item);
            let emoji = emoji_text.trim().to_string();
            self.models.add_recent_emoji_alias(recent_alias);
            self.persist_settings();
            self.models.overlay.emoji_picker_open = false;
            self.models.overlay.reaction_target_message_id = None;
            self.quick_react(message_id, emoji, cx);
            return;
        }
        let thread_focused = self.thread_input.focus_handle(cx).is_focused(window);
        let composer_focused = self.composer_input.focus_handle(cx).is_focused(window);
        if thread_focused
            || (!composer_focused
                && self.models.navigation.right_pane == RightPaneMode::Thread
                && self.models.thread_pane.open)
        {
            self.thread_insert_emoji_item(item, cx);
            return;
        }
        self.composer_insert_emoji_item(item, cx);
    }

    pub(crate) fn composer_insert_mention(&mut self, cx: &mut Context<Self>) {
        self.composer_input
            .update(cx, |input, cx| input.insert_text("@Alice ", cx));
        self.models
            .set_composer_autocomplete(Some(AutocompleteState {
                trigger: '@',
                query: "alice".to_string(),
                trigger_offset: 0,
                selected_index: 0,
                candidates: Vec::new(),
            }));
        self.refresh(cx);
    }

    pub(crate) fn thread_insert_mention(&mut self, cx: &mut Context<Self>) {
        self.thread_input
            .update(cx, |input, cx| input.insert_text("@Sam ", cx));
        self.models.set_thread_autocomplete(Some(AutocompleteState {
            trigger: '@',
            query: "sam".to_string(),
            trigger_offset: 0,
            selected_index: 0,
            candidates: Vec::new(),
        }));
        self.refresh(cx);
    }

    pub(crate) fn composer_insert_formatting(&mut self, cx: &mut Context<Self>) {
        self.composer_input
            .update(cx, |input, cx| input.insert_text("*bold* ", cx));
        self.refresh(cx);
    }

    pub(crate) fn composer_insert_link(&mut self, cx: &mut Context<Self>) {
        self.composer_input.update(cx, |input, cx| {
            input.insert_text("https://docs.acme.dev/gpui ", cx)
        });
        self.refresh(cx);
    }

    pub(crate) fn toggle_emoji_picker(&mut self, cx: &mut Context<Self>) {
        self.models.toggle_emoji_picker();
        self.sync_inputs_from_models(cx);
        self.refresh(cx);
    }

    pub(crate) fn close_emoji_picker_click(
        &mut self,
        _: &ClickEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dismiss_overlays(cx);
    }

    pub(crate) fn toggle_emoji_picker_skin_tone_expanded_click(
        &mut self,
        _: &ClickEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let expanded = !self.models.emoji_picker.skin_tone_expanded;
        self.models.set_emoji_picker_skin_tone_expanded(expanded);
        self.refresh(cx);
    }

    pub(crate) fn set_emoji_picker_skin_tone(
        &mut self,
        tone: Option<emojis::SkinTone>,
        cx: &mut Context<Self>,
    ) {
        self.models.set_emoji_picker_skin_tone(tone);
        self.models.set_emoji_picker_skin_tone_expanded(false);
        self.persist_settings();
        self.refresh(cx);
    }

    pub(crate) fn set_emoji_picker_active_group(
        &mut self,
        group: Option<emojis::Group>,
        cx: &mut Context<Self>,
    ) {
        self.models.set_emoji_picker_active_group(group);
        self.refresh(cx);
    }

    pub(crate) fn set_emoji_picker_hovered(
        &mut self,
        hovered: Option<EmojiPickerItem>,
        cx: &mut Context<Self>,
    ) {
        let current_key = self
            .models
            .emoji_picker
            .hovered
            .as_ref()
            .map(EmojiPickerItem::key);
        let next_key = hovered.as_ref().map(EmojiPickerItem::key);
        if current_key == next_key {
            return;
        }
        self.models.set_emoji_picker_hovered(hovered);
        self.refresh(cx);
    }

    pub(crate) fn open_attachment_modal(&mut self, label: &str, cx: &mut Context<Self>) {
        self.models.open_attachment_modal(label);
        self.refresh(cx);
    }

    pub(crate) fn open_image_lightbox(
        &mut self,
        source: AttachmentSource,
        caption: Option<String>,
        width: Option<u32>,
        height: Option<u32>,
        cx: &mut Context<Self>,
    ) {
        self.clear_hovered_message_immediate(cx);
        self.models
            .open_image_lightbox(source, caption, width, height);
        self.refresh(cx);
    }

    pub(crate) fn open_video_in_native_player(path: &str) {
        let _ = std::process::Command::new("open").arg(path).spawn();
    }

    pub(crate) fn save_attachment_copy(
        &mut self,
        source_path: &str,
        suggested_name: &str,
        cx: &mut Context<Self>,
    ) {
        let normalized = crate::views::normalize_local_source_path(source_path);
        let source = PathBuf::from(normalized);
        if !source.exists() {
            self.models.push_toast("Attachment file is missing", None);
            self.refresh(cx);
            return;
        }
        let Some(home) = std::env::var_os("HOME") else {
            self.models
                .push_toast("Unable to resolve home directory", None);
            self.refresh(cx);
            return;
        };
        let downloads_dir = PathBuf::from(home).join("Downloads");
        if let Err(err) = std::fs::create_dir_all(&downloads_dir) {
            self.models
                .push_toast(format!("Failed to prepare Downloads: {err}"), None);
            self.refresh(cx);
            return;
        }
        let source_name = source
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("attachment");
        let preferred = if suggested_name.trim().is_empty() {
            source_name
        } else {
            suggested_name
        };
        let destination = unique_copy_destination(&downloads_dir, preferred);
        match std::fs::copy(&source, &destination) {
            Ok(_) => {
                let _ = std::process::Command::new("open").arg(&destination).spawn();
                self.models.push_toast(
                    format!(
                        "Saved to {}",
                        destination
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or("Downloads")
                    ),
                    None,
                );
            }
            Err(err) => {
                self.models
                    .push_toast(format!("Failed to save attachment: {err}"), None);
            }
        }
        self.refresh(cx);
    }

    pub(crate) fn download_attachment_and_open(
        &mut self,
        url: &str,
        suggested_name: &str,
        cx: &mut Context<Self>,
    ) {
        let Some(home) = std::env::var_os("HOME") else {
            self.models
                .push_toast("Unable to resolve home directory", None);
            self.refresh(cx);
            return;
        };
        let downloads_dir = PathBuf::from(home).join("Downloads");
        if let Err(err) = std::fs::create_dir_all(&downloads_dir) {
            self.models
                .push_toast(format!("Failed to prepare Downloads: {err}"), None);
            self.refresh(cx);
            return;
        }
        let preferred = if suggested_name.trim().is_empty() {
            "attachment"
        } else {
            suggested_name
        };
        let destination = unique_copy_destination(&downloads_dir, preferred);
        let status = std::process::Command::new("curl")
            .arg("-L")
            .arg("--fail")
            .arg("--silent")
            .arg("--show-error")
            .arg("--output")
            .arg(&destination)
            .arg(url)
            .status();
        match status {
            Ok(status) if status.success() => {
                let _ = std::process::Command::new("open").arg(&destination).spawn();
                self.models.push_toast(
                    format!(
                        "Saved to {}",
                        destination
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or("Downloads")
                    ),
                    None,
                );
            }
            Ok(_) | Err(_) => {
                self.models
                    .push_toast("Direct download failed, opening source instead", None);
                cx.open_url(url);
            }
        }
        self.refresh(cx);
    }

    pub(crate) fn notify_attachment_not_ready(&mut self, cx: &mut Context<Self>) {
        self.models
            .push_toast("File is not available yet. Try again in a moment.", None);
        self.refresh(cx);
    }

    const HOVER_SETTLE_DELAY: Duration = Duration::from_millis(200);
    /// Delay before hiding the quick reaction strip after the mouse leaves the row,
    /// so the user can move to the strip (which may be outside the row) without it disappearing.
    const HOVER_CLEAR_DELAY: Duration = Duration::from_millis(300);

    pub(crate) fn set_hovered_message(&mut self, message_id: MessageId, cx: &mut Context<Self>) {
        if self.timeline_interactions_blocked_by_overlay() {
            self.clear_hovered_message_immediate(cx);
            return;
        }
        self.cancel_hover_clear();
        let mut needs_refresh = false;
        if self.models.overlay.sidebar_hover_tooltip.is_some() {
            self.models.hide_sidebar_hover_tooltip();
            needs_refresh = true;
        }
        let message_changed = self.models.timeline.hovered_message_id.as_ref() != Some(&message_id);
        if message_changed && self.models.overlay.reaction_hover_tooltip.is_some() {
            self.models.hide_reaction_hover_tooltip();
            needs_refresh = true;
        }
        if message_changed {
            self.models.timeline.hovered_message_id = Some(message_id);
            self.models.timeline.hovered_message_is_thread = None;
            self.models.timeline.hovered_message_anchor_x = None;
            self.models.timeline.hovered_message_anchor_y = None;
            self.models.timeline.hovered_message_window_left = None;
            self.models.timeline.hovered_message_window_top = None;
            self.models.timeline.hovered_message_window_width = None;
            self.models.timeline.hover_toolbar_settled = false;
            needs_refresh = true;
        }
        if needs_refresh {
            self.refresh(cx);
        }
    }

    pub(crate) fn set_hovered_message_with_cursor_anchor(
        &mut self,
        message_id: MessageId,
        cursor_x: f32,
        cursor_y: f32,
        is_thread: bool,
        cx: &mut Context<Self>,
    ) {
        if self.timeline_interactions_blocked_by_overlay() {
            self.clear_hovered_message_immediate(cx);
            return;
        }
        self.cancel_hover_clear();
        let mut needs_refresh = false;
        if self.models.overlay.sidebar_hover_tooltip.is_some() {
            self.models.hide_sidebar_hover_tooltip();
            needs_refresh = true;
        }
        let message_changed = self.models.timeline.hovered_message_id.as_ref() != Some(&message_id)
            || self.models.timeline.hovered_message_is_thread != Some(is_thread);
        if message_changed && self.models.overlay.reaction_hover_tooltip.is_some() {
            self.models.hide_reaction_hover_tooltip();
            needs_refresh = true;
        }

        if message_changed {
            self.models.timeline.hovered_message_id = Some(message_id.clone());
            self.models.timeline.hovered_message_is_thread = Some(is_thread);
            self.models.timeline.hovered_message_anchor_x = Some(cursor_x);
            self.models.timeline.hovered_message_anchor_y = Some(cursor_y);
            self.models.timeline.hovered_message_window_left = None;
            self.models.timeline.hovered_message_window_top = None;
            self.models.timeline.hovered_message_window_width = None;
            needs_refresh = true;
            // Require a new hover-settle delay when switching rows.
            self.models.timeline.hover_toolbar_settled = false;
            self.schedule_hover_settle(cx);
        } else {
            // While waiting for the settle delay, keep updating the anchor and restart the timer.
            // Once the toolbar has appeared, freeze its position until the pointer leaves the row.
            if !self.models.timeline.hover_toolbar_settled {
                self.models.timeline.hovered_message_anchor_x = Some(cursor_x);
                self.models.timeline.hovered_message_anchor_y = Some(cursor_y);
                self.schedule_hover_settle(cx);
            }
        }
        if needs_refresh {
            self.refresh(cx);
        }
    }

    fn schedule_hover_settle(&mut self, cx: &mut Context<Self>) {
        self.hover_settle_seq = self.hover_settle_seq.wrapping_add(1);
        let seq = self.hover_settle_seq;
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let background = cx.background_executor().clone();
            let mut async_app = cx.clone();
            async move {
                background.timer(Self::HOVER_SETTLE_DELAY).await;
                let _ = this.update(&mut async_app, |this, cx| {
                    if this.hover_settle_seq == seq {
                        this.models.timeline.hover_toolbar_settled = true;
                        this.refresh(cx);
                    }
                });
            }
        })
        .detach();
    }

    pub(crate) fn record_hovered_message_layout(
        &mut self,
        message_id: MessageId,
        is_thread: bool,
        window_left: f32,
        window_top: f32,
        window_width: f32,
        cx: &mut Context<Self>,
    ) {
        if self.models.timeline.hovered_message_id.as_ref() != Some(&message_id)
            || self.models.timeline.hovered_message_is_thread != Some(is_thread)
        {
            return;
        }
        let needs_refresh = self
            .models
            .timeline
            .hovered_message_window_left
            .is_none_or(|x| (x - window_left).abs() > 0.5)
            || self
                .models
                .timeline
                .hovered_message_window_top
                .is_none_or(|y| (y - window_top).abs() > 0.5)
            || self
                .models
                .timeline
                .hovered_message_window_width
                .is_none_or(|w| (w - window_width).abs() > 0.5);
        if !needs_refresh {
            return;
        }
        self.models.timeline.hovered_message_window_left = Some(window_left);
        self.models.timeline.hovered_message_window_top = Some(window_top);
        self.models.timeline.hovered_message_window_width = Some(window_width);
        cx.notify();
    }

    pub(crate) fn cursor_over_selectable_link(
        &self,
        position: gpui::Point<gpui::Pixels>,
        cx: &Context<Self>,
    ) -> bool {
        self.selectable_texts
            .values()
            .any(|entity| entity.read(cx).is_position_over_link(position))
    }

    pub(crate) fn any_text_selected(&self, cx: &Context<Self>) -> bool {
        self.selectable_texts
            .values()
            .any(|entity| entity.read(cx).has_selection())
    }

    /// Schedules the quick reaction strip to hide after a short delay, so the user can
    /// move the mouse to the strip (which may be outside the row) without it disappearing.
    pub(crate) fn clear_hovered_message(&mut self, cx: &mut Context<Self>) {
        self.schedule_hover_clear(cx);
    }

    /// Cancels any pending delayed hide of the strip. Call when the mouse enters the row
    /// or the strip so the strip stays visible.
    pub(crate) fn cancel_hover_clear(&mut self) {
        self.hover_clear_seq = self.hover_clear_seq.wrapping_add(1);
        self.hover_clear_pending = false;
    }

    fn schedule_hover_clear(&mut self, cx: &mut Context<Self>) {
        if self.models.timeline.hovered_message_id.is_none() || self.hover_clear_pending {
            return;
        }
        self.hover_clear_pending = true;
        self.hover_clear_seq = self.hover_clear_seq.wrapping_add(1);
        let seq = self.hover_clear_seq;
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let background = cx.background_executor().clone();
            let mut async_app = cx.clone();
            async move {
                background.timer(Self::HOVER_CLEAR_DELAY).await;
                let _ = this.update(&mut async_app, |this, cx| {
                    this.hover_clear_pending = false;
                    if this.hover_clear_seq == seq {
                        this.clear_hovered_message_immediate(cx);
                    }
                });
            }
        })
        .detach();
    }

    /// Hides the strip immediately. Use for clicks and overlay open, not for mouse leave.
    pub(crate) fn clear_hovered_message_immediate(&mut self, cx: &mut Context<Self>) {
        self.cancel_hover_clear();
        let mut needs_refresh = false;
        if self.models.timeline.hovered_message_id.is_some() {
            self.models.timeline.hovered_message_id = None;
            self.models.timeline.hovered_message_is_thread = None;
            self.models.timeline.hovered_message_anchor_x = None;
            self.models.timeline.hovered_message_anchor_y = None;
            self.models.timeline.hovered_message_window_left = None;
            self.models.timeline.hovered_message_window_top = None;
            self.models.timeline.hovered_message_window_width = None;
            self.models.timeline.hover_toolbar_settled = false;
            self.hover_settle_seq = self.hover_settle_seq.wrapping_add(1);
            needs_refresh = true;
        }
        if self.models.overlay.reaction_hover_tooltip.is_some() {
            self.models.hide_reaction_hover_tooltip();
            needs_refresh = true;
        }
        if needs_refresh {
            self.refresh(cx);
        }
    }

    pub(crate) fn show_sidebar_hover_tooltip(
        &mut self,
        text: String,
        anchor_x: f32,
        anchor_y: f32,
        cx: &mut Context<Self>,
    ) {
        const TOOLTIP_MIN_WIDTH_PX: f32 = 140.0;
        const TOOLTIP_MAX_WIDTH_PX: f32 = 220.0;
        let next_x = anchor_x + 10.0;
        let next_y = anchor_y + 10.0;
        let width_px = estimate_tooltip_width_px(&text, TOOLTIP_MIN_WIDTH_PX, TOOLTIP_MAX_WIDTH_PX);
        let mut needs_refresh = match self.models.overlay.sidebar_hover_tooltip.as_ref() {
            Some(current) => {
                current.text != text
                    || (current.anchor_x - next_x).abs() > 0.5
                    || (current.anchor_y - next_y).abs() > 0.5
                    || (current.width_px - width_px).abs() > 0.5
            }
            None => true,
        };
        if self.models.overlay.reaction_hover_tooltip.is_some() {
            self.models.hide_reaction_hover_tooltip();
            needs_refresh = true;
        }
        if !needs_refresh {
            return;
        }
        self.models
            .show_sidebar_hover_tooltip(text, next_x, next_y, width_px);
        self.refresh(cx);
    }

    pub(crate) fn clear_sidebar_hover_tooltip(&mut self, cx: &mut Context<Self>) {
        if self.models.overlay.sidebar_hover_tooltip.is_none() {
            return;
        }
        self.models.hide_sidebar_hover_tooltip();
        self.refresh(cx);
    }

    pub(crate) fn show_reaction_hover_tooltip(
        &mut self,
        text: String,
        anchor_x: f32,
        anchor_y: f32,
        cx: &mut Context<Self>,
    ) {
        const TOOLTIP_MIN_WIDTH_PX: f32 = 190.0;
        const TOOLTIP_MAX_WIDTH_PX: f32 = 520.0;
        let next_x = anchor_x + 8.0;
        let next_y = anchor_y - 28.0;
        let width_px = estimate_tooltip_width_px(&text, TOOLTIP_MIN_WIDTH_PX, TOOLTIP_MAX_WIDTH_PX);
        let mut needs_refresh = match self.models.overlay.reaction_hover_tooltip.as_ref() {
            Some(current) => {
                current.text != text
                    || (current.anchor_x - next_x).abs() > 0.5
                    || (current.anchor_y - next_y).abs() > 0.5
                    || (current.width_px - width_px).abs() > 0.5
            }
            None => true,
        };
        if self.models.overlay.sidebar_hover_tooltip.is_some() {
            self.models.hide_sidebar_hover_tooltip();
            needs_refresh = true;
        }
        if !needs_refresh {
            return;
        }
        self.models
            .show_reaction_hover_tooltip(text, next_x, next_y, width_px);
        self.refresh(cx);
    }

    pub(crate) fn clear_reaction_hover_tooltip(&mut self, cx: &mut Context<Self>) {
        if self.models.overlay.reaction_hover_tooltip.is_none() {
            return;
        }
        self.models.hide_reaction_hover_tooltip();
        self.refresh(cx);
    }

    pub(crate) fn react_to_message(&mut self, message_id: MessageId, cx: &mut Context<Self>) {
        self.models.open_reaction_picker_for_message(&message_id);
        self.sync_inputs_from_models(cx);
        self.refresh(cx);
    }

    pub(crate) fn quick_react(
        &mut self,
        message_id: MessageId,
        emoji: String,
        cx: &mut Context<Self>,
    ) {
        let Some(conversation_id) = self.app_store.snapshot().timeline.conversation_id.clone()
        else {
            return;
        };
        self.ensure_message_binding_for_active_conversation(&message_id);
        self.dispatch_ui_action(UiAction::ReactToMessage {
            conversation_id,
            message_id: message_id.clone(),
            emoji: emoji.clone(),
        });
        self.models.add_recent_emoji_alias(emoji);
        self.persist_settings();
        self.refresh(cx);
    }

    pub(crate) fn retry_failed_message_send(
        &mut self,
        message_id: MessageId,
        cx: &mut Context<Self>,
    ) {
        let retry_text = self.models.timeline.rows.iter().find_map(|row| match row {
            crate::models::timeline_model::TimelineRow::Message(message_row)
                if message_row.message.id == message_id =>
            {
                let text = message_row
                    .message
                    .fragments
                    .iter()
                    .filter_map(|fragment| match fragment {
                        MessageFragment::Text(value)
                        | MessageFragment::InlineCode(value)
                        | MessageFragment::Code { text: value, .. }
                        | MessageFragment::Quote(value) => Some(value.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                Some(text.trim().to_string())
            }
            _ => None,
        });
        let Some(retry_text) = retry_text.filter(|text| !text.is_empty()) else {
            return;
        };
        let draft_key = DraftKey::Conversation(self.models.composer.conversation_id.clone());
        self.models.composer.mode = ComposerMode::Compose;
        self.models.composer.draft_text = retry_text.clone();
        self.composer_input
            .update(cx, |input, cx| input.set_text(retry_text.clone(), cx));
        self.dispatch_ui_action(UiAction::UpdateDraft {
            key: draft_key.clone(),
            text: retry_text,
        });
        self.dispatch_ui_action(UiAction::SendMessage { key: draft_key });
        let (dispatch, pending) = self.models.send_composer_message();
        if matches!(dispatch, SendDispatch::NotSent) {
            return;
        }
        self.sync_inputs_from_models(cx);
        if let Some(pending) = pending {
            self.schedule_pending_send(pending, cx);
        }
        self.refresh(cx);
    }

    fn apply_picker_reaction_if_target(&mut self, emoji: &str, cx: &mut Context<Self>) -> bool {
        if emoji.trim().is_empty() {
            return false;
        }
        let Some(message_id) = self.models.overlay.reaction_target_message_id.clone() else {
            return false;
        };
        let Some(conversation_id) = self.app_store.snapshot().timeline.conversation_id.clone()
        else {
            return false;
        };
        self.ensure_message_binding_for_active_conversation(&message_id);
        self.dispatch_ui_action(UiAction::ReactToMessage {
            conversation_id,
            message_id: message_id.clone(),
            emoji: emoji.trim().to_string(),
        });
        self.refresh(cx);
        true
    }

    pub(crate) fn edit_message(
        &mut self,
        message_id: MessageId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.models.edit_message(&message_id) {
            self.sync_inputs_from_models(cx);
            window.focus(&self.composer_input.focus_handle(cx));
        }
        self.refresh(cx);
    }

    fn edit_last_own_message(&mut self, cx: &mut Context<Self>) {
        let Some(message_id) = self.models.find_last_own_message_id() else {
            return;
        };
        if self.models.edit_message(&message_id) {
            self.sync_inputs_from_models(cx);
            self.refresh(cx);
        }
    }

    fn cancel_edit(&mut self, cx: &mut Context<Self>) {
        if self.models.cancel_edit() {
            self.sync_inputs_from_models(cx);
            self.refresh(cx);
        }
    }

    pub(crate) fn delete_message(&mut self, message_id: MessageId, cx: &mut Context<Self>) {
        self.ensure_message_binding_for_active_conversation(&message_id);
        if self.models.delete_message(&message_id) {
            if let Some(conversation_id) =
                self.app_store.snapshot().timeline.conversation_id.clone()
            {
                self.dispatch_ui_action(UiAction::DeleteMessage {
                    conversation_id,
                    message_id: message_id.clone(),
                });
            }
            self.sync_inputs_from_models(cx);
        }
        self.refresh(cx);
    }

    pub(crate) fn copy_message_link(&mut self, message_id: MessageId, cx: &mut Context<Self>) {
        let stored_link = self.models.timeline.rows.iter().find_map(|row| match row {
            crate::models::timeline_model::TimelineRow::Message(message_row)
                if message_row.message.id == message_id =>
            {
                Some(message_row.message.permalink.clone())
            }
            _ => None,
        });

        let link = stored_link
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| self.build_permalink_from_conversation(&message_id));

        cx.write_to_clipboard(ClipboardItem::new_string(link));
        self.models.push_toast(
            "Copied message link",
            Some(ToastAction::OpenCurrentConversation),
        );
        self.refresh(cx);
    }

    fn build_permalink_from_conversation(&self, message_id: &MessageId) -> String {
        let summary = &self.models.conversation.summary;
        match &summary.group {
            Some(group) => format!(
                "keybase://chat/{}#{}/{}",
                group.display_name, summary.topic, message_id.0
            ),
            None => format!("keybase://chat/{}/{}", summary.topic, message_id.0),
        }
    }

    pub(crate) fn copy_message_text(&mut self, message_id: MessageId, cx: &mut Context<Self>) {
        let text = self.models.timeline.rows.iter().find_map(|row| match row {
            crate::models::timeline_model::TimelineRow::Message(message_row)
                if message_row.message.id == message_id =>
            {
                message_row.message.source_text.clone()
            }
            _ => None,
        });

        if let Some(text) = text.filter(|s| !s.is_empty()) {
            cx.write_to_clipboard(ClipboardItem::new_string(text));
            self.models.push_toast("Copied message text", None);
        } else {
            self.models.push_toast("No text to copy", None);
        }
        self.refresh(cx);
    }

    pub(crate) fn open_files_pane(&mut self, cx: &mut Context<Self>) {
        self.models.set_right_pane(RightPaneMode::Files);
        self.refresh(cx);
    }

    pub(crate) fn open_search_pane(&mut self, cx: &mut Context<Self>) {
        self.models.set_right_pane(RightPaneMode::Search);
        self.refresh(cx);
    }

    pub(crate) fn start_or_open_call(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let route = self.models.start_or_open_call();
        self.navigate_to(route, window, cx);
    }

    pub(crate) fn leave_call(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let on_call_route = matches!(self.models.navigation.current, Route::ActiveCall { .. });
        self.models.leave_call();
        if on_call_route {
            self.navigate_to(self.current_conversation_route(), window, cx);
        } else {
            self.refresh(cx);
        }
    }

    pub(crate) fn toggle_call_mute(&mut self, cx: &mut Context<Self>) {
        self.models.toggle_call_mute();
        self.refresh(cx);
    }

    pub(crate) fn toggle_call_screen_share(&mut self, cx: &mut Context<Self>) {
        self.models.toggle_call_screen_share();
        self.refresh(cx);
    }

    pub(crate) fn cycle_call_status(&mut self, cx: &mut Context<Self>) {
        self.models.cycle_call_status();
        self.refresh(cx);
    }

    pub(crate) fn open_new_chat(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.models.dismiss_overlays();
        self.keybase_inspector.open = false;
        self.dispatch_ui_action(UiAction::OpenNewChat);
        self.models.overlay.new_chat_open = true;
        self.sync_inputs_from_models(cx);
        self.refresh(cx);
        window.focus(&self.new_chat_input.focus_handle(cx));
    }

    pub(crate) fn new_chat_add_participant(&mut self, user: UserSummary, cx: &mut Context<Self>) {
        self.dispatch_ui_action(UiAction::NewChatAddParticipant { user });
        self.refresh(cx);
    }

    pub(crate) fn new_chat_remove_participant(&mut self, user_id: UserId, cx: &mut Context<Self>) {
        self.dispatch_ui_action(UiAction::NewChatRemoveParticipant { user_id });
        self.refresh(cx);
    }

    pub(crate) fn new_chat_create(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.dispatch_ui_action(UiAction::NewChatCreate);
        self.sync_inputs_from_models(cx);
        self.refresh(cx);
        if self.models.overlay.new_chat_open {
            window.focus(&self.new_chat_input.focus_handle(cx));
        } else {
            window.focus(&self.composer_input.focus_handle(cx));
        }
    }

    pub(crate) fn toggle_quick_switcher(&mut self, cx: &mut Context<Self>) {
        if self.models.overlay.new_chat_open || self.models.new_chat.open {
            self.dispatch_ui_action(UiAction::CloseNewChat);
        }
        self.models.toggle_quick_switcher();
        if self.models.overlay.quick_switcher_open {
            self.clear_hovered_message_immediate(cx);
        }
        self.quick_switcher_query_seq = self.quick_switcher_query_seq.wrapping_add(1);
        self.sync_inputs_from_models(cx);
        self.refresh(cx);
    }

    pub(crate) fn toggle_find_in_chat(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.models.find_in_chat.open {
            self.close_find_in_chat(window, cx);
            return;
        }
        if !matches!(
            self.models.navigation.current,
            Route::Channel { .. } | Route::DirectMessage { .. }
        ) {
            return;
        }
        let conversation_id = self.models.conversation.summary.id.clone();
        let anchor_timestamp_ms = self.find_in_chat_anchor_timestamp();
        self.models
            .find_in_chat
            .open_for_conversation(conversation_id, anchor_timestamp_ms);
        self.sync_inputs_from_models(cx);
        self.refresh(cx);
        window.focus(&self.find_in_chat_input.focus_handle(cx));
    }

    pub(crate) fn close_find_in_chat(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.models.find_in_chat.open {
            return;
        }
        self.models.find_in_chat.close();
        self.sync_inputs_from_models(cx);
        self.refresh(cx);
        window.focus(&self.composer_input.focus_handle(cx));
    }

    pub(crate) fn close_find_in_chat_click(
        &mut self,
        _: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.close_find_in_chat(window, cx);
    }

    pub(crate) fn find_next_match(&mut self, cx: &mut Context<Self>) {
        let Some(result) = self.models.find_in_chat.select_next() else {
            self.refresh(cx);
            return;
        };
        self.jump_to_find_match(result.message.id, cx);
    }

    pub(crate) fn find_previous_match(&mut self, cx: &mut Context<Self>) {
        let Some(result) = self.models.find_in_chat.select_previous() else {
            self.refresh(cx);
            return;
        };
        self.jump_to_find_match(result.message.id, cx);
    }

    pub(crate) fn toggle_command_palette(&mut self, cx: &mut Context<Self>) {
        if self.models.overlay.new_chat_open || self.models.new_chat.open {
            self.dispatch_ui_action(UiAction::CloseNewChat);
        }
        self.models.toggle_command_palette();
        if self.models.overlay.command_palette_open {
            self.clear_hovered_message_immediate(cx);
        }
        self.refresh(cx);
    }

    pub(crate) fn toggle_keybase_inspector(&mut self, cx: &mut Context<Self>) {
        self.keybase_inspector.open = !self.keybase_inspector.open;
        if self.keybase_inspector.open {
            if self.models.overlay.new_chat_open || self.models.new_chat.open {
                self.dispatch_ui_action(UiAction::CloseNewChat);
            }
            self.models.dismiss_overlays();
        }
        self.refresh(cx);
    }

    pub(crate) fn toggle_splash_screen(&mut self, cx: &mut Context<Self>) {
        self.splash_open = !self.splash_open;
        if self.splash_open {
            self.keybase_inspector.open = false;
            self.models.dismiss_overlays();
            self.clear_hovered_message_immediate(cx);
        }
        self.refresh(cx);
    }

    const SPLASH_MIN_DURATION: Duration = Duration::from_secs(2);

    fn check_splash_dismiss(&mut self, cx: &mut Context<Self>) {
        if !self.splash_open || self.splash_boot_ready {
            return;
        }
        let boot_phase = self.app_store.snapshot().app.boot_phase.clone();
        if !matches!(boot_phase, BootPhase::Ready) {
            return;
        }
        self.splash_boot_ready = true;
        let elapsed = self.splash_shown_at.elapsed();
        if elapsed >= Self::SPLASH_MIN_DURATION {
            self.splash_open = false;
        } else {
            let remaining = Self::SPLASH_MIN_DURATION - elapsed;
            cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                let background = cx.background_executor().clone();
                let mut async_app = cx.clone();
                async move {
                    background.timer(remaining).await;
                    let _ = this.update(&mut async_app, |this, cx| {
                        this.splash_open = false;
                        this.refresh(cx);
                    });
                }
            })
            .detach();
        }
    }

    fn toggle_keybase_inspector_pause(
        &mut self,
        _: &ClickEvent,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.keybase_inspector.paused = !self.keybase_inspector.paused;
        self.refresh(cx);
    }

    fn toggle_keybase_inspector_unknown_only(
        &mut self,
        _: &ClickEvent,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.keybase_inspector.unknown_only = !self.keybase_inspector.unknown_only;
        self.refresh(cx);
    }

    fn clear_keybase_inspector(&mut self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.keybase_inspector.entries.clear();
        self.keybase_inspector.seq = 0;
        self.refresh(cx);
    }

    fn copy_keybase_inspector_rows(
        &mut self,
        _: &ClickEvent,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let rows = self.keybase_inspector_entries();
        let export = self.format_keybase_inspector_export(&rows);
        cx.write_to_clipboard(ClipboardItem::new_string(export));
        self.models.push_toast(
            format!("Copied {} inspector rows", rows.len()),
            Some(ToastAction::OpenCurrentConversation),
        );
        self.refresh(cx);
    }

    fn close_keybase_inspector(&mut self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.keybase_inspector.open = false;
        self.refresh(cx);
    }

    fn find_in_chat_anchor_timestamp(&self) -> Option<i64> {
        if let Some(message_id) = self.models.timeline.highlighted_message_id.as_ref()
            && let Some(timestamp_ms) =
                timeline_message_timestamp_by_id(&self.models.timeline.rows, message_id)
        {
            return Some(timestamp_ms);
        }
        if let Some(message_id) = self.first_visible_timeline_message_id()
            && let Some(timestamp_ms) =
                timeline_message_timestamp_by_id(&self.models.timeline.rows, &message_id)
        {
            return Some(timestamp_ms);
        }
        latest_timeline_message_timestamp(&self.models.timeline.rows)
    }

    fn jump_to_find_match(&mut self, message_id: MessageId, cx: &mut Context<Self>) {
        self.models.timeline.pending_scroll_target = Some(message_id.clone());
        self.models.timeline.highlighted_message_id = Some(message_id.clone());
        self.apply_pending_timeline_scroll_target();
        if self.models.timeline.pending_scroll_target.is_some()
            && let Some(conversation_id) =
                self.app_store.snapshot().timeline.conversation_id.clone()
        {
            self.dispatch_ui_action(UiAction::JumpToMessage {
                conversation_id,
                message_id,
            });
        }
        self.refresh(cx);
    }

    fn consume_click(&mut self, _: &ClickEvent, _: &mut Window, _: &mut Context<Self>) {}

    fn schedule_quick_switcher_corpus_rebuild(&mut self, cx: &mut Context<Self>) {
        self.quick_switcher_corpus_rebuild_seq =
            self.quick_switcher_corpus_rebuild_seq.wrapping_add(1);
        let scheduled_seq = self.quick_switcher_corpus_rebuild_seq;
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let background = cx.background_executor().clone();
            let mut async_app = cx.clone();
            async move {
                background
                    .timer(QUICK_SWITCHER_CORPUS_REBUILD_COALESCE)
                    .await;
                let _ = this.update(&mut async_app, move |this, cx| {
                    if this.quick_switcher_corpus_rebuild_seq != scheduled_seq {
                        return;
                    }
                    if !this
                        .models
                        .flush_quick_switcher_local_search_corpus_if_dirty()
                    {
                        return;
                    }
                    this.quick_switcher_last_local_query.clear();
                    this.quick_switcher_last_local_matched_entry_indices = Arc::new(Vec::new());
                    this.quick_switcher_last_local_corpus_revision = 0;
                    if this.models.overlay.quick_switcher_open
                        && this.models.quick_switcher.query.trim().is_empty()
                    {
                        this.models.update_quick_switcher_query(String::new());
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn update_quick_switcher_indexing_from_internal(
        &mut self,
        method: &str,
        payload_preview: Option<&str>,
    ) -> bool {
        let before = (
            self.quick_switcher_indexing_active,
            self.quick_switcher_indexing_total_conversations,
            self.quick_switcher_indexing_completed_conversations,
            self.quick_switcher_indexing_messages_indexed,
        );
        match method {
            "zbase.internal.crawl.start" => {
                self.quick_switcher_indexing_active = true;
                self.quick_switcher_indexing_total_conversations =
                    extract_u64_from_internal_payload(payload_preview, "conversation_count");
                self.quick_switcher_indexing_completed_conversations = 0;
                self.quick_switcher_indexing_messages_indexed = 0;
            }
            "zbase.internal.crawl.conversation_progress" => {
                self.quick_switcher_indexing_active = true;
                if let Some(indexed_messages) =
                    extract_u64_from_internal_payload(payload_preview, "messages_indexed")
                {
                    self.quick_switcher_indexing_messages_indexed = indexed_messages;
                }
            }
            "zbase.internal.crawl.conversation_complete" => {
                self.quick_switcher_indexing_active = true;
                self.quick_switcher_indexing_completed_conversations = self
                    .quick_switcher_indexing_completed_conversations
                    .saturating_add(1);
                if let Some(indexed_messages) =
                    extract_u64_from_internal_payload(payload_preview, "messages_indexed")
                {
                    self.quick_switcher_indexing_messages_indexed = indexed_messages;
                }
            }
            "zbase.internal.crawl.finished" => {
                if let Some(completed) =
                    extract_u64_from_internal_payload(payload_preview, "completed_conversations")
                {
                    self.quick_switcher_indexing_completed_conversations = completed;
                }
                if let Some(total) =
                    extract_u64_from_internal_payload(payload_preview, "conversation_count")
                {
                    self.quick_switcher_indexing_total_conversations = Some(total);
                }
                if let Some(indexed_messages) =
                    extract_u64_from_internal_payload(payload_preview, "indexed_messages")
                {
                    self.quick_switcher_indexing_messages_indexed = indexed_messages;
                }
                self.quick_switcher_indexing_active = false;
            }
            _ => {}
        }
        (
            self.quick_switcher_indexing_active,
            self.quick_switcher_indexing_total_conversations,
            self.quick_switcher_indexing_completed_conversations,
            self.quick_switcher_indexing_messages_indexed,
        ) != before
    }

    fn quick_switcher_indexing_status_text(&self) -> Option<String> {
        if !self.quick_switcher_indexing_active {
            return None;
        }
        let conversation_progress = self
            .quick_switcher_indexing_total_conversations
            .map(|total| {
                format!(
                    "{} / {} conversations",
                    self.quick_switcher_indexing_completed_conversations
                        .min(total),
                    total
                )
            })
            .unwrap_or_else(|| {
                format!(
                    "{} conversations",
                    self.quick_switcher_indexing_completed_conversations
                )
            });
        if self.quick_switcher_indexing_messages_indexed > 0 {
            Some(format!(
                "Indexing older messages: {} indexed ({conversation_progress})",
                self.quick_switcher_indexing_messages_indexed
            ))
        } else {
            Some(format!(
                "Indexing older messages in background ({conversation_progress})"
            ))
        }
    }

    pub(crate) fn consume_scroll_wheel(
        &mut self,
        _: &ScrollWheelEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        window.prevent_default();
        cx.stop_propagation();
    }

    pub(crate) fn consume_mouse_move(
        &mut self,
        _: &MouseMoveEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        cx.stop_propagation();
    }

    pub(crate) fn dismiss_overlays(&mut self, cx: &mut Context<Self>) {
        if self.models.overlay.new_chat_open || self.models.new_chat.open {
            self.dispatch_ui_action(UiAction::CloseNewChat);
        }
        self.models.dismiss_overlays();
        self.keybase_inspector.open = false;
        self.splash_open = false;
        self.sync_inputs_from_models(cx);
        self.refresh(cx);
    }

    fn record_keybase_stub_event(&mut self, method: String, payload_preview: Option<String>) {
        if self.keybase_inspector.paused {
            return;
        }
        self.keybase_inspector.seq = self.keybase_inspector.seq.saturating_add(1);
        let seq = self.keybase_inspector.seq;
        let entry = self
            .keybase_inspector
            .entries
            .entry(method.clone())
            .or_insert_with(|| KeybaseInspectorEntry {
                method,
                ..KeybaseInspectorEntry::default()
            });
        entry.count = entry.count.saturating_add(1);
        entry.last_seen_seq = seq;
        entry.payload_preview = payload_preview;
    }

    fn keybase_inspector_entries(&self) -> Vec<KeybaseInspectorEntry> {
        let mut rows: Vec<KeybaseInspectorEntry> = self
            .keybase_inspector
            .entries
            .values()
            .filter(|entry| {
                !self.keybase_inspector.unknown_only || entry.method.starts_with("zbase.internal.")
            })
            .cloned()
            .collect();

        rows.sort_by_key(|row| std::cmp::Reverse(row.last_seen_seq));
        rows
    }

    fn format_keybase_inspector_export(&self, rows: &[KeybaseInspectorEntry]) -> String {
        let mut lines = Vec::with_capacity(rows.len().saturating_add(8));
        lines.push(format!("generated_unix_ms={}", now_unix_ms()));
        lines.push(format!("paused={}", self.keybase_inspector.paused));
        lines.push(format!(
            "unknown_only={}",
            self.keybase_inspector.unknown_only
        ));
        lines.push(format!("seq={}", self.keybase_inspector.seq));
        lines.push(format!("rows={}", rows.len()));
        lines.push("method\tcount\tlast_seen_seq\tpayload_preview".to_string());
        for row in rows {
            let payload = row
                .payload_preview
                .as_deref()
                .unwrap_or("—")
                .replace('\n', "\\n")
                .replace('\t', "  ");
            lines.push(format!(
                "{}\t{}\t{}\t{}",
                row.method, row.count, row.last_seen_seq, payload
            ));
        }
        lines.join("\n")
    }

    fn render_keybase_inspector_modal(&self, cx: &mut Context<Self>) -> AnyElement {
        let rows = self.keybase_inspector_entries();

        div()
            .absolute()
            .inset_0()
            .flex()
            .items_center()
            .justify_center()
            .bg(tint(0x000000, 0.45))
            .id("keybase-inspector-backdrop")
            .on_click(cx.listener(Self::close_keybase_inspector))
            .child(
                div()
                    .id("keybase-inspector-modal")
                    .w(px(860.))
                    .h(px(560.))
                    .rounded_xl()
                    .border_1()
                    .border_color(shell_border_strong())
                    .bg(panel_surface())
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .on_click(cx.listener(Self::consume_click))
                    .child(
                        div()
                            .px_4()
                            .py_3()
                            .border_b_1()
                            .border_color(shell_border_strong())
                            .flex()
                            .items_center()
                            .justify_between()
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(rgb(text_primary()))
                                            .child("Keybase Event Inspector"),
                                    )
                                    .child(
                                        div().text_xs().text_color(rgb(text_secondary())).child(
                                            format!(
                                                "{} methods tracked",
                                                self.keybase_inspector.entries.len()
                                            ),
                                        ),
                                    ),
                            )
                            .child(
                                div()
                                    .flex()
                                    .gap_2()
                                    .child(
                                        div()
                                            .id("keybase-inspector-pause")
                                            .on_click(
                                                cx.listener(Self::toggle_keybase_inspector_pause),
                                            )
                                            .child(badge(
                                                if self.keybase_inspector.paused {
                                                    "Resume"
                                                } else {
                                                    "Pause"
                                                },
                                                panel_alt_bg(),
                                                text_primary(),
                                            )),
                                    )
                                    .child(
                                        div()
                                            .id("keybase-inspector-unknown-only")
                                            .on_click(cx.listener(
                                                Self::toggle_keybase_inspector_unknown_only,
                                            ))
                                            .child(badge(
                                                if self.keybase_inspector.unknown_only {
                                                    "Showing Unknown"
                                                } else {
                                                    "Show Unknown Only"
                                                },
                                                panel_alt_bg(),
                                                text_primary(),
                                            )),
                                    )
                                    .child(
                                        div()
                                            .id("keybase-inspector-clear")
                                            .on_click(cx.listener(Self::clear_keybase_inspector))
                                            .child(badge("Clear", panel_alt_bg(), text_primary())),
                                    )
                                    .child(
                                        div()
                                            .id("keybase-inspector-copy")
                                            .on_click(
                                                cx.listener(Self::copy_keybase_inspector_rows),
                                            )
                                            .child(badge("Copy", panel_alt_bg(), text_primary())),
                                    )
                                    .child(
                                        div()
                                            .id("keybase-inspector-close")
                                            .on_click(cx.listener(Self::close_keybase_inspector))
                                            .child(badge(
                                                "Close",
                                                panel_alt_bg(),
                                                text_secondary(),
                                            )),
                                    ),
                            ),
                    )
                    .child(
                        div()
                            .flex()
                            .px_4()
                            .py_2()
                            .gap_4()
                            .border_b_1()
                            .border_color(shell_border_strong())
                            .text_xs()
                            .text_color(rgb(text_secondary()))
                            .child(div().w(px(380.)).child("Method"))
                            .child(div().w(px(80.)).child("Count"))
                            .child(div().w(px(110.)).child("Last Seen"))
                            .child(div().flex_1().child("Payload Preview")),
                    )
                    .child(
                        div()
                            .flex_1()
                            .id("keybase-inspector-scroll")
                            .overflow_y_scroll()
                            .px_4()
                            .py_2()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .children(rows.into_iter().map(|entry| {
                                div()
                                    .w_full()
                                    .px_2()
                                    .py_2()
                                    .rounded_md()
                                    .hover(|s| s.bg(subtle_surface()))
                                    .flex()
                                    .gap_4()
                                    .text_xs()
                                    .child(
                                        div()
                                            .w(px(380.))
                                            .text_color(rgb(text_primary()))
                                            .child(entry.method),
                                    )
                                    .child(
                                        div()
                                            .w(px(80.))
                                            .text_color(rgb(text_primary()))
                                            .child(format!("{}", entry.count)),
                                    )
                                    .child(
                                        div()
                                            .w(px(110.))
                                            .text_color(rgb(text_secondary()))
                                            .child(format!("#{}", entry.last_seen_seq)),
                                    )
                                    .child(div().flex_1().text_color(rgb(text_secondary())).child(
                                        entry.payload_preview.unwrap_or_else(|| "—".to_string()),
                                    ))
                            })),
                    ),
            )
            .into_any_element()
    }

    fn render_inline_autocomplete_overlay(
        &self,
        shell_layout: &ShellLayout,
        window: &Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let viewport_width = f32::from(window.viewport_size().width);
        let sidebar_total_width = self.models.sidebar.width_px + RIGHT_PANE_RESIZE_HANDLE_WIDTH_PX;
        let right_pane_width = if shell_layout.show_right_pane
            && !matches!(self.models.navigation.right_pane, RightPaneMode::Hidden)
        {
            self.models.thread_pane.width_px
        } else {
            0.0
        };
        let main_panel_width =
            (viewport_width - sidebar_total_width - right_pane_width).max(MAIN_PANEL_MIN_WIDTH_PX);
        let composer_popup_width = (main_panel_width - 32.0).clamp(240.0, 520.0);
        let composer_popup_left = (sidebar_total_width + 16.0).max(8.0);

        let thread_popup_width = (self.models.thread_pane.width_px - 32.0).clamp(220.0, 460.0);
        let thread_popup_left = (viewport_width - self.models.thread_pane.width_px + 16.0).max(8.0);

        div()
            .absolute()
            .inset_0()
            .when_some(
                self.models.composer.autocomplete.as_ref(),
                |container, autocomplete| {
                    container.when(!autocomplete.candidates.is_empty(), |container| {
                        container.child(
                            div()
                                .id("inline-autocomplete-overlay-composer")
                                .absolute()
                                .left(px(composer_popup_left))
                                .bottom(px(54.0))
                                .w(px(composer_popup_width))
                                .child(render_autocomplete_popup(autocomplete, true, cx)),
                        )
                    })
                },
            )
            .when(
                shell_layout.show_right_pane
                    && matches!(self.models.navigation.right_pane, RightPaneMode::Thread),
                |container| {
                    container.when_some(
                        self.models.thread_pane.reply_autocomplete.as_ref(),
                        |container, autocomplete| {
                            container.when(!autocomplete.candidates.is_empty(), |container| {
                                container.child(
                                    div()
                                        .id("inline-autocomplete-overlay-thread")
                                        .absolute()
                                        .left(px(thread_popup_left))
                                        .bottom(px(76.0))
                                        .w(px(thread_popup_width))
                                        .child(render_autocomplete_popup(autocomplete, false, cx)),
                                )
                            })
                        },
                    )
                },
            )
            .into_any_element()
    }

    pub(crate) fn quick_switch_to(
        &mut self,
        route: Route,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.models.dismiss_overlays();
        self.navigate_to(route, window, cx);
    }

    pub(crate) fn open_url_or_deep_link(
        &mut self,
        url: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some((route, message_id)) = self.models.resolve_deep_link(url) {
            self.navigate_to_message(route, message_id, window, cx);
        } else if let Some(username) = url.strip_prefix("zbase-mention:") {
            self.open_user_profile_card(UserId::new(username.to_string()), cx);
        } else if let Some(channel_name) = url.strip_prefix("zbase-channel:") {
            if let Some(route) = self.models.resolve_channel_mention(channel_name) {
                self.navigate_to(route, window, cx);
            }
        } else {
            cx.open_url(url);
        }
    }

    fn open_url_action(
        &mut self,
        action: &commands::OpenUrl,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_url_or_deep_link(&action.url, window, cx);
    }

    pub(crate) fn open_quick_switcher_result_at(
        &mut self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(result) = self.models.quick_switcher.results.get(index).cloned() else {
            return;
        };
        self.models.quick_switcher.selected_index = index;
        if result.kind != QuickSwitcherResultKind::Message {
            self.models
                .record_quick_switcher_selection_affinity(&result.conversation_id);
            self.persist_settings();
        }
        if let Some(message_id) = result.message_id {
            self.navigate_to_message(result.route, message_id, window, cx);
        } else {
            self.quick_switch_to(result.route, window, cx);
        }
    }

    fn quick_switcher_recent_result_index(&self, recent_index: usize) -> Option<usize> {
        if !self.models.overlay.quick_switcher_open
            || !self.models.quick_switcher.query.trim().is_empty()
        {
            return None;
        }

        self.models
            .quick_switcher
            .results
            .iter()
            .enumerate()
            .filter(|(_, result)| result.kind != QuickSwitcherResultKind::UnreadChannel)
            .nth(recent_index)
            .map(|(result_index, _)| result_index)
    }

    fn open_quick_switcher_recent_result(
        &mut self,
        recent_index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(result_index) = self.quick_switcher_recent_result_index(recent_index) else {
            return false;
        };
        self.open_quick_switcher_result_at(result_index, window, cx);
        true
    }

    pub(crate) fn run_palette_action(
        &mut self,
        action: &'static str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.models.dismiss_overlays();
        match action {
            "new-chat" => self.open_new_chat(window, cx),
            "search" => self.submit_search_input(window, cx),
            "preferences" => self.navigate_to(Route::Preferences, window, cx),
            "activity" => self.navigate_to(self.activity_route(), window, cx),
            "thread" => self.toggle_pane(RightPaneMode::Thread, window, cx),
            "call" => self.start_or_open_call(window, cx),
            "mark-all-read" => self.mark_all_conversations_read(window, cx),
            _ => self.refresh(cx),
        }
    }

    pub(crate) fn activate_toast(
        &mut self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let action = self
            .models
            .notifications
            .toasts
            .get(index)
            .and_then(|toast| toast.action.clone());

        let Some(action) = action else {
            self.models.dismiss_toast(index);
            self.refresh(cx);
            return;
        };

        self.models.dismiss_toast(index);

        match action {
            ToastAction::OpenPreferences => self.navigate_to(Route::Preferences, window, cx),
            ToastAction::FocusComposer => {
                self.navigate_to(self.current_conversation_route(), window, cx);
                window.focus(&self.composer_input.focus_handle(cx));
            }
            ToastAction::OpenThread => {
                if let Some(root_id) = self.models.thread_pane.root_message_id.clone() {
                    self.open_thread(root_id, window, cx);
                }
            }
            ToastAction::FocusThreadReply => {
                if let Some(root_id) = self.models.thread_pane.root_message_id.clone() {
                    self.open_thread(root_id, window, cx);
                    window.focus(&self.thread_input.focus_handle(cx));
                } else {
                    self.refresh(cx);
                }
            }
            ToastAction::OpenCurrentConversation => {
                self.navigate_to(self.current_conversation_route(), window, cx);
            }
            ToastAction::OpenActiveCall => {
                if let Some(route) = self.models.active_call_route() {
                    self.navigate_to(route, window, cx);
                } else {
                    self.refresh(cx);
                }
            }
        }
    }

    fn go_back_action(
        &mut self,
        _: &commands::NavigateBack,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_back(window, cx);
    }

    fn go_forward_action(
        &mut self,
        _: &commands::NavigateForward,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_forward(window, cx);
    }

    fn show_home_action(
        &mut self,
        _: &commands::ShowHome,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.open_quick_switcher_recent_result(0, window, cx) {
            return;
        }
        self.navigate_to(self.active_workspace_route(), window, cx);
    }

    fn open_quick_switcher_recent_2_action(
        &mut self,
        _: &commands::OpenQuickSwitcherRecent2,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let _ = self.open_quick_switcher_recent_result(1, window, cx);
    }

    fn open_quick_switcher_recent_3_action(
        &mut self,
        _: &commands::OpenQuickSwitcherRecent3,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let _ = self.open_quick_switcher_recent_result(2, window, cx);
    }

    fn open_quick_switcher_recent_4_action(
        &mut self,
        _: &commands::OpenQuickSwitcherRecent4,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let _ = self.open_quick_switcher_recent_result(3, window, cx);
    }

    fn open_quick_switcher_recent_5_action(
        &mut self,
        _: &commands::OpenQuickSwitcherRecent5,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let _ = self.open_quick_switcher_recent_result(4, window, cx);
    }

    fn show_activity_action(
        &mut self,
        _: &commands::ShowActivity,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_to(self.activity_route(), window, cx);
    }

    fn open_preferences_action(
        &mut self,
        _: &commands::OpenPreferences,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_to(Route::Preferences, window, cx);
    }

    fn toggle_thread_pane_action(
        &mut self,
        _: &commands::ToggleThreadPane,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.toggle_pane(RightPaneMode::Thread, window, cx);
    }

    fn toggle_members_pane_action(
        &mut self,
        _: &commands::ToggleMembersPane,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.toggle_pane(RightPaneMode::Members, window, cx);
    }

    fn toggle_details_pane_action(
        &mut self,
        _: &commands::ToggleDetailsPane,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.toggle_pane(RightPaneMode::Details, window, cx);
    }

    fn open_files_pane_action(
        &mut self,
        _: &commands::OpenFilesPane,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_files_pane(cx);
    }

    fn open_search_pane_action(
        &mut self,
        _: &commands::OpenSearchPane,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_search_pane(cx);
    }

    fn open_search_action(
        &mut self,
        _: &commands::OpenSearch,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.submit_search_input(window, cx);
    }

    fn accept_autocomplete_action(
        &mut self,
        _: &commands::AcceptAutocomplete,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(target) = self.autocomplete_target_for_focused_input(window, cx)
            && self.accept_autocomplete_selection(target, None, cx)
        {
            self.refresh(cx);
        }
    }

    fn confirm_primary_action(
        &mut self,
        _: &commands::ConfirmPrimary,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.models.overlay.file_upload_lightbox.is_some() {
            let total = self
                .models
                .overlay
                .file_upload_lightbox
                .as_ref()
                .map_or(0, |lb| lb.candidates.len());
            if total > 1 {
                self.file_upload_send_all(window, cx);
            } else {
                self.file_upload_send_current(window, cx);
            }
            return;
        }
        if let Some(target) = self.autocomplete_target_for_focused_input(window, cx)
            && self.accept_autocomplete_selection(target, None, cx)
        {
            self.refresh(cx);
            return;
        }
        if self.models.overlay.quick_switcher_open {
            if let Some(selected) = self.models.quick_switcher_selected_result().cloned() {
                if selected.kind != QuickSwitcherResultKind::Message {
                    self.models
                        .record_quick_switcher_selection_affinity(&selected.conversation_id);
                    self.persist_settings();
                }
                if let Some(message_id) = selected.message_id {
                    self.navigate_to_message(selected.route, message_id, window, cx);
                } else {
                    self.quick_switch_to(selected.route, window, cx);
                }
            }
        } else if self.models.overlay.new_chat_open {
            self.new_chat_create(window, cx);
        } else if self.search_input.focus_handle(cx).is_focused(window) {
            if matches!(self.models.navigation.current, Route::Search { .. }) {
                if let Some(index) = self.models.search.highlighted_index {
                    self.open_search_result_at(index, window, cx);
                } else {
                    self.submit_search_input(window, cx);
                }
            } else {
                self.submit_search_input(window, cx);
            }
        } else if self.composer_input.focus_handle(cx).is_focused(window) {
            self.send_composer_message(window, cx);
        } else if self.thread_input.focus_handle(cx).is_focused(window) {
            self.send_thread_reply(window, cx);
        } else if matches!(self.models.navigation.current, Route::Search { .. }) {
            if let Some(index) = self.models.search.highlighted_index {
                self.open_search_result_at(index, window, cx);
            }
        } else if matches!(self.models.navigation.current, Route::Activity { .. }) {
            if let Some(index) = self.models.notifications.highlighted_index {
                self.open_activity_item_at(index, window, cx);
            }
        } else if matches!(
            self.models.navigation.current,
            Route::Channel { .. } | Route::DirectMessage { .. }
        ) && let Some(message_id) = self.models.timeline.highlighted_message_id.clone()
        {
            self.open_thread(message_id, window, cx);
        }
    }

    fn select_previous_action(
        &mut self,
        _: &commands::SelectPrevious,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(target) = self.autocomplete_target_for_focused_input(window, cx)
            && self.move_autocomplete_selection(target, -1)
        {
            self.refresh(cx);
            return;
        }
        if self.models.overlay.quick_switcher_open {
            self.models.move_quick_switcher_selection(-1);
            self.refresh(cx);
        } else if matches!(self.models.navigation.current, Route::Search { .. }) {
            self.models.move_search_highlight(-1);
            self.refresh(cx);
        } else if matches!(self.models.navigation.current, Route::Activity { .. }) {
            self.models.move_activity_highlight(-1);
            self.refresh(cx);
        } else if matches!(
            self.models.navigation.current,
            Route::Channel { .. } | Route::DirectMessage { .. }
        ) {
            self.models.move_timeline_highlight(-1);
            self.refresh(cx);
        }
    }

    fn select_next_action(
        &mut self,
        _: &commands::SelectNext,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(target) = self.autocomplete_target_for_focused_input(window, cx)
            && self.move_autocomplete_selection(target, 1)
        {
            self.refresh(cx);
            return;
        }
        if self.models.overlay.quick_switcher_open {
            self.models.move_quick_switcher_selection(1);
            self.refresh(cx);
        } else if matches!(self.models.navigation.current, Route::Search { .. }) {
            self.models.move_search_highlight(1);
            self.refresh(cx);
        } else if matches!(self.models.navigation.current, Route::Activity { .. }) {
            self.models.move_activity_highlight(1);
            self.refresh(cx);
        } else if matches!(
            self.models.navigation.current,
            Route::Channel { .. } | Route::DirectMessage { .. }
        ) {
            self.models.move_timeline_highlight(1);
            self.refresh(cx);
        }
    }

    fn select_sidebar_previous_action(
        &mut self,
        _: &commands::SelectSidebarPrevious,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.models.move_sidebar_highlight(-1);
        self.refresh(cx);
    }

    fn select_sidebar_next_action(
        &mut self,
        _: &commands::SelectSidebarNext,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.models.move_sidebar_highlight(1);
        self.refresh(cx);
    }

    fn activate_sidebar_selection_action(
        &mut self,
        _: &commands::ActivateSidebarSelection,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(route) = self.models.sidebar.highlighted_route.clone() {
            self.navigate_to(route, window, cx);
        }
    }

    fn toggle_quick_switcher_action(
        &mut self,
        _: &commands::ToggleQuickSwitcher,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.toggle_quick_switcher(cx);
        if self.models.overlay.quick_switcher_open {
            window.focus(&self.quick_switcher_input.focus_handle(cx));
        } else {
            self.restore_chat_focus(window, cx);
        }
    }

    fn open_new_chat_action(
        &mut self,
        _: &commands::OpenNewChat,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_new_chat(window, cx);
    }

    fn toggle_find_in_chat_action(
        &mut self,
        _: &commands::ToggleFindInChat,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.toggle_find_in_chat(window, cx);
    }

    fn close_find_in_chat_action(
        &mut self,
        _: &commands::CloseFindInChat,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.close_find_in_chat(window, cx);
    }

    fn find_next_match_action(
        &mut self,
        _: &commands::FindNextMatch,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.find_next_match(cx);
        if self.models.find_in_chat.open {
            window.focus(&self.find_in_chat_input.focus_handle(cx));
        }
    }

    fn find_prev_match_action(
        &mut self,
        _: &commands::FindPrevMatch,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.find_previous_match(cx);
        if self.models.find_in_chat.open {
            window.focus(&self.find_in_chat_input.focus_handle(cx));
        }
    }

    fn toggle_command_palette_action(
        &mut self,
        _: &commands::ToggleCommandPalette,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.toggle_command_palette(cx);
    }

    fn toggle_keybase_inspector_action(
        &mut self,
        _: &commands::ToggleKeybaseInspector,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.toggle_keybase_inspector(cx);
    }

    fn toggle_splash_screen_action(
        &mut self,
        _: &commands::ToggleSplashScreen,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.toggle_splash_screen(cx);
    }

    fn close_window_action(
        &mut self,
        _: &commands::CloseWindow,
        window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
        window.remove_window();
    }

    fn quit_app_action(
        &mut self,
        _: &commands::QuitApp,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        cx.quit();
    }

    fn toggle_benchmark_capture_action(
        &mut self,
        _: &commands::ToggleBenchmarkCapture,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.toggle_perf_capture(cx);
    }

    fn dismiss_overlays_action(
        &mut self,
        _: &commands::DismissOverlays,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(target) = self.autocomplete_target_for_focused_input(window, cx) {
            let dismissed_signature = self.current_autocomplete_dismiss_signature(target, cx);
            if self.clear_autocomplete_for_target(target, cx) {
                if let Some(signature) = dismissed_signature {
                    match target {
                        InputAutocompleteTarget::Composer => {
                            self.dismissed_composer_autocomplete = Some(signature)
                        }
                        InputAutocompleteTarget::Thread => {
                            self.dismissed_thread_autocomplete = Some(signature)
                        }
                    }
                }
                self.refresh(cx);
                return;
            }
        }
        let had_overlay = self.models.overlay.new_chat_open
            || self.models.overlay.quick_switcher_open
            || self.models.overlay.command_palette_open
            || self.models.overlay.emoji_picker_open
            || self.models.overlay.fullscreen_image.is_some()
            || self.models.overlay.file_upload_lightbox.is_some()
            || self.models.overlay.profile_card_user_id.is_some()
            || self.keybase_inspector.open
            || self.splash_open;
        let is_editing = matches!(self.models.composer.mode, ComposerMode::Edit { .. });
        let should_close_right_pane = !had_overlay
            && !is_editing
            && matches!(
                self.models.navigation.right_pane,
                RightPaneMode::Thread | RightPaneMode::Profile(_) | RightPaneMode::Details
            );
        if !had_overlay && is_editing {
            self.cancel_edit(cx);
            return;
        }
        self.dismiss_overlays(cx);
        if should_close_right_pane {
            self.close_right_pane(window, cx);
        }
        if had_overlay || should_close_right_pane {
            self.restore_chat_focus(window, cx);
        }
    }

    fn dismiss_hover_toolbar_action(
        &mut self,
        _: &commands::DismissHoverToolbar,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.clear_hovered_message_immediate(cx);
    }
}

impl Render for AppWindow {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let render_t0 = Instant::now();
        if self.pending_file_upload_caption_focus {
            self.pending_file_upload_caption_focus = false;
            window.focus(&self.file_upload_caption_input.focus_handle(cx));
        }
        if !window.is_window_hovered() && self.models.timeline.hovered_message_id.is_some() {
            self.schedule_hover_clear(cx);
        }
        let resolved_theme = resolve_theme(&self.models.settings.theme_mode, window.appearance());
        let theme_changed = self.resolved_theme != resolved_theme;
        self.resolved_theme = resolved_theme;
        if theme_changed {
            self.sync_sidebar_view_state(cx);
        }

        let element = with_theme(resolved_theme, || {
            let shell_layout = shell_layout(
                &self.models.navigation.right_pane,
                self.models.thread_pane.width_px,
                self.models.sidebar.width_px,
                f32::from(window.viewport_size().width),
            );
            let capabilities = current_backend_capabilities(self.app_store.snapshot());

            let t_right = Instant::now();
            let right_pane = shell_layout.show_right_pane.then(|| {
                RightPaneHost.render(
                    &self.models.navigation,
                    &self.models.thread_pane,
                    &self.models.conversation,
                    &self.models.profile_panel,
                    &self.models.search,
                    &self.models.timeline,
                    &self.video_render_cache,
                    &self.failed_video_urls,
                    &mut self.code_highlight_cache,
                    &mut self.selectable_texts,
                    &self.thread_input,
                    &self.thread_scroll,
                    &self.profile_scroll,
                    &self.profile_social_scroll,
                    self.thread_unseen_count,
                    &capabilities,
                    cx,
                )
            });
            let right_elapsed = t_right.elapsed();
            self.perf_harness
                .record_duration(PerfTimer::RightPaneRender, right_elapsed);

            let call_dock = self
                .models
                .call
                .active_call
                .as_ref()
                .map(|_| MiniCallDock.render(&self.models.call, cx));

            div()
                .size_full()
                .flex()
                .bg(app_backdrop())
                .overflow_hidden()
                .track_focus(&self.focus_handle)
                .key_context("Workspace")
                .on_action(cx.listener(Self::go_back_action))
                .on_action(cx.listener(Self::go_forward_action))
                .on_action(cx.listener(Self::show_home_action))
                .on_action(cx.listener(Self::open_quick_switcher_recent_2_action))
                .on_action(cx.listener(Self::open_quick_switcher_recent_3_action))
                .on_action(cx.listener(Self::open_quick_switcher_recent_4_action))
                .on_action(cx.listener(Self::open_quick_switcher_recent_5_action))
                .on_action(cx.listener(Self::show_activity_action))
                .on_action(cx.listener(Self::open_preferences_action))
                .on_action(cx.listener(Self::toggle_thread_pane_action))
                .on_action(cx.listener(Self::toggle_members_pane_action))
                .on_action(cx.listener(Self::toggle_details_pane_action))
                .on_action(cx.listener(Self::open_files_pane_action))
                .on_action(cx.listener(Self::open_search_pane_action))
                .on_action(cx.listener(Self::open_search_action))
                .on_action(cx.listener(Self::accept_autocomplete_action))
                .on_action(cx.listener(Self::confirm_primary_action))
                .on_action(cx.listener(Self::select_previous_action))
                .on_action(cx.listener(Self::select_next_action))
                .on_action(cx.listener(Self::select_sidebar_previous_action))
                .on_action(cx.listener(Self::select_sidebar_next_action))
                .on_action(cx.listener(Self::activate_sidebar_selection_action))
                .on_action(cx.listener(Self::toggle_quick_switcher_action))
                .on_action(cx.listener(Self::open_new_chat_action))
                .on_action(cx.listener(Self::toggle_find_in_chat_action))
                .on_action(cx.listener(Self::close_find_in_chat_action))
                .on_action(cx.listener(Self::find_next_match_action))
                .on_action(cx.listener(Self::find_prev_match_action))
                .on_action(cx.listener(Self::toggle_command_palette_action))
                .on_action(cx.listener(Self::toggle_keybase_inspector_action))
                .on_action(cx.listener(Self::toggle_splash_screen_action))
                .on_action(cx.listener(Self::close_window_action))
                .on_action(cx.listener(Self::quit_app_action))
                .on_action(cx.listener(Self::toggle_benchmark_capture_action))
                .on_action(cx.listener(Self::dismiss_overlays_action))
                .on_action(cx.listener(Self::dismiss_hover_toolbar_action))
                .on_action(cx.listener(Self::open_url_action))
                .on_drag_move::<RightPaneResizeDrag>(
                    cx.listener(Self::update_thread_resize_drag_on_drag),
                )
                .on_drag_move::<SidebarResizeDrag>(
                    cx.listener(Self::update_sidebar_resize_drag_on_drag),
                )
                .on_mouse_move(cx.listener(Self::update_thread_resize_drag))
                .on_mouse_move(cx.listener(|this, _, _, cx| {
                    this.clear_hovered_message(cx);
                    this.clear_sidebar_hover_tooltip(cx);
                }))
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(|this, _, _, cx| {
                        // Click-away: dismiss hover toolbar when clicking anywhere outside it.
                        if this.models.timeline.hover_toolbar_settled
                            && this.models.timeline.hovered_message_id.is_some()
                        {
                            this.clear_hovered_message_immediate(cx);
                        }
                    }),
                )
                .on_mouse_up(
                    MouseButton::Left,
                    cx.listener(Self::finish_thread_resize_drag),
                )
                .on_mouse_up_out(
                    MouseButton::Left,
                    cx.listener(Self::finish_thread_resize_drag),
                )
                .child({
                    let t = Instant::now();
                    let sidebar_row_count: usize = self
                        .models
                        .sidebar
                        .sections
                        .iter()
                        .fold(0usize, |total, section| {
                            total.saturating_add(section.rows.len())
                        });
                    let el = self.sidebar_view.as_ref().map_or_else(
                        || {
                            Sidebar.render(
                                &self.models.sidebar,
                                &self.models.app.connectivity,
                                &self.models.app.current_user_display_name,
                                self.models.app.current_user_avatar_asset.as_deref(),
                                &self.sidebar_dm_avatar_assets,
                                &self.models.navigation.current,
                                None,
                                &self.sidebar_scroll_handle,
                                cx,
                            )
                        },
                        |sidebar_view| {
                            AnyView::from(sidebar_view.clone())
                                .cached(
                                    StyleRefinement::default()
                                        .w(px(self.models.sidebar.width_px))
                                        .flex_shrink_0()
                                        .h_full(),
                                )
                                .into_any_element()
                        },
                    );
                    let e = t.elapsed();
                    self.perf_harness
                        .record_duration(PerfTimer::SidebarRender, e);
                    if self.perf_harness.is_capturing() && e.as_millis() > 1 {
                        tracing::warn!(
                            "  sidebar.render: {e:?} ({sidebar_row_count} rows, {} sections)",
                            self.models.sidebar.sections.len()
                        );
                    }
                    el
                })
                .child(
                    div()
                        .w(px(RIGHT_PANE_RESIZE_HANDLE_WIDTH_PX))
                        .h_full()
                        .bg(if is_dark_theme() {
                            tint(sidebar_bg(), 0.98)
                        } else {
                            glass_surface_dark()
                        })
                        .cursor(CursorStyle::ResizeColumn)
                        .id("sidebar-resize-handle")
                        .on_drag(SidebarResizeDrag, |_, _, _, cx| {
                            cx.new(|_| SidebarResizeDragPreview)
                        })
                        .on_mouse_down(MouseButton::Left, cx.listener(Self::begin_sidebar_resize))
                        .flex()
                        .items_center()
                        .justify_center()
                        .group("resize-handle-sidebar")
                        .child(
                            div()
                                .w(px(2.))
                                .h(px(640.))
                                .rounded_full()
                                .opacity(0.)
                                .group_hover("resize-handle-sidebar", |s| s.opacity(1.))
                                .bg(rgb(border())),
                        ),
                )
                .child({
                    let t = Instant::now();
                    let el = MainPanelHost.render(
                        &self.models,
                        &self.video_render_cache,
                        &self.failed_video_urls,
                        &self.search_input,
                        &self.find_in_chat_input,
                        &self.composer_input,
                        &self.timeline_list_state,
                        self.timeline_unseen_count,
                        !self.timeline_is_near_bottom() && !self.models.timeline.rows.is_empty(),
                        cx,
                    );
                    let e = t.elapsed();
                    self.perf_harness
                        .record_duration(PerfTimer::MainPanelRender, e);
                    if self.perf_harness.is_capturing() && e.as_millis() > 1 {
                        tracing::warn!(
                            "  main_panel.render: {e:?} ({} timeline rows)",
                            self.models.timeline.rows.len()
                        );
                    }
                    if self.perf_harness.is_capturing() && right_elapsed.as_millis() > 1 {
                        tracing::warn!("  right_pane.render: {right_elapsed:?}");
                    }
                    el
                })
                .when_some(right_pane, |div, pane| div.child(pane))
                .when_some(call_dock, |div, dock| div.child(dock))
                .child({
                    let overlay_t0 = Instant::now();
                    let quick_switcher_indexing_status = self.quick_switcher_indexing_status_text();
                    let snapshot = self.app_store.snapshot();
                    let custom_emoji_index =
                        snapshot
                            .timeline
                            .conversation_id
                            .as_ref()
                            .and_then(|conversation_id| {
                                snapshot.backend.conversation_emojis.get(conversation_id)
                            });
                    let supports_custom_emoji = snapshot
                        .backend
                        .accounts
                        .values()
                        .find(|account| {
                            matches!(account.connection_state, ConnectionState::Connected)
                        })
                        .or_else(|| snapshot.backend.accounts.values().next())
                        .map(|account| account.capabilities.supports_custom_emoji)
                        .unwrap_or(false);
                    let overlay = OverlayHost.render(
                        &self.models.overlay,
                        &self.models.profile_panel,
                        &self.models.notifications,
                        &self.models.quick_switcher,
                        &self.models.new_chat,
                        &self.quick_switcher_input,
                        &self.new_chat_input,
                        &self.file_upload_caption_input,
                        &self.models.emoji_picker,
                        &self.emoji_picker_input,
                        custom_emoji_index,
                        supports_custom_emoji,
                        quick_switcher_indexing_status.as_deref(),
                        cx,
                    );
                    if self.models.overlay.quick_switcher_open {
                        tracing::debug!(
                            target: "zbase.quick_switcher.perf",
                            overlay_render_ms = overlay_t0.elapsed().as_millis(),
                            rendered_rows = self.models.quick_switcher.results.len(),
                            "quick_switcher_overlay_render"
                        );
                    }
                    overlay
                })
                .child(self.render_inline_autocomplete_overlay(&shell_layout, window, cx))
                .when_some(
                    self.models.overlay.sidebar_hover_tooltip.as_ref(),
                    |div, tooltip| div.child(render_sidebar_hover_tooltip(tooltip, window)),
                )
                .when_some(
                    self.models.overlay.reaction_hover_tooltip.as_ref(),
                    |div, tooltip| div.child(render_reaction_hover_tooltip(tooltip, window)),
                )
                .when(self.keybase_inspector.open, |div| {
                    div.child(self.render_keybase_inspector_modal(cx))
                })
                .when(self.splash_open, |div| {
                    let snapshot = self.app_store.snapshot();
                    let status = if !snapshot.app.boot_status.is_empty() {
                        snapshot.app.boot_status.clone()
                    } else {
                        match snapshot.app.boot_phase {
                            BootPhase::Launching => "Starting up…".to_string(),
                            BootPhase::HydratingLocalState => "Loading local data…".to_string(),
                            BootPhase::ConnectingBackend => "Connecting to Keybase…".to_string(),
                            BootPhase::Ready => String::new(),
                            BootPhase::Degraded => "Connection degraded".to_string(),
                            BootPhase::FatalError => "Failed to connect".to_string(),
                        }
                    };
                    div.child(crate::views::splash::SplashView.render(&status, cx))
                })
        });

        let render_elapsed = render_t0.elapsed();
        self.perf_harness
            .record_duration(PerfTimer::RenderTotal, render_elapsed);
        if self.perf_harness.is_capturing() && render_elapsed.as_millis() > 2 {
            tracing::warn!("render() took {render_elapsed:?}");
        }

        element
    }
}

fn render_sidebar_hover_tooltip(
    tooltip: &crate::models::overlay_model::SidebarHoverTooltip,
    window: &Window,
) -> AnyElement {
    let viewport = window.viewport_size();
    let viewport_width = f32::from(viewport.width);
    let viewport_height = f32::from(viewport.height);
    let width_px = tooltip.width_px.max(1.0);
    let clamped_x = tooltip
        .anchor_x
        .min((viewport_width - width_px - 8.0).max(8.0))
        .max(8.0);
    let clamped_y = tooltip
        .anchor_y
        .min((viewport_height - 28.0).max(8.0))
        .max(8.0);

    div()
        .absolute()
        .left(px(clamped_x))
        .top(px(clamped_y))
        .w(px(width_px))
        .rounded_md()
        .bg(tint(panel_bg(), 0.92))
        .border_1()
        .border_color(shell_border_strong())
        .px_2()
        .py_1()
        .text_xs()
        .text_color(rgb(text_primary()))
        .child(tooltip.text.clone())
        .into_any_element()
}

fn render_reaction_hover_tooltip(
    tooltip: &crate::models::overlay_model::ReactionHoverTooltip,
    window: &Window,
) -> AnyElement {
    let viewport = window.viewport_size();
    let viewport_width = f32::from(viewport.width);
    let viewport_height = f32::from(viewport.height);
    let width_px = tooltip.width_px.max(1.0);
    let clamped_x = tooltip
        .anchor_x
        .min((viewport_width - width_px - 8.0).max(8.0))
        .max(8.0);
    let clamped_y = tooltip
        .anchor_y
        .min((viewport_height - 28.0).max(8.0))
        .max(8.0);

    div()
        .absolute()
        .left(px(clamped_x))
        .top(px(clamped_y))
        .w(px(width_px))
        .rounded_md()
        .bg(tint(panel_bg(), 0.92))
        .border_1()
        .border_color(shell_border_strong())
        .px_2()
        .py_1()
        .text_xs()
        .text_color(rgb(text_primary()))
        .child(tooltip.text.clone())
        .into_any_element()
}

fn estimate_tooltip_width_px(text: &str, min_width_px: f32, max_width_px: f32) -> f32 {
    // Simple heuristic for `.text_xs()` to keep tooltips "hugging" content without
    // needing font metric measurement.
    let chars = text.chars().count().clamp(10, 96) as f32;
    let estimated = chars * 7.1 + 18.0; // glyphs + padding/border
    estimated.clamp(min_width_px, max_width_px)
}

fn timeline_message_count(models: &AppModels) -> usize {
    models.timeline.rows.iter().fold(0usize, |total, row| {
        let add = match row {
            crate::models::timeline_model::TimelineRow::Message(_) => 1,
            _ => 0,
        };
        total.saturating_add(add)
    })
}

fn timeline_row_index_for_message(
    rows: &[crate::models::timeline_model::TimelineRow],
    message_id: &MessageId,
) -> Option<usize> {
    rows.iter().position(|row| match row {
        crate::models::timeline_model::TimelineRow::Message(message_row) => {
            &message_row.message.id == message_id
        }
        _ => false,
    })
}

fn shell_layout(
    right_pane_mode: &RightPaneMode,
    right_pane_width_px: f32,
    sidebar_width_px: f32,
    viewport_width_px: f32,
) -> ShellLayout {
    let base_shell_width =
        SHELL_HORIZONTAL_PADDING_PX + sidebar_width_px + MAIN_PANEL_MIN_WIDTH_PX + SHELL_GAP_PX;

    let right_pane_width = match right_pane_mode {
        RightPaneMode::Hidden => 0.0,
        // Treat all non-hidden right-pane modes as resizable (thread/profile/members/etc.)
        // so the resize handle is consistent and layout gating matches the actual width.
        _ => right_pane_width_px,
    };

    let full_shell_width = if right_pane_width > 0.0 {
        base_shell_width + SHELL_GAP_PX + right_pane_width
    } else {
        base_shell_width
    };

    let show_right_pane =
        !matches!(right_pane_mode, RightPaneMode::Hidden) && viewport_width_px >= full_shell_width;

    ShellLayout {
        show_right_pane,
        min_shell_width: if show_right_pane {
            full_shell_width
        } else {
            base_shell_width
        },
    }
}

fn current_backend_capabilities(snapshot: &crate::state::state::UiState) -> BackendCapabilities {
    snapshot
        .backend
        .accounts
        .values()
        .find(|account| matches!(account.connection_state, ConnectionState::Connected))
        .or_else(|| snapshot.backend.accounts.values().next())
        .map(|account| account.capabilities.clone())
        .unwrap_or_default()
}

fn build_channel_details(
    summary: &ConversationSummary,
    member_count: u32,
    can_post: bool,
    is_archived: bool,
    backend: &crate::state::state::BackendRuntimeState,
    current_user_id: Option<&UserId>,
) -> ChannelDetails {
    let mut members = Vec::new();
    let team_role_map = backend
        .conversation_team_ids
        .get(&summary.id)
        .and_then(|team_id| backend.team_roles.get(team_id));
    let mut sorted_member_ids = backend
        .conversation_members
        .get(&summary.id)
        .map(|state| state.members.clone())
        .unwrap_or_default();
    if sorted_member_ids.is_empty() {
        sorted_member_ids = match summary.kind {
            ConversationKind::DirectMessage | ConversationKind::GroupDirectMessage => summary
                .title
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|username| UserId::new(username.to_ascii_lowercase()))
                .collect(),
            ConversationKind::Channel => Vec::new(),
        };
    }
    sorted_member_ids.sort_by(|left, right| left.0.cmp(&right.0));
    for user_id in sorted_member_ids.into_iter() {
        let profile = backend.user_profiles.get(&user_id).or_else(|| {
            backend
                .user_profiles
                .get(&UserId::new(user_id.0.to_ascii_lowercase()))
        });
        let display_name = profile
            .map(|profile| profile.display_name.trim())
            .filter(|name| !name.is_empty())
            .map(|name| name.to_string())
            .unwrap_or_else(|| user_id.0.clone());
        let preview = ChannelMemberPreview {
            user_id: user_id.clone(),
            display_name,
            avatar_asset: profile.and_then(|profile| profile.avatar_asset.clone()),
            affinity: backend
                .user_affinities
                .get(&user_id)
                .cloned()
                .or_else(|| {
                    backend
                        .user_affinities
                        .get(&UserId::new(user_id.0.to_ascii_lowercase()))
                        .cloned()
                })
                .unwrap_or(Affinity::None),
            is_team_admin_or_owner: team_role_map.is_some_and(|roles| {
                matches!(
                    roles.get(&user_id),
                    Some(TeamRoleKind::Admin | TeamRoleKind::Owner)
                )
            }),
        };
        members.push(preview);
    }
    // If we don't have a roster yet:
    // - DMs/GDMs: we can reasonably derive from title.
    // - Channels: we intentionally show empty until backend provides roster.

    members.sort_by(|left, right| {
        let left_name = left.display_name.trim().to_ascii_lowercase();
        let right_name = right.display_name.trim().to_ascii_lowercase();
        left_name
            .cmp(&right_name)
            .then_with(|| left.user_id.0.cmp(&right.user_id.0))
    });
    let member_preview = members.iter().take(6).cloned().collect::<Vec<_>>();

    let role = current_user_id
        .and_then(|user_id| team_role_map.and_then(|roles| roles.get(user_id)))
        .copied();
    let can_manage_members = matches!(role, Some(TeamRoleKind::Admin | TeamRoleKind::Owner));
    let notification_level = if summary.muted {
        NotificationLevel::Nothing
    } else {
        NotificationLevel::All
    };
    let topic = summary.topic.trim().to_string();

    let derived_member_count = if let Some(state) = backend.conversation_members.get(&summary.id) {
        state.members.len() as u32
    } else if member_count > 0 {
        member_count
    } else {
        members.len() as u32
    };

    ChannelDetails {
        conversation_id: summary.id.clone(),
        title: summary.title.clone(),
        topic: topic.clone(),
        kind: summary.kind.clone(),
        group: summary.group.clone(),
        member_count: derived_member_count,
        members,
        member_preview,
        notification_level,
        pinned_items: backend
            .conversation_pins
            .get(&summary.id)
            .map(|pinned| pinned.items.clone())
            .unwrap_or_default(),
        can_edit_topic: can_manage_members,
        can_manage_members,
        can_archive: can_manage_members,
        can_leave: matches!(summary.kind, ConversationKind::Channel),
        can_post,
        created_at: None,
        description: (!topic.is_empty()).then_some(topic),
        is_archived,
    }
}

fn is_near_bottom(handle: &ScrollHandle) -> bool {
    let max_offset = handle.max_offset();
    if max_offset.height <= px(0.) {
        return true;
    }

    let distance_from_bottom = (handle.offset().y + max_offset.height).abs();
    distance_from_bottom <= px(24.)
}

fn is_near_top(handle: &ScrollHandle) -> bool {
    handle.offset().y.abs() <= px(24.)
}

fn is_list_near_bottom(state: &ListState) -> bool {
    let max_offset = state.max_offset_for_scrollbar();
    if max_offset.height <= px(0.) {
        return true;
    }

    let scroll_offset = state.scroll_px_offset_for_scrollbar().y.abs();
    // If the user "overscrolls" past the bottom (e.g. elastic scrolling),
    // treat that as still being at the bottom.
    let distance_from_bottom = if scroll_offset >= max_offset.height {
        px(0.)
    } else {
        max_offset.height - scroll_offset
    };
    distance_from_bottom <= px(24.)
}

fn is_list_near_top(state: &ListState) -> bool {
    let offset = state.logical_scroll_top();
    offset.item_ix == 0 && offset.offset_in_item <= px(24.)
}

fn scroll_list_to_bottom(state: &ListState) {
    state.scroll_to(ListOffset {
        item_ix: state.item_count(),
        offset_in_item: px(0.),
    });
}

fn typing_indicator_label(users: &[UserId]) -> Option<String> {
    if users.is_empty() {
        None
    } else if users.len() == 1 {
        Some(format!("{} is typing…", users[0].0))
    } else {
        Some(format!("{} people are typing…", users.len()))
    }
}

fn quick_switcher_seq_from_query_id(query_id: &str) -> Option<u64> {
    query_id
        .strip_prefix("quick-switcher-")
        .and_then(|value| value.parse::<u64>().ok())
}

fn extract_u64_from_internal_payload(payload_preview: Option<&str>, key: &str) -> Option<u64> {
    let payload = payload_preview?;
    let key_index = payload.find(key)?;
    let after_key = &payload[key_index + key.len()..];
    let digits_start = after_key.find(|ch: char| ch.is_ascii_digit())?;
    let digits = after_key[digits_start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        return None;
    }
    digits.parse::<u64>().ok()
}

const SIG_SEED: u64 = 0xcbf29ce484222325;
const SIG_MUL: u64 = 0x100000001b3;

fn mix_sig(sig: &mut u64, value: u64) {
    *sig = sig.wrapping_mul(SIG_MUL).wrapping_add(value);
}

fn mix_sig_bool(sig: &mut u64, value: bool) {
    mix_sig(sig, u64::from(value));
}

fn mix_sig_str(sig: &mut u64, value: &str) {
    mix_sig(sig, value.len() as u64);
    for byte in value.as_bytes() {
        *sig = sig.wrapping_mul(SIG_MUL) ^ u64::from(*byte);
    }
}

fn mix_sig_route(sig: &mut u64, route: &Route) {
    match route {
        Route::WorkspaceHome { workspace_id } => {
            mix_sig(sig, 1);
            mix_sig_str(sig, &workspace_id.0);
        }
        Route::Channel {
            workspace_id,
            channel_id,
        } => {
            mix_sig(sig, 2);
            mix_sig_str(sig, &workspace_id.0);
            mix_sig_str(sig, &channel_id.0);
        }
        Route::DirectMessage {
            workspace_id,
            dm_id,
        } => {
            mix_sig(sig, 3);
            mix_sig_str(sig, &workspace_id.0);
            mix_sig_str(sig, &dm_id.0);
        }
        Route::Search {
            workspace_id,
            query,
        } => {
            mix_sig(sig, 4);
            mix_sig_str(sig, &workspace_id.0);
            mix_sig_str(sig, query);
        }
        Route::Activity { workspace_id } => {
            mix_sig(sig, 5);
            mix_sig_str(sig, &workspace_id.0);
        }
        Route::Preferences => {
            mix_sig(sig, 6);
        }
        Route::ActiveCall {
            workspace_id,
            call_id,
        } => {
            mix_sig(sig, 7);
            mix_sig_str(sig, &workspace_id.0);
            mix_sig_str(sig, &call_id.0);
        }
    }
}

fn mix_sig_conversation_summary(
    sig: &mut u64,
    summary: &crate::domain::conversation::ConversationSummary,
) {
    mix_sig_str(sig, &summary.id.0);
    mix_sig_str(sig, &summary.title);
    mix_sig(
        sig,
        match summary.kind {
            crate::domain::conversation::ConversationKind::Channel => 1,
            crate::domain::conversation::ConversationKind::DirectMessage => 2,
            crate::domain::conversation::ConversationKind::GroupDirectMessage => 3,
        },
    );
    mix_sig_str(sig, &summary.topic);
    mix_sig(sig, summary.unread_count as u64);
    mix_sig(sig, summary.mention_count as u64);
    mix_sig_bool(sig, summary.muted);
    match &summary.group {
        Some(group) => {
            mix_sig(sig, 1);
            mix_sig_str(sig, &group.id);
            mix_sig_str(sig, &group.display_name);
        }
        None => mix_sig(sig, 0),
    }
}

fn workspace_state_signature(workspace: &crate::state::state::UiWorkspaceState) -> u64 {
    let mut sig = SIG_SEED;
    if let Some(active_workspace_id) = workspace.active_workspace_id.as_ref() {
        mix_sig(&mut sig, 1);
        mix_sig_str(&mut sig, &active_workspace_id.0);
    } else {
        mix_sig(&mut sig, 0);
    }
    mix_sig_str(&mut sig, &workspace.workspace_name);
    mix_sig(&mut sig, workspace.channels.len() as u64);
    for summary in &workspace.channels {
        mix_sig_conversation_summary(&mut sig, summary);
    }
    mix_sig(&mut sig, workspace.direct_messages.len() as u64);
    for summary in &workspace.direct_messages {
        mix_sig_conversation_summary(&mut sig, summary);
    }
    sig
}

fn sidebar_sections_state_signature(
    sections: &[crate::state::state::UiSidebarSectionState],
) -> u64 {
    let mut sig = SIG_SEED;
    mix_sig(&mut sig, sections.len() as u64);
    for section in sections {
        match section.id.as_ref() {
            Some(id) => {
                mix_sig(&mut sig, 1);
                mix_sig_str(&mut sig, &id.0);
            }
            None => mix_sig(&mut sig, 0),
        }
        mix_sig_str(&mut sig, &section.title);
        mix_sig_bool(&mut sig, section.collapsed);
        mix_sig(&mut sig, section.rows.len() as u64);
        for row in &section.rows {
            mix_sig_str(&mut sig, &row.label);
            mix_sig(&mut sig, row.unread_count as u64);
            mix_sig(&mut sig, row.mention_count as u64);
            match row.route.as_ref() {
                Some(route) => {
                    mix_sig(&mut sig, 1);
                    mix_sig_route(&mut sig, route);
                }
                None => mix_sig(&mut sig, 0),
            }
        }
    }
    sig
}

fn first_dm_username(title: &str) -> Option<&str> {
    title
        .split(',')
        .map(str::trim)
        .find(|value| !value.is_empty())
}

fn dm_avatar_inputs_signature(
    sections: &[crate::models::sidebar_model::SidebarSection],
    direct_messages: &[crate::domain::conversation::ConversationSummary],
    user_profiles: &HashMap<UserId, crate::state::state::UserProfileState>,
) -> u64 {
    let mut sig = SIG_SEED;
    for row in sections.iter().flat_map(|section| section.rows.iter()) {
        if let Route::DirectMessage { dm_id, .. } = &row.route {
            mix_sig_str(&mut sig, &dm_id.0);
            mix_sig_str(&mut sig, &row.label);
        }
    }

    for summary in direct_messages {
        if !matches!(
            summary.kind,
            crate::domain::conversation::ConversationKind::DirectMessage
        ) {
            continue;
        }
        mix_sig_str(&mut sig, &summary.id.0);
        mix_sig_str(&mut sig, &summary.title);
        if let Some(username) = first_dm_username(&summary.title) {
            mix_sig_str(&mut sig, username);
            let user_id = UserId::new(username.to_ascii_lowercase());
            if let Some(profile) = user_profiles.get(&user_id).or_else(|| {
                if user_id.0 == username {
                    None
                } else {
                    user_profiles.get(&UserId::new(username.to_string()))
                }
            }) {
                mix_sig_str(&mut sig, &profile.display_name);
                if let Some(asset) = profile.avatar_asset.as_deref() {
                    mix_sig_str(&mut sig, asset);
                } else {
                    mix_sig(&mut sig, 0);
                }
                mix_sig(&mut sig, profile.updated_ms as u64);
            }
        }
    }
    sig
}

fn timeline_emoji_signature(
    conversation_id: Option<&ConversationId>,
    conversation_emojis: &HashMap<
        ConversationId,
        HashMap<String, crate::state::state::EmojiRenderState>,
    >,
    emoji_sources: &HashMap<String, crate::state::state::EmojiRenderState>,
) -> u64 {
    let mut sig = SIG_SEED;
    if let Some(conversation_id) = conversation_id {
        mix_sig_str(&mut sig, &conversation_id.0);
        if let Some(emoji_index) = conversation_emojis.get(conversation_id) {
            let mut aliases = emoji_index.keys().collect::<Vec<_>>();
            aliases.sort_unstable();
            mix_sig(&mut sig, aliases.len() as u64);
            for alias in aliases {
                mix_sig_str(&mut sig, alias);
                if let Some(emoji) = emoji_index.get(alias) {
                    mix_sig_str(&mut sig, &emoji.alias);
                    if let Some(unicode) = emoji.unicode.as_deref() {
                        mix_sig_str(&mut sig, unicode);
                    } else {
                        mix_sig(&mut sig, 0);
                    }
                    if let Some(asset_path) = emoji.asset_path.as_deref() {
                        mix_sig_str(&mut sig, asset_path);
                    } else {
                        mix_sig(&mut sig, 0);
                    }
                    mix_sig(&mut sig, emoji.updated_ms as u64);
                }
            }
        } else {
            mix_sig(&mut sig, 0);
        }
    } else {
        mix_sig(&mut sig, 0);
    }

    let mut source_keys = emoji_sources.keys().collect::<Vec<_>>();
    source_keys.sort_unstable();
    mix_sig(&mut sig, source_keys.len() as u64);
    for source_key in source_keys {
        mix_sig_str(&mut sig, source_key);
        if let Some(emoji) = emoji_sources.get(source_key) {
            mix_sig_str(&mut sig, &emoji.alias);
            if let Some(unicode) = emoji.unicode.as_deref() {
                mix_sig_str(&mut sig, unicode);
            } else {
                mix_sig(&mut sig, 0);
            }
            if let Some(asset_path) = emoji.asset_path.as_deref() {
                mix_sig_str(&mut sig, asset_path);
            } else {
                mix_sig(&mut sig, 0);
            }
            mix_sig(&mut sig, emoji.updated_ms as u64);
        }
    }
    sig
}

fn timeline_reactions_signature(
    conversation_id: Option<&ConversationId>,
    message_reactions: &HashMap<
        ConversationId,
        HashMap<MessageId, Vec<crate::state::state::MessageReactionState>>,
    >,
    user_profiles: &HashMap<UserId, crate::state::state::UserProfileState>,
) -> u64 {
    let mut sig = SIG_SEED;
    let Some(conversation_id) = conversation_id else {
        mix_sig(&mut sig, 0);
        return sig;
    };
    mix_sig_str(&mut sig, &conversation_id.0);
    let Some(reactions_by_message) = message_reactions.get(conversation_id) else {
        mix_sig(&mut sig, 0);
        return sig;
    };

    let mut message_ids = reactions_by_message.keys().collect::<Vec<_>>();
    message_ids.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    mix_sig(&mut sig, message_ids.len() as u64);
    for message_id in message_ids {
        mix_sig_str(&mut sig, &message_id.0);
        let Some(reactions) = reactions_by_message.get(message_id) else {
            mix_sig(&mut sig, 0);
            continue;
        };

        let mut ordered = reactions.iter().collect::<Vec<_>>();
        ordered.sort_unstable_by(|left, right| left.emoji.cmp(&right.emoji));
        mix_sig(&mut sig, ordered.len() as u64);
        for reaction in ordered {
            mix_sig_str(&mut sig, &reaction.emoji);
            if let Some(source_ref) = &reaction.source_ref {
                mix_sig_str(&mut sig, &source_ref.cache_key());
            } else {
                mix_sig(&mut sig, 0);
            }
            let mut actor_ids = reaction.actor_ids.iter().collect::<Vec<_>>();
            actor_ids.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            mix_sig(&mut sig, actor_ids.len() as u64);
            for actor_id in actor_ids {
                mix_sig_str(&mut sig, &actor_id.0);
                if let Some(profile) = user_profiles.get(actor_id).or_else(|| {
                    let lower = actor_id.0.to_ascii_lowercase();
                    if lower == actor_id.0 {
                        None
                    } else {
                        user_profiles.get(&UserId::new(lower))
                    }
                }) {
                    mix_sig_str(&mut sig, &profile.display_name);
                    mix_sig(&mut sig, profile.updated_ms as u64);
                } else {
                    mix_sig(&mut sig, 0);
                }
            }
            mix_sig(&mut sig, reaction.updated_ms as u64);
        }
    }

    sig
}

fn timeline_author_roles_signature(
    conversation_id: Option<&ConversationId>,
    conversation_team_ids: &HashMap<ConversationId, String>,
    team_roles: &HashMap<String, HashMap<UserId, crate::state::event::TeamRoleKind>>,
) -> u64 {
    let mut sig = SIG_SEED;
    let Some(conversation_id) = conversation_id else {
        mix_sig(&mut sig, 0);
        return sig;
    };
    mix_sig_str(&mut sig, &conversation_id.0);
    let Some(team_id) = conversation_team_ids.get(conversation_id) else {
        mix_sig(&mut sig, 0);
        return sig;
    };
    mix_sig_str(&mut sig, team_id);
    let Some(roles_by_user) = team_roles.get(team_id) else {
        mix_sig(&mut sig, 0);
        return sig;
    };
    let mut user_ids = roles_by_user.keys().collect::<Vec<_>>();
    user_ids.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    mix_sig(&mut sig, user_ids.len() as u64);
    for user_id in user_ids {
        mix_sig_str(&mut sig, &user_id.0);
        let Some(role) = roles_by_user.get(user_id) else {
            mix_sig(&mut sig, 0);
            continue;
        };
        let role_value = match role {
            crate::state::event::TeamRoleKind::Member => 1u64,
            crate::state::event::TeamRoleKind::Admin => 2u64,
            crate::state::event::TeamRoleKind::Owner => 3u64,
        };
        mix_sig(&mut sig, role_value);
    }
    sig
}

fn attachment_kind_sig(kind: &crate::domain::attachment::AttachmentKind) -> u64 {
    match kind {
        crate::domain::attachment::AttachmentKind::Image => 1,
        crate::domain::attachment::AttachmentKind::Video => 2,
        crate::domain::attachment::AttachmentKind::Audio => 3,
        crate::domain::attachment::AttachmentKind::File => 4,
    }
}

fn mix_optional_u32(sig: &mut u64, value: Option<u32>) {
    if let Some(value) = value {
        mix_sig(sig, 1);
        mix_sig(sig, value as u64);
    } else {
        mix_sig(sig, 0);
    }
}

fn mix_optional_u64(sig: &mut u64, value: Option<u64>) {
    if let Some(value) = value {
        mix_sig(sig, 1);
        mix_sig(sig, value);
    } else {
        mix_sig(sig, 0);
    }
}

fn mix_optional_str(sig: &mut u64, value: Option<&str>) {
    if let Some(value) = value {
        mix_sig(sig, 1);
        mix_sig_str(sig, value);
    } else {
        mix_sig(sig, 0);
    }
}

fn mix_attachment_source(sig: &mut u64, source: &crate::domain::attachment::AttachmentSource) {
    match source {
        crate::domain::attachment::AttachmentSource::Url(url) => {
            mix_sig(sig, 1);
            mix_sig_str(sig, url);
        }
        crate::domain::attachment::AttachmentSource::LocalPath(path) => {
            mix_sig(sig, 2);
            mix_sig_str(sig, path);
        }
    }
}

fn message_send_state_sig(send_state: &crate::domain::message::MessageSendState) -> u64 {
    match send_state {
        crate::domain::message::MessageSendState::Sent => 1,
        crate::domain::message::MessageSendState::Pending => 2,
        crate::domain::message::MessageSendState::Failed => 3,
    }
}

fn profile_display_name(
    user_id: &UserId,
    user_profiles: &HashMap<UserId, crate::state::state::UserProfileState>,
) -> String {
    user_profiles
        .get(user_id)
        .map(|profile| profile.display_name.clone())
        .unwrap_or_else(|| user_id.0.clone())
}

fn team_name_for_conversation(
    conversation_id: &ConversationId,
    conversation_team_ids: &HashMap<ConversationId, String>,
    channels: &[ConversationSummary],
) -> Option<String> {
    if let Some(from_channels) = channels
        .iter()
        .find(|summary| summary.id == *conversation_id)
        .and_then(|summary| summary.group.as_ref())
        .map(|group| group.id.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return Some(from_channels);
    }

    conversation_team_ids
        .get(conversation_id)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .filter(|value| !looks_like_keybase_team_id(value))
}

fn looks_like_keybase_team_id(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.len() >= 32 && trimmed.bytes().all(|b| (b as char).is_ascii_hexdigit())
}

fn format_chat_event(
    event: &ChatEvent,
    author_id: &UserId,
    user_profiles: &HashMap<UserId, crate::state::state::UserProfileState>,
    team_name: Option<String>,
) -> crate::models::timeline_model::SystemEventRow {
    use crate::models::timeline_model::{EventSpan, SystemEventIcon, SystemEventRow};

    let author = profile_display_name(author_id, user_profiles);

    match event {
        ChatEvent::MemberJoined => SystemEventRow {
            icon: SystemEventIcon::Join,
            spans: vec![
                EventSpan::Actor(author),
                EventSpan::Text("joined the conversation".to_string()),
            ],
        },
        ChatEvent::MemberLeft => SystemEventRow {
            icon: SystemEventIcon::Leave,
            spans: vec![
                EventSpan::Actor(author),
                EventSpan::Text("left the conversation".to_string()),
            ],
        },
        ChatEvent::MembersAdded { user_ids, role } => {
            let role_suffix = role
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .map(|value| format!(" as {value}"))
                .unwrap_or_default();
            let mut spans = vec![EventSpan::Actor(author), EventSpan::Text("added".into())];
            if user_ids.is_empty() {
                spans.push(EventSpan::Text(format!("members{role_suffix}")));
            } else {
                for (i, uid) in user_ids.iter().enumerate() {
                    spans.push(EventSpan::UserLink(uid.clone()));
                    if i + 1 < user_ids.len() {
                        spans.push(EventSpan::Text(",".into()));
                    }
                }
                if !role_suffix.is_empty() {
                    spans.push(EventSpan::Text(role_suffix));
                }
            }
            SystemEventRow {
                icon: SystemEventIcon::Add,
                spans,
            }
        }
        ChatEvent::MembersRemoved { user_ids } => {
            let mut spans = vec![EventSpan::Actor(author)];
            if user_ids.is_empty() {
                spans.push(EventSpan::Text("removed members".into()));
            } else {
                spans.push(EventSpan::Text("removed".into()));
                for (i, uid) in user_ids.iter().enumerate() {
                    spans.push(EventSpan::UserLink(uid.clone()));
                    if i + 1 < user_ids.len() {
                        spans.push(EventSpan::Text(",".into()));
                    }
                }
            }
            SystemEventRow {
                icon: SystemEventIcon::Remove,
                spans,
            }
        }
        ChatEvent::DescriptionChanged { description } => {
            let text = description
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .map(|value| format!("changed the channel description to \"{value}\""))
                .unwrap_or_else(|| "cleared the channel description".to_string());
            SystemEventRow {
                icon: SystemEventIcon::Description,
                spans: vec![EventSpan::Actor(author), EventSpan::Text(text)],
            }
        }
        ChatEvent::ChannelRenamed { new_name } => SystemEventRow {
            icon: SystemEventIcon::Description,
            spans: vec![
                EventSpan::Actor(author),
                EventSpan::Text(format!("set the channel name to #{new_name}")),
            ],
        },
        ChatEvent::AvatarChanged => SystemEventRow {
            icon: SystemEventIcon::Info,
            spans: vec![
                EventSpan::Actor(author),
                EventSpan::Text("changed the avatar".to_string()),
            ],
        },
        ChatEvent::MessagePinned { target_message_id } => {
            let text = if let Some(target_message_id) = target_message_id {
                format!("pinned message {}", target_message_id.0)
            } else {
                "pinned a message".to_string()
            };
            SystemEventRow {
                icon: SystemEventIcon::Pin,
                spans: vec![EventSpan::Actor(author), EventSpan::Text(text)],
            }
        }
        ChatEvent::MessageDeleted { target_message_id } => {
            let text = if let Some(target_message_id) = target_message_id {
                format!("deleted message {}", target_message_id.0)
            } else {
                "deleted a message".to_string()
            };
            SystemEventRow {
                icon: SystemEventIcon::Remove,
                spans: vec![EventSpan::Actor(author), EventSpan::Text(text)],
            }
        }
        ChatEvent::HistoryCleared => SystemEventRow {
            icon: SystemEventIcon::Remove,
            spans: vec![
                EventSpan::Actor(author),
                EventSpan::Text("cleared chat history".to_string()),
            ],
        },
        ChatEvent::RetentionChanged { summary } => SystemEventRow {
            icon: SystemEventIcon::Settings,
            spans: vec![EventSpan::Text(summary.clone())],
        },
        ChatEvent::ChannelCreated {
            channel_name,
            conv_id,
        } => SystemEventRow {
            icon: SystemEventIcon::Add,
            spans: vec![
                EventSpan::Actor(author),
                EventSpan::Text("created".to_string()),
                EventSpan::ChannelLink {
                    channel_name: channel_name.clone(),
                    team_name,
                    conv_id: conv_id.clone(),
                },
            ],
        },
        ChatEvent::Other { text } => SystemEventRow {
            icon: SystemEventIcon::Info,
            spans: vec![EventSpan::Text(text.clone())],
        },
    }
}

fn mix_chat_event_signature(sig: &mut u64, event: &ChatEvent) {
    match event {
        ChatEvent::MemberJoined => mix_sig(sig, 1),
        ChatEvent::MemberLeft => mix_sig(sig, 2),
        ChatEvent::MembersAdded { user_ids, role } => {
            mix_sig(sig, 3);
            mix_sig(sig, user_ids.len() as u64);
            for user_id in user_ids {
                mix_sig_str(sig, &user_id.0);
            }
            if let Some(role) = role {
                mix_sig_str(sig, role);
            } else {
                mix_sig(sig, 0);
            }
        }
        ChatEvent::MembersRemoved { user_ids } => {
            mix_sig(sig, 4);
            mix_sig(sig, user_ids.len() as u64);
            for user_id in user_ids {
                mix_sig_str(sig, &user_id.0);
            }
        }
        ChatEvent::DescriptionChanged { description } => {
            mix_sig(sig, 5);
            if let Some(description) = description {
                mix_sig_str(sig, description);
            } else {
                mix_sig(sig, 0);
            }
        }
        ChatEvent::ChannelRenamed { new_name } => {
            mix_sig(sig, 6);
            mix_sig_str(sig, new_name);
        }
        ChatEvent::AvatarChanged => mix_sig(sig, 7),
        ChatEvent::MessagePinned { target_message_id } => {
            mix_sig(sig, 8);
            if let Some(target_message_id) = target_message_id {
                mix_sig_str(sig, &target_message_id.0);
            } else {
                mix_sig(sig, 0);
            }
        }
        ChatEvent::MessageDeleted { target_message_id } => {
            mix_sig(sig, 9);
            if let Some(target_message_id) = target_message_id {
                mix_sig_str(sig, &target_message_id.0);
            } else {
                mix_sig(sig, 0);
            }
        }
        ChatEvent::HistoryCleared => mix_sig(sig, 10),
        ChatEvent::RetentionChanged { summary } => {
            mix_sig(sig, 11);
            mix_sig_str(sig, summary);
        }
        ChatEvent::ChannelCreated { channel_name, .. } => {
            mix_sig(sig, 13);
            mix_sig_str(sig, channel_name);
        }
        ChatEvent::Other { text } => {
            mix_sig(sig, 12);
            mix_sig_str(sig, text);
        }
    }
}

fn timeline_rows_input_signature(
    conversation_id: Option<&ConversationId>,
    messages: &[crate::domain::message::MessageRecord],
    unread_marker: Option<&MessageId>,
    loading_older: bool,
    user_profiles: &HashMap<UserId, crate::state::state::UserProfileState>,
    user_affinities: &HashMap<UserId, crate::domain::affinity::Affinity>,
    user_presences: &HashMap<UserId, crate::domain::presence::Presence>,
    current_user_id: Option<&UserId>,
    current_user_avatar: Option<&str>,
) -> u64 {
    let mut sig = SIG_SEED;
    if let Some(conversation_id) = conversation_id {
        mix_sig(&mut sig, 1);
        mix_sig_str(&mut sig, &conversation_id.0);
    } else {
        mix_sig(&mut sig, 0);
    }
    if let Some(unread_marker) = unread_marker {
        mix_sig(&mut sig, 1);
        mix_sig_str(&mut sig, &unread_marker.0);
    } else {
        mix_sig(&mut sig, 0);
    }
    mix_sig_bool(&mut sig, loading_older);
    if let Some(current_user_id) = current_user_id {
        mix_sig_str(&mut sig, &current_user_id.0);
    } else {
        mix_sig(&mut sig, 0);
    }
    if let Some(current_user_avatar) = current_user_avatar {
        mix_sig_str(&mut sig, current_user_avatar);
    } else {
        mix_sig(&mut sig, 0);
    }

    mix_sig(&mut sig, messages.len() as u64);
    for message in messages {
        mix_sig_str(&mut sig, &message.id.0);
        mix_sig_str(&mut sig, &message.conversation_id.0);
        mix_sig_str(&mut sig, &message.author_id.0);
        if let Some(timestamp_ms) = message.timestamp_ms {
            mix_sig(&mut sig, 1);
            mix_sig(&mut sig, timestamp_ms as u64);
        } else {
            mix_sig(&mut sig, 0);
        }
        mix_sig_str(&mut sig, &message.permalink);
        mix_sig(&mut sig, message.thread_reply_count as u64);
        mix_sig(&mut sig, message_send_state_sig(&message.send_state));
        if let Some(edited) = &message.edited {
            mix_sig(&mut sig, 1);
            mix_sig_str(&mut sig, &edited.edit_id.0);
            if let Some(edited_at_ms) = edited.edited_at_ms {
                mix_sig(&mut sig, 1);
                mix_sig(&mut sig, edited_at_ms as u64);
            } else {
                mix_sig(&mut sig, 0);
            }
        } else {
            mix_sig(&mut sig, 0);
        }
        if let Some(event) = &message.event {
            mix_sig(&mut sig, 1);
            mix_chat_event_signature(&mut sig, event);
        } else {
            mix_sig(&mut sig, 0);
        }

        mix_sig(&mut sig, message.fragments.len() as u64);
        for fragment in &message.fragments {
            match fragment {
                crate::domain::message::MessageFragment::Text(value) => {
                    mix_sig(&mut sig, 1);
                    mix_sig_str(&mut sig, value);
                }
                crate::domain::message::MessageFragment::InlineCode(value) => {
                    mix_sig(&mut sig, 9);
                    mix_sig_str(&mut sig, value);
                }
                crate::domain::message::MessageFragment::Mention(user_id) => {
                    mix_sig(&mut sig, 2);
                    mix_sig_str(&mut sig, &user_id.0);
                }
                crate::domain::message::MessageFragment::Emoji { alias, source_ref } => {
                    mix_sig(&mut sig, 8);
                    mix_sig_str(&mut sig, alias);
                    if let Some(source_ref) = source_ref {
                        mix_sig_str(&mut sig, &source_ref.cache_key());
                    } else {
                        mix_sig(&mut sig, 0);
                    }
                }
                crate::domain::message::MessageFragment::Code { text: value, .. } => {
                    mix_sig(&mut sig, 3);
                    mix_sig_str(&mut sig, value);
                }
                crate::domain::message::MessageFragment::Quote(value) => {
                    mix_sig(&mut sig, 4);
                    mix_sig_str(&mut sig, value);
                }
                crate::domain::message::MessageFragment::ChannelMention { name } => {
                    mix_sig(&mut sig, 5);
                    mix_sig_str(&mut sig, name);
                }
                crate::domain::message::MessageFragment::BroadcastMention(kind) => {
                    mix_sig(&mut sig, 6);
                    match kind {
                        crate::domain::message::BroadcastKind::Here => mix_sig(&mut sig, 1),
                        crate::domain::message::BroadcastKind::All => mix_sig(&mut sig, 2),
                    }
                }
                crate::domain::message::MessageFragment::Link { url, display } => {
                    mix_sig(&mut sig, 7);
                    mix_sig_str(&mut sig, url);
                    mix_sig_str(&mut sig, display);
                }
            }
        }

        mix_sig(&mut sig, message.attachments.len() as u64);
        for attachment in &message.attachments {
            mix_sig_str(&mut sig, &attachment.name);
            mix_sig(&mut sig, attachment_kind_sig(&attachment.kind));
            mix_sig(&mut sig, attachment.size_bytes);
            mix_optional_str(&mut sig, attachment.mime_type.as_deref());
            mix_optional_u32(&mut sig, attachment.width);
            mix_optional_u32(&mut sig, attachment.height);
            if let Some(preview) = attachment.preview.as_ref() {
                mix_sig(&mut sig, 1);
                mix_attachment_source(&mut sig, &preview.source);
                mix_optional_u32(&mut sig, preview.width);
                mix_optional_u32(&mut sig, preview.height);
            } else {
                mix_sig(&mut sig, 0);
            }
            mix_optional_u64(&mut sig, attachment.duration_ms);
            if let Some(waveform) = attachment.waveform.as_ref() {
                mix_sig(&mut sig, 1);
                mix_sig(&mut sig, waveform.len() as u64);
                for sample in waveform {
                    mix_sig(&mut sig, sample.to_bits() as u64);
                }
            } else {
                mix_sig(&mut sig, 0);
            }
            if let Some(source) = attachment.source.as_ref() {
                mix_sig(&mut sig, 1);
                mix_attachment_source(&mut sig, source);
            } else {
                mix_sig(&mut sig, 0);
            }
        }

        if let Some(profile) = user_profiles.get(&message.author_id).or_else(|| {
            let lower = message.author_id.0.to_ascii_lowercase();
            if lower == message.author_id.0 {
                None
            } else {
                user_profiles.get(&UserId::new(lower))
            }
        }) {
            mix_sig_str(&mut sig, &profile.display_name);
            if let Some(asset) = profile.avatar_asset.as_deref() {
                mix_sig_str(&mut sig, asset);
            } else {
                mix_sig(&mut sig, 0);
            }
            mix_sig(&mut sig, profile.updated_ms as u64);
        } else if current_user_id == Some(&message.author_id) {
            if let Some(asset) = current_user_avatar {
                mix_sig_str(&mut sig, asset);
            } else {
                mix_sig(&mut sig, 0);
            }
        }
        let affinity = user_affinities
            .get(&message.author_id)
            .copied()
            .or_else(|| {
                let lower = message.author_id.0.to_ascii_lowercase();
                if lower == message.author_id.0 {
                    None
                } else {
                    user_affinities.get(&UserId::new(lower)).copied()
                }
            })
            .unwrap_or(crate::domain::affinity::Affinity::None);
        match affinity {
            crate::domain::affinity::Affinity::None => mix_sig(&mut sig, 0),
            crate::domain::affinity::Affinity::Positive => mix_sig(&mut sig, 1),
            crate::domain::affinity::Affinity::Broken => mix_sig(&mut sig, 2),
        }
        let presence = user_presences.get(&message.author_id).or_else(|| {
            let lower = message.author_id.0.to_ascii_lowercase();
            if lower == message.author_id.0 {
                None
            } else {
                user_presences.get(&UserId::new(lower))
            }
        });
        if let Some(presence) = presence {
            match presence.availability {
                crate::domain::presence::Availability::Active => mix_sig(&mut sig, 1),
                crate::domain::presence::Availability::Away => mix_sig(&mut sig, 2),
                crate::domain::presence::Availability::DoNotDisturb => mix_sig(&mut sig, 3),
                crate::domain::presence::Availability::Offline => mix_sig(&mut sig, 4),
                crate::domain::presence::Availability::Unknown => mix_sig(&mut sig, 5),
            }
            if let Some(status_text) = presence.status_text.as_deref() {
                mix_sig_str(&mut sig, status_text);
            } else {
                mix_sig(&mut sig, 0);
            }
        } else {
            mix_sig(&mut sig, 0);
        }
    }
    sig
}

fn timeline_link_previews_signature(
    conversation_id: Option<&ConversationId>,
    messages: &[crate::domain::message::MessageRecord],
) -> u64 {
    let mut sig = SIG_SEED;
    if let Some(conversation_id) = conversation_id {
        mix_sig(&mut sig, 1);
        mix_sig_str(&mut sig, &conversation_id.0);
    } else {
        mix_sig(&mut sig, 0);
    }
    mix_sig(&mut sig, messages.len() as u64);
    for message in messages {
        mix_sig_str(&mut sig, &message.id.0);
        mix_sig(&mut sig, message.link_previews.len() as u64);
        for preview in &message.link_previews {
            mix_sig_str(&mut sig, &preview.url);
            if let Some(video_url) = preview.video_url.as_deref() {
                mix_sig_str(&mut sig, video_url);
            } else {
                mix_sig(&mut sig, 0);
            }
            if let Some(title) = preview.title.as_deref() {
                mix_sig_str(&mut sig, title);
            } else {
                mix_sig(&mut sig, 0);
            }
            if let Some(site) = preview.site.as_deref() {
                mix_sig_str(&mut sig, site);
            } else {
                mix_sig(&mut sig, 0);
            }
            if let Some(description) = preview.description.as_deref() {
                mix_sig_str(&mut sig, description);
            } else {
                mix_sig(&mut sig, 0);
            }
            if let Some(thumbnail_asset) = preview.thumbnail_asset.as_deref() {
                mix_sig_str(&mut sig, thumbnail_asset);
            } else {
                mix_sig(&mut sig, 0);
            }
            mix_sig_bool(&mut sig, preview.is_media);
            if let Some(width) = preview.media_width {
                mix_sig(&mut sig, width as u64);
            } else {
                mix_sig(&mut sig, 0);
            }
            if let Some(height) = preview.media_height {
                mix_sig(&mut sig, height as u64);
            } else {
                mix_sig(&mut sig, 0);
            }
            mix_sig_bool(&mut sig, preview.is_video);
        }
    }
    sig
}

fn link_preview_video_url(preview: &LinkPreview) -> Option<String> {
    if !preview.is_video {
        return None;
    }
    if let Some(video_url) = preview.video_url.as_deref() {
        let trimmed = video_url.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    let fallback = preview.url.trim();
    if fallback.is_empty() {
        return None;
    }
    Some(fallback.to_string())
}

fn apply_og_previews_to_messages(og_service: &OgService, messages: Vec<&mut MessageRecord>) {
    for message in messages {
        let urls = extract_link_urls_from_fragments(&message.fragments);
        if urls.is_empty() {
            continue;
        }
        for url in &urls {
            let Some(Some(og)) = og_service.lookup(url) else {
                continue;
            };
            if let Some(existing) = message
                .link_previews
                .iter_mut()
                .find(|p| urls_match(&p.url, url))
            {
                if existing.title.is_none() {
                    existing.title = og.title.clone();
                }
                if existing.site.is_none() {
                    existing.site = og.site.clone();
                }
                if existing.description.is_none() {
                    existing.description = og.description.clone();
                }
                if existing.thumbnail_asset.is_none() {
                    existing.thumbnail_asset = og.thumbnail_asset.clone();
                }
            } else {
                message.link_previews.push(og.clone());
            }
        }
    }
}

pub(crate) fn urls_match(a: &str, b: &str) -> bool {
    let normalize = |url: &str| {
        let trimmed = url.trim();
        let base = trimmed.split('#').next().unwrap_or(trimmed);
        base.to_ascii_lowercase().trim_end_matches('/').to_string()
    };
    normalize(a) == normalize(b)
}

fn extract_link_urls_from_fragments(fragments: &[MessageFragment]) -> Vec<String> {
    let mut urls = Vec::new();
    for fragment in fragments {
        match fragment {
            MessageFragment::Link { url, .. } => {
                let lower = url.to_ascii_lowercase();
                if lower.starts_with("http://")
                    || lower.starts_with("https://")
                    || (!lower.starts_with("zbase-") && lower.contains('.'))
                {
                    urls.push(url.clone());
                }
            }
            MessageFragment::Text(text) => {
                extract_urls_from_text(text, &mut urls);
            }
            _ => {}
        }
    }
    urls
}

fn extract_urls_from_text(text: &str, out: &mut Vec<String>) {
    let mut search_from = 0;
    while search_from < text.len() {
        let remaining = &text[search_from..];
        let prefix_pos = remaining
            .find("https://")
            .or_else(|| remaining.find("http://"));
        let Some(pos) = prefix_pos else {
            break;
        };
        let url_start = search_from + pos;
        let mut url_end = url_start;
        for ch in text[url_start..].chars() {
            if ch.is_whitespace() {
                break;
            }
            url_end += ch.len_utf8();
        }
        while url_end > url_start
            && matches!(
                text.as_bytes()[url_end - 1],
                b'.' | b',' | b')' | b']' | b';'
            )
        {
            url_end -= 1;
        }
        if url_end > url_start + 8 {
            out.push(text[url_start..url_end].to_string());
        }
        search_from = url_end;
    }
}

pub(crate) fn video_preview_cache_key(url: &str) -> String {
    url.trim().to_ascii_lowercase()
}

fn spawn_video_preview_decode(
    cache_key: String,
    video_url: String,
    sender: Sender<VideoDecodeOutcome>,
) {
    thread::spawn(move || {
        let render_image = fetch_video_render_image_for_url(&video_url);
        let _ = sender.send(VideoDecodeOutcome {
            cache_key,
            render_image,
        });
    });
}

fn fetch_video_render_image_for_url(url: &str) -> Option<Arc<RenderImage>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()?;
    let bytes = runtime.block_on(async {
        let response = reqwest::get(url).await.ok()?.error_for_status().ok()?;
        let payload = response.bytes().await.ok()?;
        Some(payload.to_vec())
    })?;
    decode_video_to_render_image(&bytes)
}

fn timeline_message_timestamp_by_id(
    rows: &[crate::models::timeline_model::TimelineRow],
    message_id: &MessageId,
) -> Option<i64> {
    rows.iter().find_map(|row| match row {
        crate::models::timeline_model::TimelineRow::Message(message_row)
            if &message_row.message.id == message_id =>
        {
            message_row.message.timestamp_ms
        }
        _ => None,
    })
}

fn latest_timeline_message_timestamp(
    rows: &[crate::models::timeline_model::TimelineRow],
) -> Option<i64> {
    rows.iter().rev().find_map(|row| match row {
        crate::models::timeline_model::TimelineRow::Message(message_row) => {
            message_row.message.timestamp_ms
        }
        _ => None,
    })
}

fn latest_timeline_message_id(
    rows: &[crate::models::timeline_model::TimelineRow],
) -> Option<MessageId> {
    rows.iter().rev().find_map(|row| match row {
        crate::models::timeline_model::TimelineRow::Message(message_row) => {
            Some(message_row.message.id.clone())
        }
        _ => None,
    })
}

fn latest_timeline_snapshot_message_id(
    messages: &[crate::domain::message::MessageRecord],
) -> Option<MessageId> {
    let mut latest = None;
    for message in messages {
        if latest
            .as_ref()
            .is_none_or(|current| message_id_is_after(&message.id, current))
        {
            latest = Some(message.id.clone());
        }
    }
    latest
}

fn conversation_id_from_navigated_route(route: &Route) -> Option<ConversationId> {
    match route {
        Route::Channel { channel_id, .. } => Some(ConversationId::new(channel_id.0.clone())),
        Route::DirectMessage { dm_id, .. } => Some(ConversationId::new(dm_id.0.clone())),
        _ => None,
    }
}

fn message_id_is_after(candidate: &MessageId, baseline: &MessageId) -> bool {
    match (
        candidate.0.parse::<u64>().ok(),
        baseline.0.parse::<u64>().ok(),
    ) {
        (Some(candidate), Some(baseline)) => candidate > baseline,
        _ => candidate.0 > baseline.0,
    }
}

fn sidebar_route_unread(
    sections: &[crate::models::sidebar_model::SidebarSection],
    route: &Route,
) -> bool {
    sections
        .iter()
        .flat_map(|section| section.rows.iter())
        .find(|row| &row.route == route)
        .is_some_and(|row| row.unread_count > 0 || row.mention_count > 0)
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn env_nonempty(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_usize(name: &str) -> Option<usize> {
    env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
}

impl Focusable for AppWindow {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

fn route_for_conversation_id(
    workspace_id: &WorkspaceId,
    conversation_id: &ConversationId,
) -> Route {
    Route::Channel {
        workspace_id: workspace_id.clone(),
        channel_id: crate::domain::ids::ChannelId::new(conversation_id.0.clone()),
    }
}

fn timeline_interactions_blocked_by_overlay_state(
    overlay: &crate::models::overlay_model::OverlayModel,
    keybase_inspector_open: bool,
) -> bool {
    overlay.quick_switcher_open
        || overlay.command_palette_open
        || overlay.emoji_picker_open
        || overlay.fullscreen_image.is_some()
        || overlay.file_upload_lightbox.is_some()
        || overlay.profile_card_user_id.is_some()
        || keybase_inspector_open
}

#[cfg(test)]
mod tests {
    use super::{
        latest_timeline_snapshot_message_id, timeline_interactions_blocked_by_overlay_state,
    };
    use crate::domain::{
        ids::{ConversationId, MessageId, UserId},
        message::{ChatEvent, MessageFragment, MessageRecord, MessageSendState},
    };
    use crate::models::overlay_model::OverlayModel;

    fn test_message(id: &str, event: Option<ChatEvent>) -> MessageRecord {
        MessageRecord {
            id: MessageId::new(id),
            conversation_id: ConversationId::new("kb_conv:test"),
            author_id: UserId::new("alice"),
            reply_to: None,
            thread_root_id: None,
            timestamp_ms: None,
            event,
            link_previews: Vec::new(),
            permalink: String::new(),
            fragments: vec![MessageFragment::Text(format!("message-{id}"))],
            source_text: None,
            attachments: Vec::new(),
            reactions: Vec::new(),
            thread_reply_count: 0,
            send_state: MessageSendState::Sent,
            edited: None,
        }
    }

    fn test_overlay() -> OverlayModel {
        OverlayModel {
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
        }
    }

    #[test]
    fn latest_timeline_snapshot_message_id_uses_system_event_messages() {
        let messages = vec![
            test_message(
                "10",
                Some(ChatEvent::MessageDeleted {
                    target_message_id: Some(MessageId::new("5")),
                }),
            ),
            test_message(
                "9",
                Some(ChatEvent::MessagePinned {
                    target_message_id: Some(MessageId::new("7")),
                }),
            ),
        ];

        let latest =
            latest_timeline_snapshot_message_id(&messages).expect("expected latest message id");
        assert_eq!(latest.0, "10");
    }

    #[test]
    fn timeline_interactions_blocked_by_overlay_state_respects_overlay_flags() {
        let overlay = test_overlay();
        assert!(!timeline_interactions_blocked_by_overlay_state(
            &overlay, false
        ));

        let mut quick_switcher_overlay = test_overlay();
        quick_switcher_overlay.quick_switcher_open = true;
        assert!(timeline_interactions_blocked_by_overlay_state(
            &quick_switcher_overlay,
            false
        ));

        let mut palette_overlay = test_overlay();
        palette_overlay.command_palette_open = true;
        assert!(timeline_interactions_blocked_by_overlay_state(
            &palette_overlay,
            false
        ));

        let mut emoji_overlay = test_overlay();
        emoji_overlay.emoji_picker_open = true;
        assert!(timeline_interactions_blocked_by_overlay_state(
            &emoji_overlay,
            false
        ));

        let mut profile_overlay = test_overlay();
        profile_overlay.profile_card_user_id = Some(UserId::new("bob"));
        assert!(timeline_interactions_blocked_by_overlay_state(
            &profile_overlay,
            false
        ));

        let inspector_overlay = test_overlay();
        assert!(timeline_interactions_blocked_by_overlay_state(
            &inspector_overlay,
            true
        ));
    }
}
