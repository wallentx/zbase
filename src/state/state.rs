use std::collections::HashMap;

use crate::domain::{
    backend::{AccountId, BackendCapabilities, BackendId},
    call::CallStatus,
    conversation::ConversationSummary,
    ids::{CallId, ConversationId, MessageId, SidebarSectionId, UserId, WorkspaceId},
    message::MessageRecord,
    pins::PinnedState,
    route::Route,
    search::{SearchFilter, SearchResult},
    user::UserSummary,
};

use super::{
    action::DraftKey,
    bindings::{ConversationBinding, MessageBinding, WorkspaceBinding},
    event::TeamRoleKind,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootPhase {
    Launching,
    HydratingLocalState,
    ConnectingBackend,
    Ready,
    Degraded,
    FatalError,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    Connected,
    Disconnected,
}

#[derive(Clone, Debug)]
pub enum TimelineKey {
    Conversation(ConversationId),
    Thread {
        conversation_id: ConversationId,
        root_id: MessageId,
    },
}

#[derive(Clone, Debug)]
pub struct AppState {
    pub boot_phase: BootPhase,
}

#[derive(Clone, Debug, Default)]
pub struct NavigationState {
    pub current_route: Option<Route>,
    pub active_thread_root: Option<MessageId>,
}

#[derive(Clone, Debug, Default)]
pub struct DraftState {
    pub text: String,
}

#[derive(Clone, Debug, Default)]
pub struct UiWorkspaceState {
    pub active_workspace_id: Option<WorkspaceId>,
    pub workspace_name: String,
    pub channels: Vec<ConversationSummary>,
    pub direct_messages: Vec<ConversationSummary>,
}

#[derive(Clone, Debug, Default)]
pub struct UiSidebarRowState {
    pub label: String,
    pub unread_count: u32,
    pub mention_count: u32,
    pub route: Option<Route>,
}

#[derive(Clone, Debug, Default)]
pub struct UiSidebarSectionState {
    pub id: Option<SidebarSectionId>,
    pub title: String,
    pub rows: Vec<UiSidebarRowState>,
    pub collapsed: bool,
}

#[derive(Clone, Debug, Default)]
pub struct UiSidebarState {
    pub sections: Vec<UiSidebarSectionState>,
    pub filter: String,
    pub highlighted_route: Option<Route>,
}

#[derive(Clone, Debug, Default)]
pub struct UiTimelineState {
    pub conversation_id: Option<ConversationId>,
    pub messages: Vec<MessageRecord>,
    pub typing_text: Option<String>,
    pub highlighted_message_id: Option<MessageId>,
    pub unread_marker: Option<MessageId>,
    pub older_cursor: Option<String>,
    pub newer_cursor: Option<String>,
    pub loading_older: bool,
}

#[derive(Clone, Debug, Default)]
pub struct UiThreadState {
    pub open: bool,
    pub root_message_id: Option<MessageId>,
    pub replies: Vec<MessageRecord>,
    pub reply_draft: String,
    pub loading: bool,
}

#[derive(Clone, Debug, Default)]
pub struct UiSearchState {
    pub query: String,
    pub filters: Vec<SearchFilter>,
    pub results: Vec<SearchResult>,
    pub highlighted_index: Option<usize>,
    pub is_loading: bool,
}

#[derive(Clone, Debug, Default)]
pub struct UiOverlayState {
    pub quick_switcher_open: bool,
    pub command_palette_open: bool,
    pub emoji_picker_open: bool,
    pub active_modal: Option<String>,
    pub active_context_menu: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct UiCallState {
    pub active_call_id: Option<CallId>,
    pub call_status: Option<CallStatus>,
    pub participants: Vec<UserSummary>,
    pub is_muted: bool,
    pub is_sharing_screen: bool,
}

#[derive(Clone, Debug, Default)]
pub struct UiNotificationsState {
    pub toasts: Vec<String>,
    pub notification_center_count: u32,
}

#[derive(Clone, Debug)]
pub struct AccountState {
    pub account_id: AccountId,
    pub backend_id: BackendId,
    pub display_name: String,
    pub avatar: Option<String>,
    pub connection_state: ConnectionState,
    pub capabilities: BackendCapabilities,
}

#[derive(Clone, Debug, Default)]
pub struct BackendRuntimeState {
    pub accounts: HashMap<AccountId, AccountState>,
    pub user_profiles: HashMap<UserId, UserProfileState>,
    pub conversation_emojis: HashMap<ConversationId, HashMap<String, EmojiRenderState>>,
    pub message_reactions: HashMap<ConversationId, HashMap<MessageId, Vec<MessageReactionState>>>,
    pub conversation_pins: HashMap<ConversationId, PinnedState>,
    pub conversation_team_ids: HashMap<ConversationId, String>,
    pub team_roles: HashMap<String, HashMap<UserId, TeamRoleKind>>,
    pub workspace_bindings: HashMap<crate::domain::ids::WorkspaceId, WorkspaceBinding>,
    pub conversation_bindings: HashMap<ConversationId, ConversationBinding>,
    pub message_bindings: HashMap<MessageId, MessageBinding>,
}

#[derive(Clone, Debug)]
pub struct UserProfileState {
    pub display_name: String,
    pub avatar_asset: Option<String>,
    pub updated_ms: i64,
}

#[derive(Clone, Debug)]
pub struct EmojiRenderState {
    pub alias: String,
    pub unicode: Option<String>,
    pub asset_path: Option<String>,
    pub updated_ms: i64,
}

#[derive(Clone, Debug)]
pub struct MessageReactionState {
    pub emoji: String,
    pub actor_ids: Vec<UserId>,
    pub updated_ms: i64,
}

#[derive(Clone, Debug)]
pub struct UiState {
    pub app: AppState,
    pub navigation: NavigationState,
    pub drafts: HashMap<DraftKey, DraftState>,
    pub backend: BackendRuntimeState,
    pub workspace: UiWorkspaceState,
    pub sidebar: UiSidebarState,
    pub timeline: UiTimelineState,
    pub thread: UiThreadState,
    pub search: UiSearchState,
    pub overlay: UiOverlayState,
    pub call: UiCallState,
    pub notifications: UiNotificationsState,
    pub search_query: String,
    pub sidebar_filter: String,
    pub next_op_seq: u64,
}

impl UiState {
    pub fn with_route(route: Route) -> Self {
        let mut state = Self::default();
        state.navigation.current_route = Some(route);
        state
    }
}

impl Default for UiState {
    fn default() -> Self {
        Self {
            app: AppState {
                boot_phase: BootPhase::Launching,
            },
            navigation: NavigationState::default(),
            drafts: HashMap::new(),
            backend: BackendRuntimeState::default(),
            workspace: UiWorkspaceState::default(),
            sidebar: UiSidebarState::default(),
            timeline: UiTimelineState::default(),
            thread: UiThreadState::default(),
            search: UiSearchState::default(),
            overlay: UiOverlayState::default(),
            call: UiCallState::default(),
            notifications: UiNotificationsState::default(),
            search_query: String::new(),
            sidebar_filter: String::new(),
            next_op_seq: 1,
        }
    }
}
