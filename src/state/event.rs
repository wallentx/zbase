use crate::domain::{
    call::{CallStatus, ParticipantSummary},
    conversation::ConversationSummary,
    ids::{CallId, ConversationId, MessageId, UserId, WorkspaceId},
    message::MessageRecord,
    pins::PinnedState,
    presence::Presence,
    search::SearchResult,
};

use super::{
    action::DraftKey,
    bindings::{ConversationBinding, MessageBinding, WorkspaceBinding},
    effect::UploadedAttachment,
    ids::{DebounceKey, LocalAttachmentId, OpId, QueryId},
    state::TimelineKey,
};

#[derive(Clone, Debug)]
pub enum TickEvent {
    Heartbeat,
}

#[derive(Clone, Debug, Default)]
pub struct BootstrapPayload {
    pub workspace_ids: Vec<WorkspaceId>,
    pub active_workspace_id: Option<WorkspaceId>,
    pub workspace_name: String,
    pub channels: Vec<ConversationSummary>,
    pub direct_messages: Vec<ConversationSummary>,
    pub workspace_bindings: Vec<WorkspaceBinding>,
    pub conversation_bindings: Vec<ConversationBinding>,
    pub message_bindings: Vec<MessageBinding>,
    pub selected_conversation_id: Option<ConversationId>,
    pub selected_messages: Vec<MessageRecord>,
    pub unread_marker: Option<MessageId>,
    pub account_display_name: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ConversationPayload {
    pub conversation_id: Option<ConversationId>,
    pub message_ids: Vec<MessageId>,
}

#[derive(Clone, Debug)]
pub struct PresencePatch {
    pub user_id: UserId,
    pub presence: Presence,
}

#[derive(Clone, Debug)]
pub struct ConversationEmojiEntry {
    pub alias: String,
    pub unicode: Option<String>,
    pub asset_path: Option<String>,
    pub updated_ms: i64,
}

#[derive(Clone, Debug)]
pub struct MessageReactionEntry {
    pub emoji: String,
    pub actor_ids: Vec<UserId>,
    pub updated_ms: i64,
}

#[derive(Clone, Debug)]
pub struct MessageReactionsForMessage {
    pub message_id: MessageId,
    pub reactions: Vec<MessageReactionEntry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TeamRoleKind {
    Admin,
    Owner,
}

#[derive(Clone, Debug)]
pub struct TeamRoleEntry {
    pub user_id: UserId,
    pub role: TeamRoleKind,
}

#[derive(Clone, Debug)]
pub struct CallPatch {
    pub call_id: Option<CallId>,
    pub status: CallStatus,
    pub participants: Vec<ParticipantSummary>,
}

#[derive(Clone, Debug)]
pub enum BackendEvent {
    AccountConnected {
        account_id: crate::domain::backend::AccountId,
    },
    AccountDisconnected {
        account_id: crate::domain::backend::AccountId,
        reason: String,
    },
    BootstrapLoaded {
        account_id: crate::domain::backend::AccountId,
        payload: BootstrapPayload,
    },
    WorkspaceConversationsExtended {
        workspace_id: WorkspaceId,
        channels: Vec<ConversationSummary>,
        direct_messages: Vec<ConversationSummary>,
        conversation_bindings: Vec<ConversationBinding>,
    },
    WorkspaceSynced {
        workspace_id: WorkspaceId,
    },
    ConversationLoaded {
        conversation_id: ConversationId,
        payload: ConversationPayload,
    },
    MessagesPrepended {
        key: TimelineKey,
        messages: Vec<MessageRecord>,
        cursor: Option<String>,
    },
    TimelineReplaced {
        conversation_id: ConversationId,
        messages: Vec<MessageRecord>,
        older_cursor: Option<String>,
        newer_cursor: Option<String>,
    },
    MessageUpserted(MessageRecord),
    MessageDeleted {
        conversation_id: ConversationId,
        message_id: MessageId,
    },
    TypingUpdated {
        conversation_id: ConversationId,
        users: Vec<UserId>,
    },
    PresenceUpdated {
        account_id: crate::domain::backend::AccountId,
        users: Vec<PresencePatch>,
    },
    UserProfileUpserted {
        user_id: UserId,
        display_name: String,
        avatar_asset: Option<String>,
        updated_ms: i64,
    },
    ConversationEmojisSynced {
        conversation_id: ConversationId,
        emojis: Vec<ConversationEmojiEntry>,
    },
    MessageReactionsSynced {
        conversation_id: ConversationId,
        reactions_by_message: Vec<MessageReactionsForMessage>,
    },
    PinnedStateUpdated {
        conversation_id: ConversationId,
        pinned: PinnedState,
    },
    MessageReactionApplied {
        conversation_id: ConversationId,
        message_id: MessageId,
        emoji: String,
        actor_id: UserId,
        updated_ms: i64,
    },
    TeamRolesUpdated {
        conversation_id: ConversationId,
        team_id: String,
        roles: Vec<TeamRoleEntry>,
        updated_ms: i64,
    },
    ConversationUnreadChanged {
        conversation_id: ConversationId,
        unread_count: u32,
        mention_count: u32,
        read_upto: Option<MessageId>,
    },
    ReadMarkerUpdated {
        conversation_id: ConversationId,
        read_upto: Option<MessageId>,
    },
    SearchResults {
        query_id: QueryId,
        results: Vec<SearchResult>,
        is_complete: bool,
    },
    CallUpdated(CallPatch),
    KeybaseNotifyStub {
        method: String,
        payload_preview: Option<String>,
    },
}

#[derive(Clone, Debug)]
pub enum EffectResult {
    DraftSaved {
        key: DraftKey,
    },
    DraftSaveFailed {
        key: DraftKey,
        error: String,
    },
    SettingsSaved,
    SettingsSaveFailed {
        error: String,
    },
    UploadProgress {
        op_id: OpId,
        attachment_id: LocalAttachmentId,
        sent_bytes: u64,
        total_bytes: u64,
    },
    UploadFinished {
        op_id: OpId,
        attachment: UploadedAttachment,
    },
    UploadFailed {
        op_id: OpId,
        attachment_id: LocalAttachmentId,
        error: String,
    },
    DebounceElapsed {
        key: DebounceKey,
    },
}

#[derive(Clone, Debug)]
pub enum AppEvent {
    Ui(super::action::UiAction),
    Backend(BackendEvent),
    EffectResult(EffectResult),
    Tick(TickEvent),
}
