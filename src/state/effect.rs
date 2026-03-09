use crate::domain::{
    attachment::AttachmentKind,
    backend::AccountId,
    ids::{CallId, ConversationId, MessageId, WorkspaceId},
    search::SearchFilter,
};

use super::{
    action::DraftKey,
    ids::{ClientMessageId, LocalAttachmentId, OpId, QueryId},
    state::TimelineKey,
};

#[derive(Clone, Debug)]
pub enum SearchScope {
    Global,
    Account(AccountId),
    Workspace(WorkspaceId),
    Conversation(ConversationId),
}

#[derive(Clone, Debug)]
pub struct UploadedAttachment {
    pub local_id: LocalAttachmentId,
    pub name: String,
    pub kind: AttachmentKind,
    pub remote_token: String,
}

#[derive(Clone, Debug)]
pub enum BackendCommand {
    ConnectAccount {
        account_id: AccountId,
    },
    LoadBootstrap {
        account_id: AccountId,
    },
    LoadWorkspace {
        workspace_id: WorkspaceId,
    },
    LoadConversation {
        conversation_id: ConversationId,
    },
    LoadThread {
        conversation_id: ConversationId,
        root_id: MessageId,
    },
    LoadOlderMessages {
        key: TimelineKey,
        cursor: String,
    },
    JumpToMessage {
        conversation_id: ConversationId,
        message_id: MessageId,
    },
    SendMessage {
        op_id: OpId,
        draft_key: DraftKey,
        conversation_id: ConversationId,
        client_message_id: ClientMessageId,
        text: String,
        attachments: Vec<UploadedAttachment>,
        reply_to: Option<MessageId>,
    },
    EditMessage {
        op_id: OpId,
        conversation_id: ConversationId,
        message_id: MessageId,
        text: String,
    },
    DeleteMessage {
        op_id: OpId,
        conversation_id: ConversationId,
        message_id: MessageId,
    },
    ReactToMessage {
        op_id: OpId,
        conversation_id: ConversationId,
        message_id: MessageId,
        reaction: String,
    },
    PinMessage {
        op_id: OpId,
        conversation_id: ConversationId,
        message_id: MessageId,
    },
    UnpinMessage {
        op_id: OpId,
        conversation_id: ConversationId,
    },
    MarkRead {
        conversation_id: ConversationId,
        message_id: Option<MessageId>,
    },
    Search {
        query_id: QueryId,
        scope: SearchScope,
        query: String,
        filters: Vec<SearchFilter>,
    },
    StartCall {
        op_id: OpId,
        conversation_id: ConversationId,
    },
    LeaveCall {
        op_id: OpId,
        call_id: CallId,
    },
}

#[derive(Clone, Debug)]
pub enum Effect {
    Backend(BackendCommand),
    LoadSettings,
    LoadDraft { key: DraftKey },
    PersistDraft { key: DraftKey },
    PersistSettings,
}
