use crate::{
    domain::{
        backend::{
            AccountId, BackendCapabilities, BackendId, ProviderConversationRef, ProviderMessageRef,
            ProviderWorkspaceRef,
        },
        ids::{CallId, ConversationId, WorkspaceId},
        search::SearchFilter,
    },
    state::{
        effect::UploadedAttachment,
        event::BackendEvent,
        ids::{ClientMessageId, OpId, QueryId},
    },
};

#[derive(Clone, Debug)]
pub enum BackendError {
    MissingAccount(AccountId),
    MissingWorkspaceBinding(String),
    MissingConversationBinding(String),
    MissingMessageBinding(String),
    MissingBackend(BackendId),
}

#[derive(Clone, Debug)]
pub enum RoutedBackendCommand {
    LoadBootstrap {
        account_id: AccountId,
    },
    LoadWorkspace {
        account_id: AccountId,
        workspace: ProviderWorkspaceRef,
    },
    LoadConversation {
        account_id: AccountId,
        conversation: ProviderConversationRef,
    },
    LoadThread {
        account_id: AccountId,
        conversation: ProviderConversationRef,
        root_message: ProviderMessageRef,
    },
    LoadOlderMessages {
        account_id: AccountId,
        conversation: ProviderConversationRef,
        cursor: String,
    },
    JumpToMessage {
        account_id: AccountId,
        conversation: ProviderConversationRef,
        message_id: String,
    },
    SendMessage {
        op_id: OpId,
        account_id: AccountId,
        conversation: ProviderConversationRef,
        client_message_id: ClientMessageId,
        text: String,
        attachments: Vec<UploadedAttachment>,
        reply_to: Option<ProviderMessageRef>,
    },
    EditMessage {
        op_id: OpId,
        account_id: AccountId,
        conversation: ProviderConversationRef,
        message: ProviderMessageRef,
        text: String,
    },
    DeleteMessage {
        op_id: OpId,
        account_id: AccountId,
        conversation: ProviderConversationRef,
        message: ProviderMessageRef,
    },
    ReactToMessage {
        op_id: OpId,
        account_id: AccountId,
        conversation: ProviderConversationRef,
        message: ProviderMessageRef,
        reaction: String,
    },
    PinMessage {
        op_id: OpId,
        account_id: AccountId,
        conversation: ProviderConversationRef,
        message: ProviderMessageRef,
    },
    UnpinMessage {
        op_id: OpId,
        account_id: AccountId,
        conversation: ProviderConversationRef,
    },
    MarkRead {
        account_id: AccountId,
        conversation: ProviderConversationRef,
        message: Option<ProviderMessageRef>,
    },
    Search {
        account_id: Option<AccountId>,
        workspace_id: Option<WorkspaceId>,
        conversation_id: Option<ConversationId>,
        query_id: QueryId,
        query: String,
        filters: Vec<SearchFilter>,
    },
    StartCall {
        op_id: OpId,
        account_id: AccountId,
        conversation: ProviderConversationRef,
    },
    LeaveCall {
        op_id: OpId,
        account_id: AccountId,
        call_id: CallId,
    },
}

pub trait ChatBackend: Send {
    fn backend_id(&self) -> BackendId;
    fn capabilities(&self) -> BackendCapabilities;
    fn connect_account(
        &mut self,
        account_id: &AccountId,
    ) -> Result<Vec<BackendEvent>, BackendError>;
    fn execute(&mut self, cmd: RoutedBackendCommand) -> Result<Vec<BackendEvent>, BackendError>;
    fn poll_events(&mut self) -> Vec<BackendEvent> {
        Vec::new()
    }
}
