use crate::domain::{
    backend::{
        AccountId, BackendId, ProviderConversationRef, ProviderMessageRef, ProviderWorkspaceRef,
    },
    ids::{ConversationId, MessageId, WorkspaceId},
};

#[derive(Clone, Debug)]
pub struct WorkspaceBinding {
    pub workspace_id: WorkspaceId,
    pub backend_id: BackendId,
    pub account_id: AccountId,
    pub provider_workspace_ref: ProviderWorkspaceRef,
}

#[derive(Clone, Debug)]
pub struct ConversationBinding {
    pub conversation_id: ConversationId,
    pub backend_id: BackendId,
    pub account_id: AccountId,
    pub provider_conversation_ref: ProviderConversationRef,
}

#[derive(Clone, Debug)]
pub struct MessageBinding {
    pub message_id: MessageId,
    pub backend_id: BackendId,
    pub account_id: AccountId,
    pub provider_message_ref: ProviderMessageRef,
}
