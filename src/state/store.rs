use crate::domain::ids::{ConversationId, MessageId, WorkspaceId};

use super::{
    action::UiAction,
    bindings::{ConversationBinding, MessageBinding, WorkspaceBinding},
    event::{AppEvent, BackendEvent},
    reducer::reduce,
    state::{AccountState, UiState},
};

pub struct AppStore {
    state: UiState,
}

impl AppStore {
    pub fn new(state: UiState) -> Self {
        Self { state }
    }

    pub fn dispatch(&mut self, event: AppEvent) -> Vec<super::effect::Effect> {
        reduce(&mut self.state, event).effects
    }

    pub fn dispatch_ui(&mut self, action: UiAction) -> Vec<super::effect::Effect> {
        self.dispatch(AppEvent::Ui(action))
    }

    pub fn dispatch_backend(&mut self, event: BackendEvent) -> Vec<super::effect::Effect> {
        self.dispatch(AppEvent::Backend(event))
    }

    pub fn snapshot(&self) -> &UiState {
        &self.state
    }

    pub fn register_account(&mut self, account: AccountState) {
        self.state
            .backend
            .accounts
            .insert(account.account_id.clone(), account);
    }

    pub fn register_workspace_binding(&mut self, binding: WorkspaceBinding) {
        self.state
            .backend
            .workspace_bindings
            .insert(binding.workspace_id.clone(), binding);
    }

    pub fn register_conversation_binding(&mut self, binding: ConversationBinding) {
        self.state
            .backend
            .conversation_bindings
            .insert(binding.conversation_id.clone(), binding);
    }

    pub fn register_message_binding(&mut self, binding: MessageBinding) {
        self.state
            .backend
            .message_bindings
            .insert(binding.message_id.clone(), binding);
    }

    #[allow(dead_code)]
    pub fn binding_for_workspace(&self, workspace_id: &WorkspaceId) -> Option<&WorkspaceBinding> {
        self.state.backend.workspace_bindings.get(workspace_id)
    }

    #[allow(dead_code)]
    pub fn binding_for_conversation(
        &self,
        conversation_id: &ConversationId,
    ) -> Option<&ConversationBinding> {
        self.state
            .backend
            .conversation_bindings
            .get(conversation_id)
    }

    #[allow(dead_code)]
    pub fn binding_for_message(&self, message_id: &MessageId) -> Option<&MessageBinding> {
        self.state.backend.message_bindings.get(message_id)
    }
}
