use std::collections::HashMap;

use crate::{
    domain::{
        backend::{AccountId, BackendId, ProviderConversationRef, ProviderMessageRef},
        ids::{ConversationId, MessageId, WorkspaceId},
    },
    state::{
        bindings::{ConversationBinding, MessageBinding, WorkspaceBinding},
        effect::{BackendCommand, Effect, SearchScope},
        event::BackendEvent,
        ids::QueryId,
        state::TimelineKey,
    },
};

use super::traits::{BackendError, ChatBackend, RoutedBackendCommand};

#[derive(Default)]
pub struct BackendRouter {
    backends: HashMap<BackendId, Box<dyn ChatBackend>>,
    accounts: HashMap<AccountId, BackendId>,
    workspace_bindings: HashMap<WorkspaceId, WorkspaceBinding>,
    conversation_bindings: HashMap<ConversationId, ConversationBinding>,
    message_bindings: HashMap<MessageId, MessageBinding>,
}

impl BackendRouter {
    pub fn register_backend(&mut self, backend: Box<dyn ChatBackend>) {
        let backend_id = backend.backend_id();
        self.backends.insert(backend_id, backend);
    }

    pub fn register_account(&mut self, account_id: AccountId, backend_id: BackendId) {
        self.accounts.insert(account_id, backend_id);
    }

    pub fn register_workspace_binding(&mut self, binding: WorkspaceBinding) {
        self.workspace_bindings
            .insert(binding.workspace_id.clone(), binding);
    }

    pub fn register_conversation_binding(&mut self, binding: ConversationBinding) {
        self.conversation_bindings
            .insert(binding.conversation_id.clone(), binding);
    }

    pub fn register_message_binding(&mut self, binding: MessageBinding) {
        self.message_bindings
            .insert(binding.message_id.clone(), binding);
    }

    pub fn apply_effects(&mut self, effects: &[Effect]) -> Result<Vec<BackendEvent>, BackendError> {
        let mut events = Vec::new();

        for effect in effects {
            let Effect::Backend(command) = effect else {
                continue;
            };
            events.extend(self.route_command(command.clone())?);
        }

        Ok(events)
    }

    pub fn poll_backends(&mut self) -> Vec<BackendEvent> {
        let mut events = Vec::new();
        for backend in self.backends.values_mut() {
            events.extend(backend.poll_events());
        }
        events
    }

    pub fn route_command(
        &mut self,
        command: BackendCommand,
    ) -> Result<Vec<BackendEvent>, BackendError> {
        match command {
            BackendCommand::ConnectAccount { account_id } => {
                let backend = self.backend_for_account(&account_id)?;
                backend.connect_account(&account_id)
            }
            BackendCommand::LoadBootstrap { account_id } => {
                let backend = self.backend_for_account(&account_id)?;
                backend.execute(RoutedBackendCommand::LoadBootstrap { account_id })
            }
            BackendCommand::LoadWorkspace { workspace_id } => {
                let binding = self
                    .workspace_bindings
                    .get(&workspace_id)
                    .cloned()
                    .ok_or_else(|| BackendError::MissingWorkspaceBinding(workspace_id.0.clone()))?;
                let backend = self.backend_for_binding(&binding.backend_id)?;
                backend.execute(RoutedBackendCommand::LoadWorkspace {
                    account_id: binding.account_id,
                    workspace: binding.provider_workspace_ref,
                })
            }
            BackendCommand::LoadConversation { conversation_id } => {
                let binding = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let backend = self.backend_for_binding(&binding.backend_id)?;
                backend.execute(RoutedBackendCommand::LoadConversation {
                    account_id: binding.account_id,
                    conversation: binding.provider_conversation_ref,
                })
            }
            BackendCommand::LoadConversationMembers { conversation_id } => {
                let binding = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let backend = self.backend_for_binding(&binding.backend_id)?;
                backend.execute(RoutedBackendCommand::LoadConversationMembers {
                    account_id: binding.account_id,
                    conversation: binding.provider_conversation_ref,
                })
            }
            BackendCommand::LoadThread {
                conversation_id,
                root_id,
            } => {
                let conversation = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let message = self
                    .message_bindings
                    .get(&root_id)
                    .cloned()
                    .ok_or_else(|| BackendError::MissingMessageBinding(root_id.0.clone()))?;
                let backend = self.backend_for_binding(&conversation.backend_id)?;
                backend.execute(RoutedBackendCommand::LoadThread {
                    account_id: conversation.account_id,
                    conversation: conversation.provider_conversation_ref,
                    root_message: message.provider_message_ref,
                })
            }
            BackendCommand::LoadOlderMessages { key, cursor } => {
                let conversation_id = match key {
                    TimelineKey::Conversation(conversation_id)
                    | TimelineKey::Thread {
                        conversation_id, ..
                    } => conversation_id,
                };
                let binding = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let backend = self.backend_for_binding(&binding.backend_id)?;
                backend.execute(RoutedBackendCommand::LoadOlderMessages {
                    account_id: binding.account_id,
                    conversation: binding.provider_conversation_ref,
                    cursor,
                })
            }
            BackendCommand::JumpToMessage {
                conversation_id,
                message_id,
            } => {
                let binding = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let backend = self.backend_for_binding(&binding.backend_id)?;
                backend.execute(RoutedBackendCommand::JumpToMessage {
                    account_id: binding.account_id,
                    conversation: binding.provider_conversation_ref,
                    message_id: message_id.0,
                })
            }
            BackendCommand::SendMessage {
                op_id,
                draft_key: _,
                conversation_id,
                client_message_id,
                text,
                attachments,
                reply_to,
            } => {
                let binding = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let reply_ref = reply_to
                    .as_ref()
                    .and_then(|message_id| self.message_bindings.get(message_id))
                    .map(|binding| binding.provider_message_ref.clone())
                    .or_else(|| {
                        reply_to
                            .as_ref()
                            .map(|message_id| ProviderMessageRef::new(message_id.0.clone()))
                    });
                let backend = self.backend_for_binding(&binding.backend_id)?;
                backend.execute(RoutedBackendCommand::SendMessage {
                    op_id,
                    account_id: binding.account_id,
                    conversation: binding.provider_conversation_ref,
                    client_message_id,
                    text,
                    attachments,
                    reply_to: reply_ref,
                })
            }
            BackendCommand::SendAttachment {
                op_id,
                draft_key: _,
                conversation_id,
                client_message_id,
                local_path,
                filename,
                caption,
            } => {
                let binding = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let backend = self.backend_for_binding(&binding.backend_id)?;
                backend.execute(RoutedBackendCommand::SendAttachment {
                    op_id,
                    account_id: binding.account_id,
                    conversation: binding.provider_conversation_ref,
                    client_message_id,
                    local_path,
                    filename,
                    caption,
                })
            }
            BackendCommand::EditMessage {
                op_id,
                conversation_id,
                message_id,
                text,
            } => {
                let conversation = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let message = self
                    .message_bindings
                    .get(&message_id)
                    .cloned()
                    .ok_or_else(|| BackendError::MissingMessageBinding(message_id.0.clone()))?;
                let backend = self.backend_for_binding(&conversation.backend_id)?;
                backend.execute(RoutedBackendCommand::EditMessage {
                    op_id,
                    account_id: conversation.account_id,
                    conversation: conversation.provider_conversation_ref,
                    message: message.provider_message_ref,
                    text,
                })
            }
            BackendCommand::DeleteMessage {
                op_id,
                conversation_id,
                message_id,
            } => {
                let conversation = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let message = self
                    .message_bindings
                    .get(&message_id)
                    .cloned()
                    .ok_or_else(|| BackendError::MissingMessageBinding(message_id.0.clone()))?;
                let backend = self.backend_for_binding(&conversation.backend_id)?;
                backend.execute(RoutedBackendCommand::DeleteMessage {
                    op_id,
                    account_id: conversation.account_id,
                    conversation: conversation.provider_conversation_ref,
                    message: message.provider_message_ref,
                })
            }
            BackendCommand::ReactToMessage {
                op_id,
                conversation_id,
                message_id,
                reaction,
            } => {
                let conversation = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let message = self
                    .message_bindings
                    .get(&message_id)
                    .cloned()
                    .ok_or_else(|| BackendError::MissingMessageBinding(message_id.0.clone()))?;
                let backend = self.backend_for_binding(&conversation.backend_id)?;
                backend.execute(RoutedBackendCommand::ReactToMessage {
                    op_id,
                    account_id: conversation.account_id,
                    conversation: conversation.provider_conversation_ref,
                    message: message.provider_message_ref,
                    reaction,
                })
            }
            BackendCommand::PinMessage {
                op_id,
                conversation_id,
                message_id,
            } => {
                let conversation = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let message = self
                    .message_bindings
                    .get(&message_id)
                    .cloned()
                    .ok_or_else(|| BackendError::MissingMessageBinding(message_id.0.clone()))?;
                let backend = self.backend_for_binding(&conversation.backend_id)?;
                backend.execute(RoutedBackendCommand::PinMessage {
                    op_id,
                    account_id: conversation.account_id,
                    conversation: conversation.provider_conversation_ref,
                    message: message.provider_message_ref,
                })
            }
            BackendCommand::UnpinMessage {
                op_id,
                conversation_id,
            } => {
                let conversation = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let backend = self.backend_for_binding(&conversation.backend_id)?;
                backend.execute(RoutedBackendCommand::UnpinMessage {
                    op_id,
                    account_id: conversation.account_id,
                    conversation: conversation.provider_conversation_ref,
                })
            }
            BackendCommand::MarkRead {
                conversation_id,
                message_id,
            } => {
                let conversation = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let message = message_id
                    .as_ref()
                    .and_then(|message_id| self.message_bindings.get(message_id))
                    .map(|binding| binding.provider_message_ref.clone())
                    .or_else(|| {
                        message_id
                            .as_ref()
                            .map(|id| ProviderMessageRef::new(id.0.clone()))
                    });
                let backend = self.backend_for_binding(&conversation.backend_id)?;
                backend.execute(RoutedBackendCommand::MarkRead {
                    account_id: conversation.account_id,
                    conversation: conversation.provider_conversation_ref,
                    message,
                })
            }
            BackendCommand::Search {
                query_id,
                scope,
                query,
                filters,
            } => self.route_search(query_id, scope, query, filters),
            BackendCommand::SearchUsers { query_id, query } => {
                let (account_id, backend_id) = self.default_account_backend()?;
                let backend = self.backend_for_binding(&backend_id)?;
                backend.execute(RoutedBackendCommand::SearchUsers {
                    account_id,
                    query_id,
                    query,
                })
            }
            BackendCommand::CreateConversation {
                op_id,
                participants,
                kind,
            } => {
                let (account_id, backend_id) = self.default_account_backend()?;
                let workspace_binding = self
                    .default_workspace_binding_for_account(&account_id, &backend_id)?
                    .clone();
                let participant_ids = participants
                    .into_iter()
                    .map(|participant| participant.0)
                    .collect::<Vec<_>>();
                let events = {
                    let backend = self.backend_for_binding(&backend_id)?;
                    backend.execute(RoutedBackendCommand::CreateConversation {
                        op_id,
                        account_id,
                        workspace: workspace_binding.provider_workspace_ref,
                        participants: participant_ids,
                        kind,
                    })?
                };
                for event in &events {
                    if let BackendEvent::ConversationCreated {
                        conversation_binding,
                        ..
                    } = event
                    {
                        self.register_conversation_binding(conversation_binding.clone());
                    }
                }
                Ok(events)
            }
            BackendCommand::StartCall {
                op_id,
                conversation_id,
            } => {
                let binding = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let backend = self.backend_for_binding(&binding.backend_id)?;
                backend.execute(RoutedBackendCommand::StartCall {
                    op_id,
                    account_id: binding.account_id,
                    conversation: binding.provider_conversation_ref,
                })
            }
            BackendCommand::LeaveCall { op_id, call_id } => {
                let (account_id, backend_id) = self.default_account_backend()?;
                let backend = self.backend_for_binding(&backend_id)?;
                backend.execute(RoutedBackendCommand::LeaveCall {
                    op_id,
                    account_id,
                    call_id,
                })
            }
            BackendCommand::LoadUserProfile { user_id } => {
                let (account_id, backend_id) = self.default_account_backend()?;
                let backend = self.backend_for_binding(&backend_id)?;
                backend.execute(RoutedBackendCommand::LoadUserProfile {
                    account_id,
                    user_id,
                })
            }
            BackendCommand::RefreshParticipants {
                user_id,
                conversation_id,
            } => {
                let (account_id, backend_id) = self.default_account_backend()?;
                let backend = self.backend_for_binding(&backend_id)?;
                backend.execute(RoutedBackendCommand::RefreshParticipants {
                    account_id,
                    user_id,
                    conversation_id,
                })
            }
            BackendCommand::LoadSocialGraphList { user_id, list_type } => {
                let (account_id, backend_id) = self.default_account_backend()?;
                let backend = self.backend_for_binding(&backend_id)?;
                backend.execute(RoutedBackendCommand::LoadSocialGraphList {
                    account_id,
                    user_id,
                    list_type,
                })
            }
            BackendCommand::FollowUser { user_id } => {
                let (account_id, backend_id) = self.default_account_backend()?;
                let backend = self.backend_for_binding(&backend_id)?;
                backend.execute(RoutedBackendCommand::FollowUser {
                    account_id,
                    user_id,
                })
            }
            BackendCommand::UnfollowUser { user_id } => {
                let (account_id, backend_id) = self.default_account_backend()?;
                let backend = self.backend_for_binding(&backend_id)?;
                backend.execute(RoutedBackendCommand::UnfollowUser {
                    account_id,
                    user_id,
                })
            }
            BackendCommand::ResolveChannel {
                workspace_id,
                team_name,
                channel_name,
            } => {
                let (account_id, backend_id, provider_workspace_ref) = if let Some(binding) =
                    self.workspace_bindings.get(&workspace_id).cloned()
                {
                    (
                        binding.account_id,
                        binding.backend_id,
                        binding.provider_workspace_ref,
                    )
                } else {
                    let (account_id, backend_id) = self.default_account_backend()?;
                    (
                        account_id,
                        backend_id,
                        crate::domain::backend::ProviderWorkspaceRef::new(workspace_id.0.clone()),
                    )
                };
                let backend = self.backend_for_binding(&backend_id)?;
                backend.execute(RoutedBackendCommand::ResolveChannel {
                    account_id,
                    workspace: provider_workspace_ref,
                    team_name,
                    channel_name,
                    workspace_id,
                })
            }
            BackendCommand::ResolveChannelById {
                workspace_id,
                conversation_id,
            } => {
                let (account_id, backend_id) = if let Some(binding) =
                    self.conversation_bindings.get(&conversation_id).cloned()
                {
                    (binding.account_id, binding.backend_id)
                } else {
                    self.default_account_backend()?
                };
                let provider_ref = ProviderConversationRef::new(conversation_id.0.clone());
                let backend = self.backend_for_binding(&backend_id)?;
                backend.execute(RoutedBackendCommand::ResolveChannelById {
                    account_id,
                    conversation: provider_ref,
                    workspace_id,
                })
            }
            BackendCommand::JoinChannel {
                workspace_id,
                conversation_id,
            } => {
                let binding = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let backend = self.backend_for_binding(&binding.backend_id)?;
                backend.execute(RoutedBackendCommand::JoinChannel {
                    account_id: binding.account_id,
                    conversation: binding.provider_conversation_ref,
                    workspace_id,
                })
            }
        }
    }

    fn route_search(
        &mut self,
        query_id: QueryId,
        scope: SearchScope,
        query: String,
        filters: Vec<crate::domain::search::SearchFilter>,
    ) -> Result<Vec<BackendEvent>, BackendError> {
        match scope {
            SearchScope::Global => {
                let mut events = Vec::new();
                for backend in self.backends.values_mut() {
                    events.extend(backend.execute(RoutedBackendCommand::Search {
                        account_id: None,
                        workspace_id: None,
                        conversation_id: None,
                        query_id: query_id.clone(),
                        query: query.clone(),
                        filters: filters.clone(),
                    })?);
                }
                Ok(events)
            }
            SearchScope::Account(account_id) => {
                let backend = self.backend_for_account(&account_id)?;
                backend.execute(RoutedBackendCommand::Search {
                    account_id: Some(account_id),
                    workspace_id: None,
                    conversation_id: None,
                    query_id,
                    query,
                    filters,
                })
            }
            SearchScope::Workspace(workspace_id) => {
                let routed_workspace_id = workspace_id.clone();
                let binding = self
                    .workspace_bindings
                    .get(&workspace_id)
                    .cloned()
                    .ok_or_else(|| BackendError::MissingWorkspaceBinding(workspace_id.0.clone()))?;
                let backend = self.backend_for_binding(&binding.backend_id)?;
                backend.execute(RoutedBackendCommand::Search {
                    account_id: Some(binding.account_id),
                    workspace_id: Some(routed_workspace_id),
                    conversation_id: None,
                    query_id,
                    query,
                    filters,
                })
            }
            SearchScope::Conversation(conversation_id) => {
                let routed_conversation_id = conversation_id.clone();
                let binding = self
                    .conversation_bindings
                    .get(&conversation_id)
                    .cloned()
                    .ok_or_else(|| {
                        BackendError::MissingConversationBinding(conversation_id.0.clone())
                    })?;
                let backend = self.backend_for_binding(&binding.backend_id)?;
                backend.execute(RoutedBackendCommand::Search {
                    account_id: Some(binding.account_id),
                    workspace_id: None,
                    conversation_id: Some(routed_conversation_id),
                    query_id,
                    query,
                    filters,
                })
            }
        }
    }

    fn backend_for_account(
        &mut self,
        account_id: &AccountId,
    ) -> Result<&mut (dyn ChatBackend + '_), BackendError> {
        let backend_id = self
            .accounts
            .get(account_id)
            .cloned()
            .ok_or_else(|| BackendError::MissingAccount(account_id.clone()))?;
        self.backend_for_binding(&backend_id)
    }

    fn backend_for_binding(
        &mut self,
        backend_id: &BackendId,
    ) -> Result<&mut (dyn ChatBackend + '_), BackendError> {
        match self.backends.get_mut(backend_id) {
            Some(backend) => Ok(backend.as_mut()),
            None => Err(BackendError::MissingBackend(backend_id.clone())),
        }
    }

    fn default_account_backend(&self) -> Result<(AccountId, BackendId), BackendError> {
        self.accounts
            .iter()
            .next()
            .map(|(account_id, backend_id)| (account_id.clone(), backend_id.clone()))
            .ok_or_else(|| BackendError::MissingAccount(AccountId::default()))
    }

    fn default_workspace_binding_for_account(
        &self,
        account_id: &AccountId,
        backend_id: &BackendId,
    ) -> Result<&WorkspaceBinding, BackendError> {
        self.workspace_bindings
            .values()
            .find(|binding| &binding.account_id == account_id && &binding.backend_id == backend_id)
            .ok_or_else(|| BackendError::MissingWorkspaceBinding(account_id.0.clone()))
    }
}
