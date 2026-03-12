use crate::domain::{
    affinity::Affinity,
    attachment::{
        AttachmentKind, AttachmentPreview, AttachmentSource, AttachmentSummary,
        attachment_kind_from_path,
    },
    backend::AccountId,
    conversation::{ConversationKind, ConversationSummary},
    ids::{ChannelId, ConversationId, DmId, MessageId, SidebarSectionId, UserId, WorkspaceId},
    message::{MessageFragment, MessageRecord, MessageSendState},
    presence::Presence,
    profile::{ProfileSection, SocialGraph, SocialGraphEntry, SocialGraphListType, UserProfile},
    route::Route,
};
use crate::util::formatting::now_unix_ms;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use super::{
    action::{DraftKey, UiAction},
    effect::{BackendCommand, Effect, SearchScope},
    event::{AppEvent, BackendEvent},
    ids::{ClientMessageId, OpId, QueryId},
    state::{
        BootPhase, ConnectionState, UiSidebarRowState, UiSidebarSectionState, UiState,
        UserProfileState,
    },
};

#[derive(Clone, Debug, Default)]
pub struct ReducerOutput {
    pub effects: Vec<Effect>,
}

pub fn reduce(state: &mut UiState, event: AppEvent) -> ReducerOutput {
    match event {
        AppEvent::Ui(action) => reduce_ui_action(state, action),
        AppEvent::Backend(event) => reduce_backend_event(state, event),
        AppEvent::EffectResult(_) | AppEvent::Tick(_) => ReducerOutput::default(),
    }
}

fn reduce_ui_action(state: &mut UiState, action: UiAction) -> ReducerOutput {
    match action {
        UiAction::StartApp => {
            state.app.boot_phase = BootPhase::HydratingLocalState;
            ReducerOutput {
                effects: vec![
                    Effect::LoadSettings,
                    Effect::Backend(BackendCommand::ConnectAccount {
                        account_id: AccountId::new("account_demo_keybase"),
                    }),
                    Effect::Backend(BackendCommand::LoadBootstrap {
                        account_id: AccountId::new("account_demo_keybase"),
                    }),
                ],
            }
        }
        UiAction::Navigate(route) => {
            state.navigation.current_route = Some(route.clone());
            state.sidebar.highlighted_route = Some(route.clone());

            let Some(conversation_id) = conversation_id_from_route(&route) else {
                return ReducerOutput::default();
            };

            let changed = state.timeline.conversation_id.as_ref() != Some(&conversation_id);
            state.timeline.conversation_id = Some(conversation_id.clone());
            state.navigation.active_thread_root = None;
            state.thread.open = false;
            state.thread.root_message_id = None;
            state.thread.replies.clear();
            state.thread.reply_draft.clear();
            state.thread.loading = false;
            if changed {
                state.timeline.messages.clear();
                state.timeline.typing_text = None;
                state.timeline.highlighted_message_id = None;
                state.timeline.older_cursor = None;
                state.timeline.newer_cursor = None;
                state.timeline.loading_older = false;
            }

            let mut effects = vec![Effect::Backend(BackendCommand::LoadConversation {
                conversation_id: conversation_id.clone(),
            })];
            if state
                .backend
                .accounts
                .values()
                .any(|account| account.capabilities.supports_conversation_members)
            {
                effects.push(Effect::Backend(BackendCommand::LoadConversationMembers {
                    conversation_id,
                }));
            }
            ReducerOutput { effects }
        }
        UiAction::NavigateQuiet(route) => {
            state.navigation.current_route = Some(route.clone());
            state.sidebar.highlighted_route = Some(route.clone());

            let Some(conversation_id) = conversation_id_from_route(&route) else {
                return ReducerOutput::default();
            };

            let changed = state.timeline.conversation_id.as_ref() != Some(&conversation_id);
            state.timeline.conversation_id = Some(conversation_id);
            state.navigation.active_thread_root = None;
            state.thread.open = false;
            state.thread.root_message_id = None;
            state.thread.replies.clear();
            state.thread.reply_draft.clear();
            state.thread.loading = false;
            if changed {
                state.timeline.messages.clear();
                state.timeline.typing_text = None;
                state.timeline.highlighted_message_id = None;
                state.timeline.older_cursor = None;
                state.timeline.newer_cursor = None;
                state.timeline.loading_older = false;
            }

            ReducerOutput::default()
        }
        UiAction::NavigateBack => ReducerOutput::default(),
        UiAction::OpenThread { root_id } => {
            state.navigation.active_thread_root = Some(root_id.clone());
            state.thread.open = true;
            if state.thread.root_message_id.as_ref() != Some(&root_id) {
                state.thread.replies.clear();
            }
            state.thread.root_message_id = Some(root_id.clone());
            state.thread.reply_draft = state
                .drafts
                .get(&DraftKey::Thread(root_id.clone()))
                .map(|draft| draft.text.clone())
                .unwrap_or_default();
            state.thread.loading = true;
            let Some(conversation_id) = state.timeline.conversation_id.clone() else {
                return ReducerOutput::default();
            };
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::LoadThread {
                    conversation_id,
                    root_id,
                })],
            }
        }
        UiAction::CloseRightPane => {
            state.navigation.active_thread_root = None;
            state.thread.open = false;
            state.thread.root_message_id = None;
            state.thread.replies.clear();
            state.thread.reply_draft.clear();
            state.thread.loading = false;
            ReducerOutput::default()
        }
        UiAction::SetSidebarFilter(filter) => {
            state.sidebar_filter = filter;
            state.sidebar.filter = state.sidebar_filter.clone();
            ReducerOutput::default()
        }
        UiAction::SetSearchQuery(query) => {
            state.search_query = query;
            state.search.query = state.search_query.clone();
            ReducerOutput::default()
        }
        UiAction::SubmitSearch => {
            state.search.is_loading = true;
            state.search.results.clear();
            state.search.highlighted_index = None;
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::Search {
                    query_id: next_query_id(state),
                    scope: match state.navigation.current_route.as_ref() {
                        Some(Route::Channel { workspace_id, .. })
                        | Some(Route::DirectMessage { workspace_id, .. })
                        | Some(Route::Activity { workspace_id })
                        | Some(Route::Search { workspace_id, .. })
                        | Some(Route::WorkspaceHome { workspace_id })
                        | Some(Route::ActiveCall { workspace_id, .. }) => {
                            SearchScope::Workspace(workspace_id.clone())
                        }
                        Some(Route::Preferences) | None => SearchScope::Global,
                    },
                    query: state.search_query.clone(),
                    filters: state.search.filters.clone(),
                })],
            }
        }
        UiAction::OpenNewChat => {
            state.new_chat = super::state::UiNewChatState::default();
            state.new_chat.open = true;
            ReducerOutput::default()
        }
        UiAction::CloseNewChat => {
            state.new_chat = super::state::UiNewChatState::default();
            ReducerOutput::default()
        }
        UiAction::NewChatSearchUsers { query } => {
            state.new_chat.search_query = query.clone();
            state.new_chat.error = None;
            if query.trim().is_empty() {
                state.new_chat.search_results.clear();
                return ReducerOutput::default();
            }
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::SearchUsers {
                    query_id: next_query_id(state),
                    query,
                })],
            }
        }
        UiAction::NewChatAddParticipant { user } => {
            if state
                .new_chat
                .selected_participants
                .iter()
                .all(|entry| entry.id != user.id)
            {
                state.new_chat.selected_participants.push(user);
            }
            state.new_chat.error = None;
            ReducerOutput::default()
        }
        UiAction::NewChatRemoveParticipant { user_id } => {
            state
                .new_chat
                .selected_participants
                .retain(|entry| entry.id != user_id);
            ReducerOutput::default()
        }
        UiAction::NewChatCreate => {
            if state.new_chat.selected_participants.is_empty() {
                state.new_chat.error = Some("Select at least one participant.".to_string());
                return ReducerOutput::default();
            }
            state.new_chat.creating = true;
            state.new_chat.error = None;
            let participants = state
                .new_chat
                .selected_participants
                .iter()
                .map(|entry| entry.id.clone())
                .collect::<Vec<_>>();
            let kind = if participants.len() == 1 {
                ConversationKind::DirectMessage
            } else {
                ConversationKind::GroupDirectMessage
            };
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::CreateConversation {
                    op_id: next_op_id(state, "new-chat"),
                    participants,
                    kind,
                })],
            }
        }
        UiAction::QuickSwitcherSearch { seq, query } => ReducerOutput {
            effects: vec![Effect::Backend(BackendCommand::Search {
                query_id: QueryId::new(format!("quick-switcher-{seq}")),
                scope: match state.navigation.current_route.as_ref() {
                    Some(Route::Channel { workspace_id, .. })
                    | Some(Route::DirectMessage { workspace_id, .. })
                    | Some(Route::Activity { workspace_id })
                    | Some(Route::Search { workspace_id, .. })
                    | Some(Route::WorkspaceHome { workspace_id })
                    | Some(Route::ActiveCall { workspace_id, .. }) => {
                        SearchScope::Workspace(workspace_id.clone())
                    }
                    Some(Route::Preferences) | None => SearchScope::Global,
                },
                query,
                filters: Vec::new(),
            })],
        },
        UiAction::FindInChatSearch {
            seq,
            conversation_id,
            query,
        } => ReducerOutput {
            effects: vec![Effect::Backend(BackendCommand::Search {
                query_id: QueryId::new(format!("find-in-chat-{seq}")),
                scope: SearchScope::Conversation(conversation_id),
                query,
                filters: Vec::new(),
            })],
        },
        UiAction::ToggleSearchFilter(filter) => {
            if let Some(index) = state.search.filters.iter().position(|item| item == &filter) {
                state.search.filters.remove(index);
            } else {
                state.search.filters.push(filter);
            }
            ReducerOutput::default()
        }
        UiAction::UpdateDraft { key, text } => {
            if let DraftKey::Thread(_) = &key {
                state.thread.reply_draft = text.clone();
            }
            state.drafts.entry(key.clone()).or_default().text = text;
            ReducerOutput {
                effects: vec![Effect::PersistDraft { key }],
            }
        }
        UiAction::SendMessage { key } => {
            let Some(draft) = state.drafts.get(&key) else {
                return ReducerOutput::default();
            };
            let draft_text = draft.text.clone();
            if draft_text.trim().is_empty() {
                return ReducerOutput::default();
            }

            match key.clone() {
                DraftKey::Conversation(conversation_id) => {
                    let client_message_id = next_client_message_id(state);
                    let op_id = next_op_id(state, "send");
                    add_pending_message_to_state(
                        state,
                        &conversation_id,
                        &draft_text,
                        Vec::new(),
                        None,
                        &client_message_id,
                    );
                    ReducerOutput {
                        effects: vec![Effect::Backend(BackendCommand::SendMessage {
                            op_id,
                            draft_key: key,
                            conversation_id,
                            client_message_id,
                            text: draft_text,
                            attachments: Vec::new(),
                            reply_to: None,
                        })],
                    }
                }
                DraftKey::Thread(root_id) => {
                    let Some(conversation_id) = state.timeline.conversation_id.clone() else {
                        return ReducerOutput::default();
                    };
                    let reply_to = state
                        .thread
                        .replies
                        .last()
                        .map(|message| message.id.clone())
                        .unwrap_or_else(|| root_id.clone());
                    let client_message_id = next_client_message_id(state);
                    let op_id = next_op_id(state, "send");
                    add_pending_message_to_state(
                        state,
                        &conversation_id,
                        &draft_text,
                        Vec::new(),
                        Some(root_id.clone()),
                        &client_message_id,
                    );
                    ReducerOutput {
                        effects: vec![Effect::Backend(BackendCommand::SendMessage {
                            op_id,
                            draft_key: key,
                            conversation_id,
                            client_message_id,
                            text: draft_text,
                            attachments: Vec::new(),
                            reply_to: Some(reply_to),
                        })],
                    }
                }
            }
        }
        UiAction::EditMessage {
            conversation_id,
            message_id,
            text,
        } => {
            let text = text.trim().to_string();
            if text.is_empty() {
                return ReducerOutput::default();
            }
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::EditMessage {
                    op_id: next_op_id(state, "edit"),
                    conversation_id,
                    message_id,
                    text,
                })],
            }
        }
        UiAction::DeleteMessage {
            conversation_id,
            message_id,
        } => {
            state.timeline.messages.retain(|m| m.id != message_id);
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::DeleteMessage {
                    op_id: next_op_id(state, "delete"),
                    conversation_id,
                    message_id,
                })],
            }
        }
        UiAction::SendAttachment {
            key,
            local_path,
            filename,
            caption,
        } => {
            let local_path = local_path.trim().to_string();
            if local_path.is_empty() {
                return ReducerOutput::default();
            }

            let filename = filename.trim().to_string();
            let filename_fallback = if filename.is_empty() {
                "attachment".to_string()
            } else {
                filename
            };
            let caption = caption.trim().to_string();
            let pending_attachment_kind = attachment_kind_from_path(Path::new(&local_path));
            let pending_attachment_source = AttachmentSource::LocalPath(local_path.clone());
            let pending_attachment_preview =
                matches!(pending_attachment_kind, AttachmentKind::Image).then(|| {
                    AttachmentPreview {
                        source: pending_attachment_source.clone(),
                        width: None,
                        height: None,
                    }
                });
            let pending_attachment = AttachmentSummary {
                name: filename_fallback.clone(),
                kind: pending_attachment_kind,
                preview: pending_attachment_preview,
                source: Some(pending_attachment_source),
                ..AttachmentSummary::default()
            };

            match key.clone() {
                DraftKey::Conversation(conversation_id) => {
                    let client_message_id = next_client_message_id(state);
                    let op_id = next_op_id(state, "send-attachment");
                    add_pending_message_to_state(
                        state,
                        &conversation_id,
                        &caption,
                        vec![pending_attachment.clone()],
                        None,
                        &client_message_id,
                    );
                    ReducerOutput {
                        effects: vec![Effect::Backend(BackendCommand::SendAttachment {
                            op_id,
                            draft_key: key,
                            conversation_id,
                            client_message_id,
                            local_path: local_path.clone(),
                            filename: filename_fallback.clone(),
                            caption: caption.clone(),
                        })],
                    }
                }
                DraftKey::Thread(root_id) => {
                    let Some(conversation_id) = state.timeline.conversation_id.clone() else {
                        return ReducerOutput::default();
                    };
                    let client_message_id = next_client_message_id(state);
                    let op_id = next_op_id(state, "send-attachment");
                    add_pending_message_to_state(
                        state,
                        &conversation_id,
                        &caption,
                        vec![pending_attachment.clone()],
                        Some(root_id),
                        &client_message_id,
                    );
                    ReducerOutput {
                        effects: vec![Effect::Backend(BackendCommand::SendAttachment {
                            op_id,
                            draft_key: key,
                            conversation_id,
                            client_message_id,
                            local_path,
                            filename: filename_fallback,
                            caption,
                        })],
                    }
                }
            }
        }
        UiAction::ReactToMessage {
            conversation_id,
            message_id,
            emoji,
        } => {
            let actor_id = current_user_id(state);
            toggle_runtime_reaction(
                state,
                conversation_id.clone(),
                message_id.clone(),
                emoji.clone(),
                actor_id,
                now_unix_ms(),
            );
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::ReactToMessage {
                    op_id: next_op_id(state, "react"),
                    conversation_id,
                    message_id,
                    reaction: emoji,
                })],
            }
        }
        UiAction::StartCall { conversation_id } => ReducerOutput {
            effects: vec![Effect::Backend(BackendCommand::StartCall {
                op_id: next_op_id(state, "call"),
                conversation_id,
            })],
        },
        UiAction::MarkConversationRead {
            conversation_id,
            message_id,
        } => ReducerOutput {
            effects: vec![Effect::Backend(BackendCommand::MarkRead {
                conversation_id,
                message_id,
            })],
        },
        UiAction::JumpToMessage {
            conversation_id,
            message_id,
        } => {
            state.timeline.conversation_id = Some(conversation_id.clone());
            state.timeline.messages.clear();
            state.timeline.highlighted_message_id = Some(message_id.clone());
            state.timeline.typing_text = None;
            state.timeline.older_cursor = None;
            state.timeline.newer_cursor = None;
            state.timeline.loading_older = true;
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::JumpToMessage {
                    conversation_id,
                    message_id,
                })],
            }
        }
        UiAction::LoadOlderMessages { key, cursor } => {
            state.timeline.loading_older = true;
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::LoadOlderMessages {
                    key,
                    cursor,
                })],
            }
        }
        UiAction::OpenOrCreateDirectMessage { user_id } => {
            let target_user = UserId::new(user_id.0.trim().to_ascii_lowercase());
            if target_user.0.is_empty() {
                return ReducerOutput::default();
            }
            if let Some(conversation_id) = find_existing_direct_message_with_user(
                &state.workspace.direct_messages,
                &target_user,
            ) {
                let workspace_id = state
                    .workspace
                    .active_workspace_id
                    .clone()
                    .unwrap_or_else(|| WorkspaceId::new("ws_primary"));
                return reduce_ui_action(
                    state,
                    UiAction::Navigate(Route::DirectMessage {
                        workspace_id,
                        dm_id: DmId::new(conversation_id.0),
                    }),
                );
            }
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::CreateConversation {
                    op_id: next_op_id(state, "profile-message"),
                    participants: vec![target_user],
                    kind: ConversationKind::DirectMessage,
                })],
            }
        }
        UiAction::ShowUserProfileCard { user_id } | UiAction::ShowUserProfilePanel { user_id } => {
            state.backend.profile_panel.loading.insert(user_id.clone());
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::LoadUserProfile { user_id })],
            }
        }
        UiAction::RefreshProfilePresence {
            user_id,
            conversation_id,
        } => ReducerOutput {
            effects: vec![Effect::Backend(BackendCommand::RefreshParticipants {
                user_id,
                conversation_id,
            })],
        },
        UiAction::LoadSocialGraphList { user_id, list_type } => {
            state
                .backend
                .profile_panel
                .loading_social_list
                .insert(user_id.clone());
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::LoadSocialGraphList {
                    user_id,
                    list_type,
                })],
            }
        }
        UiAction::FollowUser { user_id } => {
            state
                .backend
                .user_affinities
                .insert(user_id.clone(), Affinity::Positive);
            update_profile_affinity(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                Affinity::Positive,
            );
            update_follow_status(&mut state.backend.profile_panel.profiles, &user_id, true);
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::FollowUser { user_id })],
            }
        }
        UiAction::UnfollowUser { user_id } => {
            state
                .backend
                .user_affinities
                .insert(user_id.clone(), Affinity::None);
            update_profile_affinity(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                Affinity::None,
            );
            update_follow_status(&mut state.backend.profile_panel.profiles, &user_id, false);
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::UnfollowUser { user_id })],
            }
        }
    }
}

fn reduce_backend_event(state: &mut UiState, event: BackendEvent) -> ReducerOutput {
    match event {
        BackendEvent::AccountConnected { account_id } => {
            if let Some(account) = state.backend.accounts.get_mut(&account_id) {
                account.connection_state = ConnectionState::Connected;
            }
            state.app.boot_phase = BootPhase::ConnectingBackend;
        }
        BackendEvent::AccountDisconnected { account_id, .. } => {
            if let Some(account) = state.backend.accounts.get_mut(&account_id) {
                account.connection_state = ConnectionState::Disconnected;
            }
            state.app.boot_phase = BootPhase::Degraded;
        }
        BackendEvent::BootstrapLoaded {
            account_id,
            payload,
        } => {
            let mut bootstrap_effects = Vec::new();
            state.backend.message_reactions.clear();
            state.backend.conversation_pins.clear();
            state.backend.conversation_team_ids.clear();
            state.backend.team_roles.clear();
            if let Some(account) = state.backend.accounts.get_mut(&account_id)
                && let Some(display_name) = payload.account_display_name
            {
                account.display_name = display_name;
            }

            state.workspace.active_workspace_id = payload.active_workspace_id.clone();
            state.workspace.workspace_name = payload.workspace_name;
            state.workspace.channels = payload.channels;
            state.workspace.direct_messages = payload.direct_messages;

            for binding in payload.workspace_bindings {
                state
                    .backend
                    .workspace_bindings
                    .insert(binding.workspace_id.clone(), binding);
            }
            for binding in payload.conversation_bindings {
                state
                    .backend
                    .conversation_bindings
                    .insert(binding.conversation_id.clone(), binding);
            }
            for binding in payload.message_bindings {
                state
                    .backend
                    .message_bindings
                    .insert(binding.message_id.clone(), binding);
            }

            let workspace_id = state
                .workspace
                .active_workspace_id
                .clone()
                .unwrap_or_else(|| WorkspaceId::new("ws_primary"));
            state.sidebar.sections = build_sidebar_sections(
                &state.workspace.channels,
                &state.workspace.direct_messages,
                &workspace_id,
                &state.backend.user_profiles,
            );

            if let Some(conversation_id) = payload.selected_conversation_id {
                state.timeline.conversation_id = Some(conversation_id.clone());
                if let Some(route) = route_for_conversation_id(
                    &conversation_id,
                    &workspace_id,
                    &state.workspace.channels,
                    &state.workspace.direct_messages,
                ) {
                    state.navigation.current_route = Some(route.clone());
                    state.sidebar.highlighted_route = Some(route);
                }
                bootstrap_effects.push(Effect::Backend(BackendCommand::LoadConversation {
                    conversation_id,
                }));
            }

            state.timeline.messages = payload.selected_messages;
            normalize_messages_by_id(&mut state.timeline.messages);
            for message in state.timeline.messages.clone() {
                apply_message_reactions(state, &message);
            }
            state.timeline.unread_marker = payload.unread_marker;
            state.timeline.loading_older = false;
            state.timeline.older_cursor = state
                .timeline
                .messages
                .first()
                .map(|message| message.id.0.clone());
            state.timeline.newer_cursor = state
                .timeline
                .messages
                .last()
                .map(|message| message.id.0.clone());
            state.app.boot_phase = BootPhase::Ready;
            return ReducerOutput {
                effects: bootstrap_effects,
            };
        }
        BackendEvent::WorkspaceConversationsExtended {
            workspace_id,
            channels,
            direct_messages,
            conversation_bindings,
        } => {
            if state.workspace.active_workspace_id.is_none() {
                state.workspace.active_workspace_id = Some(workspace_id.clone());
            }
            merge_conversation_summaries(&mut state.workspace.channels, channels);
            merge_conversation_summaries(&mut state.workspace.direct_messages, direct_messages);
            for binding in conversation_bindings {
                state
                    .backend
                    .conversation_bindings
                    .insert(binding.conversation_id.clone(), binding);
            }
            let sidebar_workspace_id = state
                .workspace
                .active_workspace_id
                .clone()
                .unwrap_or(workspace_id);
            state.sidebar.sections = build_sidebar_sections(
                &state.workspace.channels,
                &state.workspace.direct_messages,
                &sidebar_workspace_id,
                &state.backend.user_profiles,
            );
        }
        BackendEvent::WorkspaceSynced { .. } | BackendEvent::ConversationLoaded { .. } => {}
        BackendEvent::MessagesPrepended {
            key,
            messages,
            cursor,
        } => match key {
            super::state::TimelineKey::Conversation(conversation_id) => {
                if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                    let previous_oldest_id = state
                        .timeline
                        .messages
                        .first()
                        .map(|message| message.id.0.clone());
                    for message in &messages {
                        apply_message_reactions(state, message);
                    }
                    let mut merged = messages;
                    merged.extend(state.timeline.messages.clone());
                    normalize_messages_by_id(&mut merged);
                    trim_timeline_for_prepend(&mut merged);
                    recompute_thread_reply_counts(&mut merged);
                    state.timeline.messages = merged;
                    let next_oldest_id = state
                        .timeline
                        .messages
                        .first()
                        .map(|message| message.id.0.clone());
                    let no_older_progress =
                        next_oldest_id == previous_oldest_id || state.timeline.messages.is_empty();
                    state.timeline.older_cursor = if no_older_progress { None } else { cursor };
                    state.timeline.loading_older = false;
                    state.timeline.newer_cursor = state
                        .timeline
                        .messages
                        .last()
                        .map(|message| message.id.0.clone());
                }
            }
            super::state::TimelineKey::Thread {
                conversation_id,
                root_id,
            } => {
                if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                    state.thread.open = true;
                    state.thread.root_message_id = Some(root_id);
                    state.thread.loading = false;
                    state.thread.replies = messages;
                    normalize_messages_by_id(&mut state.thread.replies);
                }
            }
        },
        BackendEvent::TimelineReplaced {
            conversation_id,
            messages,
            older_cursor,
            newer_cursor,
        } => {
            if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                for message in &messages {
                    apply_message_reactions(state, message);
                }
                // TimelineReplaced is emitted by background conversation loads. A just-arrived live
                // message can land via MessageUpserted while the load is in-flight; if the eventual
                // replace page doesn't include it yet, we'd otherwise "lose" it until re-enter.
                let mut merged = messages;
                merged.extend(state.timeline.messages.clone());
                normalize_messages_by_id(&mut merged);
                recompute_thread_reply_counts(&mut merged);
                trim_timeline_for_append(&mut merged);
                state.timeline.messages = merged;
                // Prefer computed cursors from merged state to avoid regressing past newer arrivals.
                state.timeline.older_cursor = state
                    .timeline
                    .messages
                    .first()
                    .map(|message| message.id.0.clone())
                    .or(older_cursor);
                state.timeline.newer_cursor = state
                    .timeline
                    .messages
                    .last()
                    .map(|message| message.id.0.clone())
                    .or(newer_cursor);
                state.timeline.loading_older = false;
            }
        }
        BackendEvent::MessageUpserted(message) => {
            upsert_message_in_state(state, message);
        }
        BackendEvent::MessageSendConfirmed {
            op_id: _,
            client_message_id,
            mut server_message,
        } => {
            let pending_id = pending_message_id_for_client(&client_message_id);
            let pending_snapshot = state
                .timeline
                .messages
                .iter()
                .find(|message| message.id == pending_id)
                .cloned();
            if let Some(pending_message) = pending_snapshot.as_ref() {
                merge_pending_message_attachment_sources(&mut server_message, pending_message);
            }
            replace_pending_message_id(state, &pending_id, &server_message.id);
            upsert_message_in_state(state, server_message);
        }
        BackendEvent::MessageSendFailed {
            op_id: _,
            client_message_id,
            error: _,
        } => {
            let pending_id = pending_message_id_for_client(&client_message_id);
            mark_message_failed(state, &pending_id);
        }
        BackendEvent::MessageDeleted {
            conversation_id,
            message_id,
        } => {
            if let Some(reactions) = state.backend.message_reactions.get_mut(&conversation_id) {
                reactions.remove(&message_id);
            }
            if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                state
                    .timeline
                    .messages
                    .retain(|message| message.id != message_id);
                state
                    .thread
                    .replies
                    .retain(|message| message.id != message_id);
                state.timeline.newer_cursor = state
                    .timeline
                    .messages
                    .last()
                    .map(|message| message.id.0.clone());
                if state.timeline.older_cursor.is_some() {
                    state.timeline.older_cursor = state
                        .timeline
                        .messages
                        .first()
                        .map(|message| message.id.0.clone());
                }
            }
        }
        BackendEvent::TypingUpdated {
            conversation_id,
            users,
        } => {
            if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                state.timeline.typing_text = if users.is_empty() {
                    None
                } else if users.len() == 1 {
                    Some(format!("{} is typing…", users[0].0))
                } else {
                    Some(format!("{} people are typing…", users.len()))
                };
            }
        }
        BackendEvent::UserProfileUpserted {
            user_id,
            display_name,
            avatar_asset,
            updated_ms,
        } => {
            state.backend.user_profiles.insert(
                user_id.clone(),
                super::state::UserProfileState {
                    display_name: display_name.clone(),
                    avatar_asset: avatar_asset.clone(),
                    updated_ms,
                },
            );
            for account in state.backend.accounts.values_mut() {
                if account.display_name == user_id.0 {
                    account.avatar = avatar_asset.clone();
                }
            }
        }
        BackendEvent::UserProfileLoaded {
            account_id,
            profile,
        } => {
            let user_id = profile.user_id.clone();
            let mut profile = profile;
            if let Some(presence) = presence_for_user(&state.backend.user_presences, &user_id) {
                profile.presence = presence;
            }
            state.backend.profile_panel.loading.remove(&user_id);
            state
                .backend
                .user_presences
                .insert(user_id.clone(), profile.presence.clone());
            state
                .backend
                .profile_panel
                .profiles
                .insert(user_id.clone(), profile.clone());
            state.backend.user_profiles.insert(
                user_id.clone(),
                super::state::UserProfileState {
                    display_name: profile.display_name.clone(),
                    avatar_asset: profile.avatar_asset.clone(),
                    updated_ms: now_unix_ms(),
                },
            );
            if let Some(account) = state.backend.accounts.get_mut(&account_id)
                && account.display_name.eq_ignore_ascii_case(&user_id.0)
            {
                account.avatar = profile.avatar_asset.clone();
            }

            let needs_social_load = profile.sections.iter().any(|section| {
                matches!(
                    section,
                    ProfileSection::SocialGraph(graph)
                        if graph.followers.is_none() && graph.following.is_none()
                )
            });
            if needs_social_load
                && !state
                    .backend
                    .profile_panel
                    .loading_social_list
                    .contains(&user_id)
            {
                state
                    .backend
                    .profile_panel
                    .loading_social_list
                    .insert(user_id.clone());
                return ReducerOutput {
                    effects: vec![Effect::Backend(BackendCommand::LoadSocialGraphList {
                        user_id,
                        list_type: SocialGraphListType::Followers,
                    })],
                };
            }
        }
        BackendEvent::SocialGraphListLoaded {
            user_id,
            list_type,
            entries,
        } => {
            state
                .backend
                .profile_panel
                .loading_social_list
                .remove(&user_id);
            if let Some(profile) = state.backend.profile_panel.profiles.get_mut(&user_id) {
                merge_social_graph_entries(profile, list_type, entries);
            }
        }
        BackendEvent::FollowStatusChanged {
            user_id,
            you_are_following,
        } => {
            let next_affinity = if you_are_following {
                Affinity::Positive
            } else {
                Affinity::None
            };
            state
                .backend
                .user_affinities
                .insert(user_id.clone(), next_affinity);
            update_profile_affinity(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                next_affinity,
            );
            update_follow_status(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                you_are_following,
            );
        }
        BackendEvent::FollowStatusChangeFailed {
            user_id,
            attempted_follow,
            error: _,
        } => {
            let reverted_follow = !attempted_follow;
            let reverted_affinity = if reverted_follow {
                Affinity::Positive
            } else {
                Affinity::None
            };
            state
                .backend
                .user_affinities
                .insert(user_id.clone(), reverted_affinity);
            update_profile_affinity(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                reverted_affinity,
            );
            update_follow_status(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                reverted_follow,
            );
        }
        BackendEvent::AffinityChanged { user_id, affinity } => {
            state
                .backend
                .user_affinities
                .insert(user_id.clone(), affinity);
            update_profile_affinity(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                affinity,
            );
            update_follow_status(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                matches!(affinity, Affinity::Positive),
            );
        }
        BackendEvent::AffinitySynced { affinities } => {
            for (user_id, affinity) in &affinities {
                update_profile_affinity(
                    &mut state.backend.profile_panel.profiles,
                    user_id,
                    *affinity,
                );
                update_follow_status(
                    &mut state.backend.profile_panel.profiles,
                    user_id,
                    matches!(affinity, Affinity::Positive),
                );
            }
            state.backend.user_affinities = affinities;
        }
        BackendEvent::PresenceUpdated {
            account_id: _,
            users,
        } => {
            for patch in users {
                state
                    .backend
                    .user_presences
                    .insert(patch.user_id.clone(), patch.presence.clone());
                update_profile_presence(
                    &mut state.backend.profile_panel.profiles,
                    &patch.user_id,
                    patch.presence,
                );
            }
        }
        BackendEvent::ConversationEmojisSynced {
            conversation_id,
            emojis,
        } => {
            let mut index = std::collections::HashMap::new();
            for emoji in emojis {
                index.insert(
                    emoji.alias.to_ascii_lowercase(),
                    super::state::EmojiRenderState {
                        alias: emoji.alias,
                        unicode: emoji.unicode,
                        asset_path: emoji.asset_path,
                        updated_ms: emoji.updated_ms,
                    },
                );
            }
            state
                .backend
                .conversation_emojis
                .insert(conversation_id, index);
        }
        BackendEvent::EmojiSourceSynced {
            source_ref,
            alias,
            unicode,
            asset_path,
            updated_ms,
        } => {
            state.backend.emoji_sources.insert(
                source_ref.cache_key(),
                super::state::EmojiRenderState {
                    alias,
                    unicode,
                    asset_path,
                    updated_ms,
                },
            );
        }
        BackendEvent::MessageReactionsSynced {
            conversation_id,
            reactions_by_message,
        } => {
            let entry = state
                .backend
                .message_reactions
                .entry(conversation_id)
                .or_default();
            for message_reactions in reactions_by_message {
                if message_reactions.reactions.is_empty() {
                    entry.remove(&message_reactions.message_id);
                    continue;
                }
                entry.insert(
                    message_reactions.message_id,
                    message_reactions
                        .reactions
                        .into_iter()
                        .map(|reaction| super::state::MessageReactionState {
                            emoji: reaction.emoji,
                            source_ref: reaction.source_ref,
                            actor_ids: reaction.actor_ids,
                            updated_ms: reaction.updated_ms,
                        })
                        .collect(),
                );
            }
        }
        BackendEvent::PinnedStateUpdated {
            conversation_id,
            pinned,
        } => {
            state
                .backend
                .conversation_pins
                .insert(conversation_id, pinned);
        }
        BackendEvent::MessageReactionApplied {
            conversation_id,
            message_id,
            emoji,
            source_ref,
            actor_id,
            updated_ms,
        } => add_runtime_reaction(
            state,
            conversation_id,
            message_id,
            emoji,
            source_ref,
            actor_id,
            updated_ms,
        ),
        BackendEvent::MessageReactionRemoved {
            conversation_id,
            message_id,
            emoji,
            actor_id,
        } => {
            let Some(reactions_by_message) =
                state.backend.message_reactions.get_mut(&conversation_id)
            else {
                return ReducerOutput::default();
            };
            let Some(reactions) = reactions_by_message.get_mut(&message_id) else {
                return ReducerOutput::default();
            };
            if let Some(reaction) = reactions
                .iter_mut()
                .find(|entry| entry.emoji.eq_ignore_ascii_case(&emoji))
            {
                reaction.actor_ids.retain(|id| id != &actor_id);
            }
            reactions.retain(|entry| !entry.actor_ids.is_empty());
            if reactions.is_empty() {
                reactions_by_message.remove(&message_id);
            }
        }
        BackendEvent::ReactionFailed { op_id: _, error: _ } => {}
        BackendEvent::TeamRolesUpdated {
            conversation_id,
            team_id,
            roles,
            updated_ms: _,
        } => {
            state
                .backend
                .conversation_team_ids
                .insert(conversation_id, team_id.clone());
            let mut role_index = HashMap::new();
            for role_entry in roles {
                role_index.insert(role_entry.user_id, role_entry.role);
            }
            state.backend.team_roles.insert(team_id, role_index);
        }
        BackendEvent::ConversationMembersUpdated {
            conversation_id,
            members,
            updated_ms,
            is_complete,
        } => {
            state.backend.conversation_members.insert(
                conversation_id,
                super::state::ConversationMembersState {
                    members,
                    updated_ms,
                    is_complete,
                },
            );
        }
        BackendEvent::ConversationUnreadChanged {
            conversation_id,
            unread_count,
            mention_count,
            read_upto,
        } => {
            let mut changed = false;
            for summary in state.workspace.channels.iter_mut() {
                if summary.id == conversation_id {
                    summary.unread_count = unread_count;
                    summary.mention_count = mention_count;
                    changed = true;
                    break;
                }
            }
            if !changed {
                for summary in state.workspace.direct_messages.iter_mut() {
                    if summary.id == conversation_id {
                        summary.unread_count = unread_count;
                        summary.mention_count = mention_count;
                        changed = true;
                        break;
                    }
                }
            }
            if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                state.timeline.unread_marker = read_upto;
            }
            if changed {
                let workspace_id = state
                    .workspace
                    .active_workspace_id
                    .clone()
                    .unwrap_or_else(|| WorkspaceId::new("ws_primary"));
                state.sidebar.sections = build_sidebar_sections(
                    &state.workspace.channels,
                    &state.workspace.direct_messages,
                    &workspace_id,
                    &state.backend.user_profiles,
                );
            }
        }
        BackendEvent::ReadMarkerUpdated {
            conversation_id,
            read_upto,
        } => {
            if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                state.timeline.unread_marker = read_upto;
            }
        }
        BackendEvent::CallUpdated(_) => {}
        BackendEvent::SearchResults {
            query_id: _,
            results,
            is_complete,
        } => {
            state.search.results.extend(results);
            state.search.highlighted_index = (!state.search.results.is_empty()).then_some(0);
            state.search.is_loading = !is_complete;
        }
        BackendEvent::UserSearchResults {
            query_id: _,
            results,
        } => {
            state.new_chat.search_results = results;
            state.new_chat.creating = false;
        }
        BackendEvent::ConversationCreated {
            op_id: _,
            workspace_id,
            conversation,
            conversation_binding,
        } => {
            state.backend.conversation_bindings.insert(
                conversation_binding.conversation_id.clone(),
                conversation_binding,
            );
            match &conversation.kind {
                ConversationKind::Channel => {
                    merge_conversation_summaries(
                        &mut state.workspace.channels,
                        vec![conversation.clone()],
                    );
                }
                ConversationKind::DirectMessage | ConversationKind::GroupDirectMessage => {
                    merge_conversation_summaries(
                        &mut state.workspace.direct_messages,
                        vec![conversation.clone()],
                    );
                }
            }
            if state.workspace.active_workspace_id.is_none() {
                state.workspace.active_workspace_id = Some(workspace_id.clone());
            }
            let sidebar_workspace_id = state
                .workspace
                .active_workspace_id
                .as_ref()
                .unwrap_or(&workspace_id);
            state.sidebar.sections = build_sidebar_sections(
                &state.workspace.channels,
                &state.workspace.direct_messages,
                sidebar_workspace_id,
                &state.backend.user_profiles,
            );
            let route = route_for_summary(&conversation, &workspace_id);
            state.navigation.current_route = Some(route.clone());
            state.sidebar.highlighted_route = Some(route);
            state.timeline.conversation_id = Some(conversation.id.clone());
            state.timeline.messages.clear();
            state.timeline.typing_text = None;
            state.timeline.highlighted_message_id = None;
            state.timeline.unread_marker = None;
            state.timeline.older_cursor = None;
            state.timeline.newer_cursor = None;
            state.timeline.loading_older = false;
            state.navigation.active_thread_root = None;
            state.thread.open = false;
            state.thread.root_message_id = None;
            state.thread.replies.clear();
            state.thread.reply_draft.clear();
            state.thread.loading = false;
            state.new_chat = super::state::UiNewChatState::default();
            return ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::LoadConversation {
                    conversation_id: conversation.id,
                })],
            };
        }
        BackendEvent::BootStatus(status) => {
            state.app.boot_status = status.clone();
        }
        BackendEvent::KeybaseNotifyStub { .. } => {}
    }

    ReducerOutput::default()
}

fn current_user_id(state: &UiState) -> UserId {
    state
        .backend
        .accounts
        .values()
        .find(|account| matches!(account.connection_state, ConnectionState::Connected))
        .or_else(|| state.backend.accounts.values().next())
        .map(|account| UserId::new(account.display_name.clone()))
        .unwrap_or_else(|| UserId::new("user_me"))
}

fn presence_for_user(
    user_presences: &HashMap<UserId, Presence>,
    user_id: &UserId,
) -> Option<Presence> {
    user_presences.get(user_id).cloned().or_else(|| {
        let lower = user_id.0.to_ascii_lowercase();
        if lower == user_id.0 {
            None
        } else {
            user_presences.get(&UserId::new(lower)).cloned()
        }
    })
}

fn update_profile_presence(
    profiles: &mut HashMap<UserId, UserProfile>,
    user_id: &UserId,
    presence: Presence,
) {
    if let Some(profile) = profiles.get_mut(user_id) {
        profile.presence = presence.clone();
        return;
    }
    let lower = user_id.0.to_ascii_lowercase();
    if lower != user_id.0
        && let Some(profile) = profiles.get_mut(&UserId::new(lower))
    {
        profile.presence = presence;
    }
}

fn update_profile_affinity(
    profiles: &mut HashMap<UserId, UserProfile>,
    user_id: &UserId,
    affinity: Affinity,
) {
    if let Some(profile) = profiles.get_mut(user_id) {
        profile.affinity = affinity;
    }
    for profile in profiles.values_mut() {
        for section in &mut profile.sections {
            let ProfileSection::SocialGraph(graph) = section else {
                continue;
            };
            if let Some(followers) = graph.followers.as_mut() {
                for entry in followers
                    .iter_mut()
                    .filter(|entry| entry.user_id == *user_id)
                {
                    entry.affinity = affinity;
                }
            }
            if let Some(following) = graph.following.as_mut() {
                for entry in following
                    .iter_mut()
                    .filter(|entry| entry.user_id == *user_id)
                {
                    entry.affinity = affinity;
                }
            }
        }
    }
}

fn update_follow_status(
    profiles: &mut HashMap<UserId, UserProfile>,
    user_id: &UserId,
    you_are_following: bool,
) {
    if let Some(profile) = profiles.get_mut(user_id) {
        for section in &mut profile.sections {
            if let ProfileSection::SocialGraph(graph) = section {
                graph.you_are_following = you_are_following;
            }
        }
        return;
    }
    let lower = user_id.0.to_ascii_lowercase();
    if lower != user_id.0
        && let Some(profile) = profiles.get_mut(&UserId::new(lower))
    {
        for section in &mut profile.sections {
            if let ProfileSection::SocialGraph(graph) = section {
                graph.you_are_following = you_are_following;
            }
        }
    }
}

fn merge_social_graph_entries(
    profile: &mut UserProfile,
    list_type: SocialGraphListType,
    entries: Vec<SocialGraphEntry>,
) {
    for section in &mut profile.sections {
        let ProfileSection::SocialGraph(graph) = section else {
            continue;
        };
        match list_type {
            SocialGraphListType::Followers => {
                graph.followers_count = Some(entries.len() as u32);
                graph.followers = Some(entries);
            }
            SocialGraphListType::Following => {
                graph.following_count = Some(entries.len() as u32);
                graph.following = Some(entries);
            }
        }
        return;
    }

    let mut graph = SocialGraph {
        followers_count: None,
        following_count: None,
        is_following_you: false,
        you_are_following: false,
        followers: None,
        following: None,
    };
    match list_type {
        SocialGraphListType::Followers => {
            graph.followers_count = Some(entries.len() as u32);
            graph.followers = Some(entries);
        }
        SocialGraphListType::Following => {
            graph.following_count = Some(entries.len() as u32);
            graph.following = Some(entries);
        }
    }
    profile.sections.push(ProfileSection::SocialGraph(graph));
}

fn pending_message_id_for_client(client_message_id: &ClientMessageId) -> MessageId {
    MessageId::new(format!("local:{}", client_message_id.0))
}

fn add_pending_message_to_state(
    state: &mut UiState,
    conversation_id: &ConversationId,
    text: &str,
    attachments: Vec<AttachmentSummary>,
    reply_to: Option<MessageId>,
    client_message_id: &ClientMessageId,
) {
    if state.timeline.conversation_id.as_ref() != Some(conversation_id) {
        return;
    }
    let pending_id = pending_message_id_for_client(client_message_id);
    if state
        .timeline
        .messages
        .iter()
        .any(|message| message.id == pending_id)
    {
        return;
    }
    let text = text.trim();
    let (fragments, source_text) = if text.is_empty() {
        (Vec::new(), None)
    } else {
        (
            vec![MessageFragment::Text(text.to_string())],
            Some(text.to_string()),
        )
    };
    let thread_root_id = reply_to.clone();
    let pending_message = MessageRecord {
        id: pending_id,
        conversation_id: conversation_id.clone(),
        author_id: current_user_id(state),
        reply_to: reply_to.clone(),
        thread_root_id: thread_root_id.clone(),
        timestamp_ms: Some(now_unix_ms()),
        event: None,
        link_previews: Vec::new(),
        permalink: format!("zbase://pending/{}", client_message_id.0),
        fragments,
        source_text,
        attachments,
        reactions: Vec::new(),
        thread_reply_count: 0,
        send_state: MessageSendState::Pending,
        edited: None,
    };
    state.timeline.messages.push(pending_message.clone());
    if let Some(root_id) = thread_root_id {
        refresh_thread_reply_count(&mut state.timeline.messages, &root_id);
        if state.thread.root_message_id.as_ref() == Some(&root_id) {
            state.thread.replies.push(pending_message);
            normalize_messages_by_id(&mut state.thread.replies);
        }
    }
    normalize_messages_by_id(&mut state.timeline.messages);
    trim_timeline_for_append(&mut state.timeline.messages);
    state.timeline.newer_cursor = state
        .timeline
        .messages
        .last()
        .map(|message| message.id.0.clone());
    if state.timeline.older_cursor.is_none() {
        state.timeline.older_cursor = state
            .timeline
            .messages
            .first()
            .map(|message| message.id.0.clone());
    }
}

fn upsert_message_in_state(state: &mut UiState, message: MessageRecord) {
    if state.timeline.conversation_id.as_ref() != Some(&message.conversation_id) {
        // Keep the active timeline stable; incoming events for other conversations
        // should not force navigation.
        return;
    }

    apply_message_reactions(state, &message);
    if let Some(existing) = state
        .timeline
        .messages
        .iter_mut()
        .find(|existing| existing.id == message.id)
    {
        *existing = message.clone();
    } else {
        state.timeline.messages.push(message.clone());
    }
    if let Some(root_id) = message
        .thread_root_id
        .clone()
        .or_else(|| message.reply_to.clone())
    {
        refresh_thread_reply_count(&mut state.timeline.messages, &root_id);
        if state.thread.root_message_id.as_ref() == Some(&root_id) {
            if let Some(existing) = state
                .thread
                .replies
                .iter_mut()
                .find(|existing| existing.id == message.id)
            {
                *existing = message.clone();
            } else {
                state.thread.replies.push(message.clone());
            }
            normalize_messages_by_id(&mut state.thread.replies);
        }
    }
    normalize_messages_by_id(&mut state.timeline.messages);
    trim_timeline_for_append(&mut state.timeline.messages);
    state.timeline.newer_cursor = state
        .timeline
        .messages
        .last()
        .map(|message| message.id.0.clone());
    if state.timeline.older_cursor.is_none() && state.timeline.messages.len() == 1 {
        state.timeline.older_cursor = state
            .timeline
            .messages
            .first()
            .map(|message| message.id.0.clone());
    }
}

fn replace_pending_message_id(state: &mut UiState, pending_id: &MessageId, _server_id: &MessageId) {
    state
        .timeline
        .messages
        .retain(|message| message.id != *pending_id);
    state
        .thread
        .replies
        .retain(|message| message.id != *pending_id);
}

fn merge_pending_message_attachment_sources(
    server_message: &mut MessageRecord,
    pending_message: &MessageRecord,
) {
    if pending_message.attachments.is_empty() {
        return;
    }
    if server_message.attachments.is_empty() {
        server_message.attachments = pending_message.attachments.clone();
        return;
    }
    for (index, server_attachment) in server_message.attachments.iter_mut().enumerate() {
        let pending_attachment = pending_message
            .attachments
            .iter()
            .find(|candidate| {
                !candidate.name.is_empty()
                    && !server_attachment.name.is_empty()
                    && candidate.name.eq_ignore_ascii_case(&server_attachment.name)
            })
            .or_else(|| pending_message.attachments.get(index));
        let Some(pending_attachment) = pending_attachment else {
            continue;
        };
        let pending_has_local_source = matches!(
            &pending_attachment.source,
            Some(AttachmentSource::LocalPath(p)) if !p.is_empty()
        );
        if server_attachment.source.is_none() || pending_has_local_source {
            server_attachment.source = pending_attachment.source.clone();
        }
        let pending_has_local_preview = matches!(
            &pending_attachment.preview,
            Some(preview) if matches!(&preview.source, AttachmentSource::LocalPath(p) if !p.is_empty())
        );
        if server_attachment.preview.is_none() || pending_has_local_preview {
            server_attachment.preview = pending_attachment.preview.clone();
        }
        if server_attachment.width.is_none() {
            server_attachment.width = pending_attachment.width;
        }
        if server_attachment.height.is_none() {
            server_attachment.height = pending_attachment.height;
        }
        if server_attachment.mime_type.is_none() {
            server_attachment.mime_type = pending_attachment.mime_type.clone();
        }
        if server_attachment.duration_ms.is_none() {
            server_attachment.duration_ms = pending_attachment.duration_ms;
        }
        if server_attachment.waveform.is_none() {
            server_attachment.waveform = pending_attachment.waveform.clone();
        }
    }
}

fn mark_message_failed(state: &mut UiState, pending_id: &MessageId) {
    if let Some(message) = state
        .timeline
        .messages
        .iter_mut()
        .find(|message| message.id == *pending_id)
    {
        message.send_state = MessageSendState::Failed;
    }
    if let Some(message) = state
        .thread
        .replies
        .iter_mut()
        .find(|message| message.id == *pending_id)
    {
        message.send_state = MessageSendState::Failed;
    }
}

fn add_runtime_reaction(
    state: &mut UiState,
    conversation_id: ConversationId,
    message_id: MessageId,
    emoji: String,
    source_ref: Option<crate::domain::message::EmojiSourceRef>,
    actor_id: UserId,
    updated_ms: i64,
) {
    let message_entry = state
        .backend
        .message_reactions
        .entry(conversation_id)
        .or_default()
        .entry(message_id)
        .or_default();
    if let Some(existing) = message_entry
        .iter_mut()
        .find(|reaction| reaction.emoji.eq_ignore_ascii_case(&emoji))
    {
        if existing.source_ref.is_none() && source_ref.is_some() {
            existing.source_ref = source_ref;
        }
        if !existing.actor_ids.iter().any(|id| id == &actor_id) {
            existing.actor_ids.push(actor_id);
            existing
                .actor_ids
                .sort_by(|left, right| left.0.cmp(&right.0));
        }
        existing.updated_ms = updated_ms;
    } else {
        message_entry.push(super::state::MessageReactionState {
            emoji,
            source_ref,
            actor_ids: vec![actor_id],
            updated_ms,
        });
        message_entry.sort_by(|left, right| left.emoji.cmp(&right.emoji));
    }
}

fn toggle_runtime_reaction(
    state: &mut UiState,
    conversation_id: ConversationId,
    message_id: MessageId,
    emoji: String,
    actor_id: UserId,
    updated_ms: i64,
) {
    let reactions_by_message = state
        .backend
        .message_reactions
        .entry(conversation_id)
        .or_default();
    let remove_message;
    {
        let message_entry = reactions_by_message.entry(message_id.clone()).or_default();
        if let Some(existing) = message_entry
            .iter_mut()
            .find(|reaction| reaction.emoji.eq_ignore_ascii_case(&emoji))
        {
            if existing.actor_ids.iter().any(|id| id == &actor_id) {
                existing.actor_ids.retain(|id| id != &actor_id);
            } else {
                existing.actor_ids.push(actor_id);
                existing
                    .actor_ids
                    .sort_by(|left, right| left.0.cmp(&right.0));
            }
            existing.updated_ms = updated_ms;
        } else {
            message_entry.push(super::state::MessageReactionState {
                emoji,
                source_ref: None,
                actor_ids: vec![actor_id],
                updated_ms,
            });
            message_entry.sort_by(|left, right| left.emoji.cmp(&right.emoji));
        }
        message_entry.retain(|entry| !entry.actor_ids.is_empty());
        remove_message = message_entry.is_empty();
    }
    if remove_message {
        reactions_by_message.remove(&message_id);
    }
}

fn next_op_id(state: &mut UiState, prefix: &str) -> OpId {
    let value = format!("{prefix}-{}", state.next_op_seq);
    state.next_op_seq += 1;
    OpId::new(value)
}

fn next_query_id(state: &mut UiState) -> QueryId {
    let value = format!("query-{}", state.next_op_seq);
    state.next_op_seq += 1;
    QueryId::new(value)
}

fn next_client_message_id(state: &mut UiState) -> ClientMessageId {
    let value = format!("client-message-{}", state.next_op_seq);
    state.next_op_seq += 1;
    ClientMessageId::new(value)
}

const TIMELINE_WINDOW_MAX: usize = 5000;

fn compare_message_ids(left: &MessageId, right: &MessageId) -> std::cmp::Ordering {
    let left_num = left.0.parse::<u64>().ok();
    let right_num = right.0.parse::<u64>().ok();

    match (left_num, right_num) {
        (Some(left_num), Some(right_num)) => {
            left_num.cmp(&right_num).then_with(|| left.0.cmp(&right.0))
        }
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.0.cmp(&right.0),
    }
}

fn normalize_messages_by_id(messages: &mut Vec<crate::domain::message::MessageRecord>) {
    if messages.len() <= 1 {
        return;
    }
    let mut latest_by_id = HashMap::with_capacity(messages.len());
    for message in std::mem::take(messages) {
        match latest_by_id.entry(message.id.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(message);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if should_replace_duplicate(entry.get(), &message) {
                    entry.insert(message);
                }
            }
        }
    }
    let mut normalized = latest_by_id.into_values().collect::<Vec<_>>();
    normalized.sort_by(|left, right| compare_message_ids(&left.id, &right.id));
    *messages = normalized;
}

fn should_replace_duplicate(
    existing: &crate::domain::message::MessageRecord,
    incoming: &crate::domain::message::MessageRecord,
) -> bool {
    const NON_TEXT_PLACEHOLDER_BODY: &str = "<non-text message>";
    let existing_is_placeholder = existing.event.is_none()
        && existing.attachments.is_empty()
        && existing.link_previews.is_empty()
        && matches!(
            existing.fragments.as_slice(),
            [crate::domain::message::MessageFragment::Text(text)] if text.trim() == NON_TEXT_PLACEHOLDER_BODY
        );
    let incoming_is_placeholder = incoming.event.is_none()
        && incoming.attachments.is_empty()
        && incoming.link_previews.is_empty()
        && matches!(
            incoming.fragments.as_slice(),
            [crate::domain::message::MessageFragment::Text(text)] if text.trim() == NON_TEXT_PLACEHOLDER_BODY
        );
    if existing_is_placeholder != incoming_is_placeholder {
        return existing_is_placeholder;
    }
    match (&existing.edited, &incoming.edited) {
        (None, Some(_)) => true,
        (Some(_), None) => false,
        (Some(a), Some(b)) => b.edited_at_ms.unwrap_or(0) > a.edited_at_ms.unwrap_or(0),
        (None, None) => incoming.timestamp_ms.unwrap_or(0) > existing.timestamp_ms.unwrap_or(0),
    }
}

fn trim_timeline_for_append(messages: &mut Vec<crate::domain::message::MessageRecord>) {
    if messages.len() <= TIMELINE_WINDOW_MAX {
        return;
    }
    let overflow = messages.len() - TIMELINE_WINDOW_MAX;
    messages.drain(0..overflow);
}

fn trim_timeline_for_prepend(messages: &mut Vec<crate::domain::message::MessageRecord>) {
    if messages.len() <= TIMELINE_WINDOW_MAX {
        return;
    }
    messages.truncate(TIMELINE_WINDOW_MAX);
}

fn refresh_thread_reply_count(
    messages: &mut [crate::domain::message::MessageRecord],
    root_id: &crate::domain::ids::MessageId,
) {
    let reply_count = messages
        .iter()
        .filter(|message| {
            message.id != *root_id && message.thread_root_id.as_ref() == Some(root_id)
        })
        .map(|message| message.id.clone())
        .collect::<HashSet<_>>()
        .len() as u32;
    if let Some(root) = messages.iter_mut().find(|message| message.id == *root_id) {
        root.thread_reply_count = root.thread_reply_count.max(reply_count);
    }
}

fn recompute_thread_reply_counts(messages: &mut [crate::domain::message::MessageRecord]) {
    let mut reply_count_by_root = HashMap::new();
    for message in messages.iter() {
        let Some(root_id) = message.thread_root_id.as_ref() else {
            continue;
        };
        if message.id == *root_id {
            continue;
        }
        *reply_count_by_root.entry(root_id.clone()).or_insert(0u32) += 1;
    }
    for message in messages.iter_mut() {
        if let Some(reply_count) = reply_count_by_root.get(&message.id) {
            message.thread_reply_count = message.thread_reply_count.max(*reply_count);
        }
    }
}

fn apply_message_reactions(state: &mut UiState, message: &crate::domain::message::MessageRecord) {
    let entry = state
        .backend
        .message_reactions
        .entry(message.conversation_id.clone())
        .or_default();
    if message.reactions.is_empty() {
        entry.remove(&message.id);
        return;
    }
    let mut reactions = message
        .reactions
        .iter()
        .map(|reaction| super::state::MessageReactionState {
            emoji: reaction.emoji.clone(),
            source_ref: reaction.source_ref.clone(),
            actor_ids: reaction.actor_ids.clone(),
            updated_ms: 0,
        })
        .collect::<Vec<_>>();
    reactions.sort_by(|left, right| left.emoji.cmp(&right.emoji));
    entry.insert(message.id.clone(), reactions);
}

fn merge_conversation_summaries(
    existing: &mut Vec<ConversationSummary>,
    incoming: Vec<ConversationSummary>,
) {
    let mut index_by_id = existing
        .iter()
        .enumerate()
        .map(|(index, summary)| (summary.id.clone(), index))
        .collect::<HashMap<_, _>>();
    for summary in incoming {
        if let Some(index) = index_by_id.get(&summary.id).copied() {
            existing[index] = summary;
        } else {
            index_by_id.insert(summary.id.clone(), existing.len());
            existing.push(summary);
        }
    }
}

fn build_sidebar_sections(
    channels: &[ConversationSummary],
    direct_messages: &[ConversationSummary],
    workspace_id: &WorkspaceId,
    user_profiles: &HashMap<UserId, UserProfileState>,
) -> Vec<UiSidebarSectionState> {
    let mut sections = Vec::new();
    let mut unread_rows = Vec::new();

    for summary in channels.iter().chain(direct_messages.iter()) {
        if summary.muted {
            continue;
        }
        let is_dm = matches!(
            summary.kind,
            ConversationKind::DirectMessage | ConversationKind::GroupDirectMessage
        );
        let include_in_unread = summary.unread_count > 0 || summary.mention_count > 0;
        if !include_in_unread {
            continue;
        }
        let label = if is_dm {
            dm_display_label(&summary.title, user_profiles)
        } else if let Some(group) = &summary.group {
            format!("{} #{}", group.display_name, summary.title)
        } else {
            format!("#{}", summary.title)
        };
        unread_rows.push((
            summary.last_activity_ms,
            UiSidebarRowState {
                label,
                unread_count: summary.unread_count,
                mention_count: summary.mention_count,
                route: Some(route_for_summary(summary, workspace_id)),
            },
        ));
    }
    unread_rows.sort_by(|left, right| right.0.cmp(&left.0));
    let unread_rows = unread_rows
        .into_iter()
        .map(|(_, row)| row)
        .collect::<Vec<_>>();
    if !unread_rows.is_empty() {
        sections.push(UiSidebarSectionState {
            id: Some(SidebarSectionId::new("unread")),
            title: "Unread".to_string(),
            rows: unread_rows,
            collapsed: false,
        });
    }

    let mut sorted_channels = channels.to_vec();
    sorted_channels.sort_by(|left, right| {
        left.title
            .to_ascii_lowercase()
            .cmp(&right.title.to_ascii_lowercase())
            .then_with(|| left.id.0.cmp(&right.id.0))
    });

    let mut group_order: Vec<(String, String)> = Vec::new();
    let mut group_rows: std::collections::HashMap<String, Vec<UiSidebarRowState>> =
        std::collections::HashMap::new();
    let mut ungrouped_rows = Vec::new();

    for summary in &sorted_channels {
        let row = UiSidebarRowState {
            label: summary.title.clone(),
            unread_count: summary.unread_count,
            mention_count: summary.mention_count,
            route: Some(route_for_summary(summary, workspace_id)),
        };

        if let Some(group) = &summary.group {
            if !group_rows.contains_key(&group.id) {
                group_order.push((group.id.clone(), group.display_name.clone()));
            }
            group_rows.entry(group.id.clone()).or_default().push(row);
        } else {
            ungrouped_rows.push(row);
        }
    }

    group_order.sort_by(|left, right| {
        left.1
            .to_ascii_lowercase()
            .cmp(&right.1.to_ascii_lowercase())
            .then_with(|| left.0.cmp(&right.0))
    });

    for (group_id, display_name) in group_order {
        if let Some(rows) = group_rows.remove(&group_id) {
            let has_unread = rows
                .iter()
                .any(|r| r.unread_count > 0 || r.mention_count > 0);
            sections.push(UiSidebarSectionState {
                id: Some(SidebarSectionId::new(format!("group_{group_id}"))),
                title: display_name,
                rows,
                collapsed: !has_unread,
            });
        }
    }

    if !ungrouped_rows.is_empty() {
        let has_unread = ungrouped_rows
            .iter()
            .any(|r| r.unread_count > 0 || r.mention_count > 0);
        sections.push(UiSidebarSectionState {
            id: Some(SidebarSectionId::new("channels")),
            title: "Channels".to_string(),
            rows: ungrouped_rows,
            collapsed: !has_unread,
        });
    }

    // Keep backend order for DMs so sidebar reflects recency.
    let dm_rows = direct_messages
        .iter()
        .map(|summary| UiSidebarRowState {
            label: dm_display_label(&summary.title, user_profiles),
            unread_count: summary.unread_count,
            mention_count: summary.mention_count,
            route: Some(route_for_summary(summary, workspace_id)),
        })
        .collect::<Vec<_>>();
    if !dm_rows.is_empty() {
        sections.push(UiSidebarSectionState {
            id: Some(SidebarSectionId::new("dms")),
            title: "DMs".to_string(),
            rows: dm_rows,
            collapsed: false,
        });
    }

    sections
}

fn dm_display_label(title: &str, user_profiles: &HashMap<UserId, UserProfileState>) -> String {
    let mut participants = Vec::new();
    for username in title
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let display_name = user_profiles
            .get(&UserId::new(username.to_ascii_lowercase()))
            .map(|profile| profile.display_name.trim())
            .filter(|name| !name.is_empty())
            .filter(|name| !name.eq_ignore_ascii_case(username))
            .unwrap_or(username);
        participants.push(display_name.to_string());
    }
    if participants.is_empty() {
        title.to_string()
    } else {
        participants.join(", ")
    }
}

fn find_existing_direct_message_with_user(
    direct_messages: &[ConversationSummary],
    user_id: &UserId,
) -> Option<ConversationId> {
    direct_messages
        .iter()
        .find(|summary| {
            matches!(summary.kind, ConversationKind::DirectMessage)
                && dm_title_mentions_user(&summary.title, &user_id.0)
        })
        .map(|summary| summary.id.clone())
}

fn dm_title_mentions_user(title: &str, user_id: &str) -> bool {
    let target = user_id.trim().trim_start_matches('@');
    if target.is_empty() {
        return false;
    }
    title
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .any(|participant| participant.eq_ignore_ascii_case(target))
}

fn route_for_summary(summary: &ConversationSummary, workspace_id: &WorkspaceId) -> Route {
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

fn route_for_conversation_id(
    conversation_id: &ConversationId,
    workspace_id: &WorkspaceId,
    channels: &[ConversationSummary],
    direct_messages: &[ConversationSummary],
) -> Option<Route> {
    if conversation_id.0.is_empty() {
        return None;
    }
    if direct_messages
        .iter()
        .any(|summary| summary.id == *conversation_id)
    {
        return Some(Route::DirectMessage {
            workspace_id: workspace_id.clone(),
            dm_id: DmId::new(conversation_id.0.clone()),
        });
    }
    if channels
        .iter()
        .any(|summary| summary.id == *conversation_id)
    {
        return Some(Route::Channel {
            workspace_id: workspace_id.clone(),
            channel_id: ChannelId::new(conversation_id.0.clone()),
        });
    }
    Some(Route::Channel {
        workspace_id: workspace_id.clone(),
        channel_id: ChannelId::new(conversation_id.0.clone()),
    })
}

fn conversation_id_from_route(route: &Route) -> Option<ConversationId> {
    match route {
        Route::Channel { channel_id, .. } => Some(ConversationId::new(channel_id.0.clone())),
        Route::DirectMessage { dm_id, .. } => Some(ConversationId::new(dm_id.0.clone())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::ids::UserId,
        domain::message::MessageSendState,
        state::event::{TeamRoleEntry, TeamRoleKind},
        state::state::TimelineKey,
    };

    fn message(
        conversation_id: &ConversationId,
        id: &str,
        timestamp_ms: i64,
    ) -> crate::domain::message::MessageRecord {
        crate::domain::message::MessageRecord {
            id: MessageId::new(id.to_string()),
            conversation_id: conversation_id.clone(),
            author_id: UserId::new("alice"),
            reply_to: None,
            thread_root_id: None,
            timestamp_ms: Some(timestamp_ms),
            event: None,
            link_previews: Vec::new(),
            permalink: format!("kb://{id}"),
            fragments: vec![crate::domain::message::MessageFragment::Text(format!(
                "m{id}"
            ))],
            source_text: None,
            attachments: Vec::new(),
            reactions: Vec::new(),
            thread_reply_count: 0,
            send_state: MessageSendState::Sent,
            edited: None,
        }
    }

    fn dm_summary(id: &str, title: &str) -> ConversationSummary {
        ConversationSummary {
            id: ConversationId::new(id),
            title: title.to_string(),
            kind: ConversationKind::DirectMessage,
            topic: String::new(),
            group: None,
            unread_count: 0,
            mention_count: 0,
            muted: false,
            last_activity_ms: 0,
        }
    }

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

    fn profile_with_follow_status(user_id: &str, you_are_following: bool) -> UserProfile {
        UserProfile {
            user_id: UserId::new(user_id),
            username: user_id.to_string(),
            display_name: user_id.to_string(),
            avatar_asset: None,
            presence: Presence {
                availability: crate::domain::presence::Availability::Unknown,
                status_text: None,
            },
            affinity: if you_are_following {
                Affinity::Positive
            } else {
                Affinity::None
            },
            bio: None,
            location: None,
            title: None,
            sections: vec![ProfileSection::SocialGraph(SocialGraph {
                followers_count: Some(0),
                following_count: Some(0),
                is_following_you: false,
                you_are_following,
                followers: Some(Vec::new()),
                following: Some(Vec::new()),
            })],
        }
    }

    fn profile_following_flag(profile: &UserProfile) -> bool {
        profile
            .sections
            .iter()
            .find_map(|section| match section {
                ProfileSection::SocialGraph(graph) => Some(graph.you_are_following),
                _ => None,
            })
            .unwrap_or(false)
    }

    #[test]
    fn unread_section_includes_channels_with_unread_messages() {
        let workspace_id = WorkspaceId::new("ws");
        let mut channel = channel_summary("kb_conv:general", "general");
        channel.unread_count = 2;
        channel.last_activity_ms = 2_000;

        let mut dm = dm_summary("kb_conv:dm_alice", "alice");
        dm.unread_count = 1;
        dm.last_activity_ms = 1_000;

        let sections = build_sidebar_sections(&[channel], &[dm], &workspace_id, &HashMap::new());
        let unread = sections
            .iter()
            .find(|section| section.id.as_ref().is_some_and(|id| id.0 == "unread"))
            .expect("unread section should exist");

        assert!(
            unread.rows.iter().any(|row| {
                matches!(
                    row.route,
                    Some(Route::Channel { ref channel_id, .. }) if channel_id.0 == "kb_conv:general"
                )
            }),
            "unread section should include channels with unread_count > 0"
        );
        assert!(
            unread.rows.iter().any(|row| {
                matches!(
                    row.route,
                    Some(Route::DirectMessage { ref dm_id, .. }) if dm_id.0 == "kb_conv:dm_alice"
                )
            }),
            "unread section should keep unread direct messages"
        );
    }

    #[test]
    fn follow_user_ui_action_updates_follow_status_optimistically() {
        let mut state = UiState::default();
        let user_id = UserId::new("cmmarslender");
        state.backend.profile_panel.profiles.insert(
            user_id.clone(),
            profile_with_follow_status(&user_id.0, false),
        );

        let output = reduce_ui_action(
            &mut state,
            UiAction::FollowUser {
                user_id: user_id.clone(),
            },
        );

        assert_eq!(output.effects.len(), 1);
        assert!(matches!(
            &output.effects[0],
            Effect::Backend(BackendCommand::FollowUser { user_id: effected })
                if effected == &user_id
        ));
        assert_eq!(
            state.backend.user_affinities.get(&user_id),
            Some(&Affinity::Positive)
        );
        assert!(profile_following_flag(
            state
                .backend
                .profile_panel
                .profiles
                .get(&user_id)
                .expect("profile should exist")
        ));
    }

    #[test]
    fn follow_status_change_failed_reverts_optimistic_follow_toggle() {
        let mut state = UiState::default();
        let user_id = UserId::new("cmmarslender");
        state.backend.profile_panel.profiles.insert(
            user_id.clone(),
            profile_with_follow_status(&user_id.0, false),
        );
        let _ = reduce_ui_action(
            &mut state,
            UiAction::FollowUser {
                user_id: user_id.clone(),
            },
        );

        reduce_backend_event(
            &mut state,
            BackendEvent::FollowStatusChangeFailed {
                user_id: user_id.clone(),
                attempted_follow: true,
                error: "rpc failed".to_string(),
            },
        );

        assert_eq!(
            state.backend.user_affinities.get(&user_id),
            Some(&Affinity::None)
        );
        assert!(!profile_following_flag(
            state
                .backend
                .profile_panel
                .profiles
                .get(&user_id)
                .expect("profile should exist")
        ));
    }

    #[test]
    fn follow_status_changed_updates_lowercase_profile_key_without_forced_refresh() {
        let mut state = UiState::default();
        let lower = UserId::new("cmmarslender");
        state
            .backend
            .profile_panel
            .profiles
            .insert(lower.clone(), profile_with_follow_status(&lower.0, false));

        let output = reduce_backend_event(
            &mut state,
            BackendEvent::FollowStatusChanged {
                user_id: UserId::new("CMMARSLENDER"),
                you_are_following: true,
            },
        );

        assert!(output.effects.is_empty());
        assert!(profile_following_flag(
            state
                .backend
                .profile_panel
                .profiles
                .get(&lower)
                .expect("profile should exist")
        ));
    }

    #[test]
    fn open_or_create_direct_message_navigates_existing_direct_message() {
        let mut state = UiState::default();
        state.workspace.active_workspace_id = Some(WorkspaceId::new("ws_primary"));
        state.workspace.direct_messages.push(dm_summary(
            "kb_conv:dm-existing",
            "cameroncooper,cmmarslender",
        ));

        let output = reduce_ui_action(
            &mut state,
            UiAction::OpenOrCreateDirectMessage {
                user_id: UserId::new("cmmarslender"),
            },
        );

        assert_eq!(
            state.timeline.conversation_id,
            Some(ConversationId::new("kb_conv:dm-existing"))
        );
        assert!(matches!(
            state.navigation.current_route,
            Some(Route::DirectMessage { dm_id, .. }) if dm_id.0 == "kb_conv:dm-existing"
        ));
        assert_eq!(output.effects.len(), 1);
        assert!(matches!(
            &output.effects[0],
            Effect::Backend(BackendCommand::LoadConversation { conversation_id })
                if conversation_id.0 == "kb_conv:dm-existing"
        ));
    }

    #[test]
    fn open_or_create_direct_message_creates_when_missing() {
        let mut state = UiState::default();

        let output = reduce_ui_action(
            &mut state,
            UiAction::OpenOrCreateDirectMessage {
                user_id: UserId::new("cmmarslender"),
            },
        );

        assert_eq!(output.effects.len(), 1);
        assert!(matches!(
            &output.effects[0],
            Effect::Backend(BackendCommand::CreateConversation {
                participants,
                kind: ConversationKind::DirectMessage,
                ..
            }) if participants.len() == 1 && participants[0].0 == "cmmarslender"
        ));
    }

    #[test]
    fn refresh_thread_reply_count_counts_all_descendants() {
        let conversation_id = ConversationId::new("kb_conv:test");
        let root_id = MessageId::new("100");

        let mut root = message(&conversation_id, "100", 1_000);
        root.thread_reply_count = 1;

        let mut direct = message(&conversation_id, "101", 1_001);
        direct.reply_to = Some(root_id.clone());
        direct.thread_root_id = Some(root_id.clone());

        let mut nested = message(&conversation_id, "102", 1_002);
        nested.reply_to = Some(MessageId::new("101"));
        nested.thread_root_id = Some(root_id.clone());

        let mut messages = vec![root, direct, nested];
        refresh_thread_reply_count(&mut messages, &root_id);

        let root = messages
            .iter()
            .find(|message| message.id == root_id)
            .expect("root should exist");
        assert_eq!(
            root.thread_reply_count, 2,
            "all descendants should contribute to thread reply count"
        );
    }

    #[test]
    fn refresh_thread_reply_count_dedupes_descendants_by_message_id() {
        let conversation_id = ConversationId::new("kb_conv:test");
        let root_id = MessageId::new("200");

        let mut root = message(&conversation_id, "200", 2_000);
        root.thread_reply_count = 0;

        let mut first = message(&conversation_id, "201", 2_001);
        first.reply_to = Some(root_id.clone());
        first.thread_root_id = Some(root_id.clone());

        let mut second = message(&conversation_id, "202", 2_002);
        second.reply_to = Some(root_id.clone());
        second.thread_root_id = Some(root_id.clone());

        let mut third = message(&conversation_id, "203", 2_003);
        third.reply_to = Some(root_id.clone());
        third.thread_root_id = Some(root_id.clone());

        let mut duplicate_second = second.clone();
        duplicate_second.timestamp_ms = Some(2_100);

        let mut messages = vec![root, first, second, third, duplicate_second];
        refresh_thread_reply_count(&mut messages, &root_id);

        let root = messages
            .iter()
            .find(|message| message.id == root_id)
            .expect("root should exist");
        assert_eq!(root.thread_reply_count, 3);
    }

    #[test]
    fn team_roles_updated_populates_conversation_and_team_role_indexes() {
        let mut state = UiState::default();
        let conversation_id = ConversationId::new("kb_conv:team");
        let team_id = "kb_team:abcd".to_string();

        reduce_backend_event(
            &mut state,
            BackendEvent::TeamRolesUpdated {
                conversation_id: conversation_id.clone(),
                team_id: team_id.clone(),
                roles: vec![
                    TeamRoleEntry {
                        user_id: UserId::new("alice"),
                        role: TeamRoleKind::Owner,
                    },
                    TeamRoleEntry {
                        user_id: UserId::new("bob"),
                        role: TeamRoleKind::Admin,
                    },
                ],
                updated_ms: 123,
            },
        );

        assert_eq!(
            state.backend.conversation_team_ids.get(&conversation_id),
            Some(&team_id)
        );
        let role_index = state
            .backend
            .team_roles
            .get(&team_id)
            .expect("team role index should be present");
        assert_eq!(
            role_index.get(&UserId::new("alice")),
            Some(&TeamRoleKind::Owner)
        );
        assert_eq!(
            role_index.get(&UserId::new("bob")),
            Some(&TeamRoleKind::Admin)
        );
    }

    #[test]
    fn messages_prepended_prefers_existing_duplicate_and_keeps_id_order() {
        let mut state = UiState::default();
        let conversation_id = ConversationId::new("kb_conv:test");
        state.timeline.conversation_id = Some(conversation_id.clone());
        state.timeline.messages = vec![
            message(&conversation_id, "200", 2_000),
            message(&conversation_id, "300", 3_000),
        ];

        reduce_backend_event(
            &mut state,
            BackendEvent::MessagesPrepended {
                key: TimelineKey::Conversation(conversation_id.clone()),
                messages: vec![
                    message(&conversation_id, "100", 1_000),
                    message(&conversation_id, "200", 1_500),
                ],
                cursor: Some("100".to_string()),
            },
        );

        let ids = state
            .timeline
            .messages
            .iter()
            .map(|message| message.id.0.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["100", "200", "300"]);
        let middle = state
            .timeline
            .messages
            .iter()
            .find(|message| message.id.0 == "200")
            .expect("id 200 should exist");
        assert_eq!(middle.timestamp_ms, Some(2_000));
    }

    #[test]
    fn message_upserted_resorts_timeline_by_message_id() {
        let mut state = UiState::default();
        let conversation_id = ConversationId::new("kb_conv:test");
        state.timeline.conversation_id = Some(conversation_id.clone());
        state.timeline.messages = vec![
            message(&conversation_id, "100", 1_000),
            message(&conversation_id, "300", 3_000),
        ];

        reduce_backend_event(
            &mut state,
            BackendEvent::MessageUpserted(message(&conversation_id, "200", 2_000)),
        );

        let ids = state
            .timeline
            .messages
            .iter()
            .map(|message| message.id.0.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["100", "200", "300"]);
    }
}
