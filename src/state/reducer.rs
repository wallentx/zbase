use crate::domain::{
    backend::AccountId,
    conversation::{ConversationKind, ConversationSummary},
    ids::{ChannelId, ConversationId, DmId, MessageId, SidebarSectionId, UserId, WorkspaceId},
    route::Route,
};
use std::collections::{HashMap, HashSet};

use super::{
    action::{DraftKey, UiAction},
    effect::{BackendCommand, Effect, SearchScope},
    event::{AppEvent, BackendEvent},
    ids::{ClientMessageId, OpId, QueryId},
    state::{
        BootPhase, ConnectionState, DraftState, UiSidebarRowState, UiSidebarSectionState, UiState,
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

            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::LoadConversation {
                    conversation_id,
                })],
            }
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
            state
                .drafts
                .entry(key.clone())
                .or_insert_with(DraftState::default)
                .text = text;
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
                DraftKey::Conversation(conversation_id) => ReducerOutput {
                    effects: vec![Effect::Backend(BackendCommand::SendMessage {
                        op_id: next_op_id(state, "send"),
                        draft_key: key,
                        conversation_id,
                        client_message_id: next_client_message_id(state),
                        text: draft_text,
                        attachments: Vec::new(),
                        reply_to: None,
                    })],
                },
                DraftKey::Thread(root_id) => {
                    let Some(conversation_id) = state.timeline.conversation_id.clone() else {
                        return ReducerOutput::default();
                    };
                    ReducerOutput {
                        effects: vec![Effect::Backend(BackendCommand::SendMessage {
                            op_id: next_op_id(state, "send"),
                            draft_key: key,
                            conversation_id,
                            client_message_id: next_client_message_id(state),
                            text: draft_text,
                            attachments: Vec::new(),
                            reply_to: Some(root_id),
                        })],
                    }
                }
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
            if let Some(account) = state.backend.accounts.get_mut(&account_id) {
                if let Some(display_name) = payload.account_display_name {
                    account.display_name = display_name;
                }
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
                    for message in &messages {
                        apply_message_reactions(state, message);
                    }
                    let mut merged = messages;
                    merged.extend(state.timeline.messages.clone());
                    normalize_messages_by_id(&mut merged);
                    trim_timeline_for_prepend(&mut merged);
                    for message in merged.clone() {
                        if let Some(root_id) = message.thread_root_id.clone() {
                            refresh_thread_reply_count(&mut merged, &root_id);
                        }
                    }
                    state.timeline.messages = merged;
                    state.timeline.older_cursor = cursor;
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
                let mut replaced = messages;
                normalize_messages_by_id(&mut replaced);
                for message in replaced.clone() {
                    if let Some(root_id) = message.thread_root_id.clone() {
                        refresh_thread_reply_count(&mut replaced, &root_id);
                    }
                }
                state.timeline.messages = replaced;
                state.timeline.older_cursor = older_cursor;
                state.timeline.newer_cursor = newer_cursor;
                state.timeline.loading_older = false;
            }
        }
        BackendEvent::MessageUpserted(message) => {
            if state.timeline.conversation_id.as_ref() != Some(&message.conversation_id) {
                // Keep the active timeline stable; incoming events for other conversations
                // should not force navigation.
                return ReducerOutput::default();
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
            if let Some(root_id) = message.thread_root_id.clone() {
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
            actor_id,
            updated_ms,
        } => {
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
                    actor_ids: vec![actor_id],
                    updated_ms,
                });
                message_entry.sort_by(|left, right| left.emoji.cmp(&right.emoji));
            }
        }
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
        BackendEvent::PresenceUpdated { .. } | BackendEvent::CallUpdated(_) => {}
        BackendEvent::SearchResults {
            query_id: _,
            results,
            is_complete,
        } => {
            state.search.results.extend(results);
            state.search.highlighted_index = (!state.search.results.is_empty()).then_some(0);
            state.search.is_loading = !is_complete;
        }
        BackendEvent::KeybaseNotifyStub { .. } => {}
    }

    ReducerOutput::default()
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

const TIMELINE_WINDOW_MAX: usize = 800;

fn compare_message_ids(left: &MessageId, right: &MessageId) -> std::cmp::Ordering {
    match (left.0.parse::<u64>().ok(), right.0.parse::<u64>().ok()) {
        (Some(left), Some(right)) => left.cmp(&right),
        _ => left.0.cmp(&right.0),
    }
}

fn normalize_messages_by_id(messages: &mut Vec<crate::domain::message::MessageRecord>) {
    if messages.len() <= 1 {
        return;
    }
    let mut latest_by_id = HashMap::with_capacity(messages.len());
    for message in std::mem::take(messages) {
        latest_by_id.insert(message.id.clone(), message);
    }
    let mut normalized = latest_by_id.into_values().collect::<Vec<_>>();
    normalized.sort_by(|left, right| compare_message_ids(&left.id, &right.id));
    *messages = normalized;
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
        let include_in_unread = (is_dm && summary.unread_count > 0) || summary.mention_count > 0;
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
            attachments: Vec::new(),
            reactions: Vec::new(),
            thread_reply_count: 0,
            send_state: MessageSendState::Sent,
            edited: None,
        }
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
