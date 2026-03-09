use crate::domain::{
    ids::{ConversationId, MessageId},
    route::Route,
    search::SearchFilter,
};

use super::state::TimelineKey;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DraftKey {
    Conversation(ConversationId),
    Thread(MessageId),
}

#[derive(Clone, Debug)]
pub enum UiAction {
    StartApp,
    Navigate(Route),
    NavigateBack,
    OpenThread {
        root_id: MessageId,
    },
    CloseRightPane,
    SetSidebarFilter(String),
    SetSearchQuery(String),
    SubmitSearch,
    QuickSwitcherSearch {
        seq: u64,
        query: String,
    },
    FindInChatSearch {
        seq: u64,
        conversation_id: ConversationId,
        query: String,
    },
    ToggleSearchFilter(SearchFilter),
    UpdateDraft {
        key: DraftKey,
        text: String,
    },
    SendMessage {
        key: DraftKey,
    },
    StartCall {
        conversation_id: ConversationId,
    },
    MarkConversationRead {
        conversation_id: ConversationId,
        message_id: Option<MessageId>,
    },
    JumpToMessage {
        conversation_id: ConversationId,
        message_id: MessageId,
    },
    LoadOlderMessages {
        key: TimelineKey,
        cursor: String,
    },
}
