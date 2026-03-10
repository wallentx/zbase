use crate::domain::{
    ids::{ConversationId, MessageId, UserId},
    profile::SocialGraphListType,
    route::Route,
    search::SearchFilter,
    user::UserSummary,
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
    OpenNewChat,
    CloseNewChat,
    NewChatSearchUsers {
        query: String,
    },
    NewChatAddParticipant {
        user: UserSummary,
    },
    NewChatRemoveParticipant {
        user_id: UserId,
    },
    NewChatCreate,
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
    EditMessage {
        conversation_id: ConversationId,
        message_id: MessageId,
        text: String,
    },
    DeleteMessage {
        conversation_id: ConversationId,
        message_id: MessageId,
    },
    SendAttachment {
        key: DraftKey,
        local_path: String,
        filename: String,
        caption: String,
    },
    ReactToMessage {
        conversation_id: ConversationId,
        message_id: MessageId,
        emoji: String,
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
    ShowUserProfileCard {
        user_id: UserId,
    },
    ShowUserProfilePanel {
        user_id: UserId,
    },
    OpenOrCreateDirectMessage {
        user_id: UserId,
    },
    RefreshProfilePresence {
        user_id: UserId,
        conversation_id: Option<ConversationId>,
    },
    LoadSocialGraphList {
        user_id: UserId,
        list_type: SocialGraphListType,
    },
    FollowUser {
        user_id: UserId,
    },
    UnfollowUser {
        user_id: UserId,
    },
}
