use crate::domain::{
    ids::{ConversationId, MessageId, UserId},
    message::MessageRecord,
    user::UserSummary,
};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct InlineEmojiRender {
    pub alias: String,
    pub unicode: Option<String>,
    pub asset_path: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ReactionActorRender {
    pub user_id: String,
    pub display_name: String,
}

#[derive(Clone, Debug)]
pub struct MessageReactionRender {
    pub emoji: String,
    pub count: usize,
    pub actors: Vec<ReactionActorRender>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TeamAuthorRole {
    Admin,
    Owner,
}

#[derive(Clone, Debug)]
pub struct MessageRow {
    pub author: UserSummary,
    pub message: MessageRecord,
    pub show_header: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SystemEventIcon {
    Join,
    Leave,
    Add,
    Remove,
    Pin,
    Description,
    Settings,
    Info,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventSpan {
    Actor(String),
    Text(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SystemEventRow {
    pub icon: SystemEventIcon,
    pub spans: Vec<EventSpan>,
}

#[derive(Clone, Debug)]
pub enum TimelineRow {
    DateDivider(String),
    UnreadDivider(String),
    SystemEvent(SystemEventRow),
    Message(MessageRow),
    TypingIndicator(String),
    LoadingIndicator(String),
}

#[derive(Clone, Debug)]
pub struct TimelineModel {
    pub conversation_id: ConversationId,
    pub rows: Vec<TimelineRow>,
    pub highlighted_message_id: Option<MessageId>,
    pub unread_marker: Option<MessageId>,
    pub emoji_index: HashMap<String, InlineEmojiRender>,
    pub reaction_index: HashMap<MessageId, Vec<MessageReactionRender>>,
    pub author_role_index: HashMap<UserId, TeamAuthorRole>,
    pub pending_scroll_target: Option<MessageId>,
    pub older_cursor: Option<String>,
    pub newer_cursor: Option<String>,
    pub loading_older: bool,
}

impl TimelineModel {
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}
