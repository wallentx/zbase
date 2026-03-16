use crate::domain::{
    affinity::Affinity,
    ids::{ConversationId, MessageId, UserId},
    message::{EmojiSourceRef, MessageRecord},
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
    pub source_ref: Option<EmojiSourceRef>,
    pub count: usize,
    pub actors: Vec<ReactionActorRender>,
    pub reacted_by_me: bool,
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
    ChannelLink {
        channel_name: String,
        team_name: Option<String>,
        conv_id: Option<ConversationId>,
    },
    UserLink(UserId),
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
    LoadingIndicator(String),
}

#[derive(Clone, Debug)]
pub struct TimelineModel {
    pub conversation_id: ConversationId,
    pub current_user_id: Option<UserId>,
    pub rows: Vec<TimelineRow>,
    pub highlighted_message_id: Option<MessageId>,
    pub editing_message_id: Option<MessageId>,
    pub unread_marker: Option<MessageId>,
    pub affinity_index: HashMap<UserId, Affinity>,
    pub emoji_index: HashMap<String, InlineEmojiRender>,
    pub emoji_source_index: HashMap<String, InlineEmojiRender>,
    pub reaction_index: HashMap<MessageId, Vec<MessageReactionRender>>,
    pub author_role_index: HashMap<UserId, TeamAuthorRole>,
    pub pending_scroll_target: Option<MessageId>,
    pub older_cursor: Option<String>,
    pub newer_cursor: Option<String>,
    pub loading_older: bool,
    pub hovered_message_id: Option<MessageId>,
    pub hovered_message_is_thread: Option<bool>,
    /// Cursor X position (window coords) captured on hover.
    pub hovered_message_anchor_x: Option<f32>,
    /// Cursor Y position (window coords) captured when the hover settled.
    pub hovered_message_anchor_y: Option<f32>,
    /// Hovered message container left edge in window coords.
    pub hovered_message_window_left: Option<f32>,
    /// Hovered message container top edge in window coords.
    pub hovered_message_window_top: Option<f32>,
    /// Hovered message container width in window coords.
    pub hovered_message_window_width: Option<f32>,
    /// Whether the hover toolbar is settled (mouse was still long enough).
    pub hover_toolbar_settled: bool,
    pub typing_text: Option<String>,
    pub quick_react_recent: Option<QuickReactRecent>,
}

#[derive(Clone, Debug)]
pub struct QuickReactRecent {
    pub alias: String,
    pub unicode: Option<String>,
    pub asset_path: Option<String>,
}

impl TimelineModel {
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}
