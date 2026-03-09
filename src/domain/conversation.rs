use crate::domain::ids::ConversationId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConversationKind {
    Channel,
    DirectMessage,
    GroupDirectMessage,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ConversationGroup {
    pub id: String,
    pub display_name: String,
}

#[derive(Clone, Debug)]
pub struct ConversationSummary {
    pub id: ConversationId,
    pub title: String,
    pub kind: ConversationKind,
    pub topic: String,
    pub group: Option<ConversationGroup>,
    pub unread_count: u32,
    pub mention_count: u32,
    pub muted: bool,
    pub last_activity_ms: i64,
}
