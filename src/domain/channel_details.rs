use crate::domain::{
    affinity::Affinity,
    conversation::{ConversationGroup, ConversationKind},
    ids::{ConversationId, UserId},
    pins::PinnedItem,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotificationLevel {
    All,
    MentionsOnly,
    Nothing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelMemberPreview {
    pub user_id: UserId,
    pub display_name: String,
    pub avatar_asset: Option<String>,
    pub affinity: Affinity,
}

#[derive(Clone, Debug)]
pub struct ChannelDetails {
    pub conversation_id: ConversationId,
    pub title: String,
    pub topic: String,
    pub kind: ConversationKind,
    pub group: Option<ConversationGroup>,
    pub member_count: u32,
    pub member_preview: Vec<ChannelMemberPreview>,
    pub notification_level: NotificationLevel,
    pub pinned_items: Vec<PinnedItem>,
    pub can_edit_topic: bool,
    pub can_manage_members: bool,
    pub can_archive: bool,
    pub can_leave: bool,
    pub can_post: bool,
    pub created_at: Option<String>,
    pub description: Option<String>,
    pub is_archived: bool,
}
