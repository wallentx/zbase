use crate::domain::conversation::ConversationSummary;
use crate::domain::pins::PinnedItem;

#[derive(Clone, Debug)]
pub struct ConversationModel {
    pub summary: ConversationSummary,
    pub pinned_message: Option<PinnedItem>,
    pub avatar_asset: Option<String>,
    pub member_count: u32,
    pub can_post: bool,
    pub is_archived: bool,
}
