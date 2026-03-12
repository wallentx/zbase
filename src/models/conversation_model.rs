use crate::domain::channel_details::ChannelDetails;
use crate::domain::conversation::ConversationSummary;
use crate::domain::pins::PinnedItem;

#[derive(Clone, Debug)]
pub struct ConversationModel {
    pub summary: ConversationSummary,
    pub pinned_message: Option<PinnedItem>,
    pub details: Option<ChannelDetails>,
    pub avatar_asset: Option<String>,
    pub member_count: u32,
    pub can_post: bool,
    pub is_archived: bool,
}
