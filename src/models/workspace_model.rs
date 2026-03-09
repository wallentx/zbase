use crate::domain::{conversation::ConversationSummary, ids::WorkspaceId};

#[derive(Clone, Debug)]
pub struct WorkspaceModel {
    pub workspace_id: WorkspaceId,
    pub workspace_name: String,
    pub channels: Vec<ConversationSummary>,
    pub direct_messages: Vec<ConversationSummary>,
}
