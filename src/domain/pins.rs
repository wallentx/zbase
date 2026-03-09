use crate::domain::ids::{MessageId, UserId};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PinnedState {
    pub items: Vec<PinnedItem>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PinnedItem {
    pub id: String,
    pub target: PinnedTarget,
    pub pinned_by: Option<UserId>,
    pub pinned_at_ms: Option<i64>,
    pub preview: Option<PinnedPreview>,
}

impl PinnedItem {
    pub fn message_id(&self) -> Option<&MessageId> {
        match &self.target {
            PinnedTarget::Message { message_id } => Some(message_id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PinnedTarget {
    Message { message_id: MessageId },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PinnedPreview {
    pub author_label: Option<String>,
    pub text: Option<String>,
}
