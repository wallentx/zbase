use crate::domain::{
    attachment::AttachmentSummary,
    ids::{ConversationId, MessageId},
};

#[derive(Clone, Debug)]
pub enum ComposerMode {
    Compose,
    Edit { message_id: MessageId },
    ReplyInThread { root_id: MessageId },
}

#[derive(Clone, Debug)]
pub struct AutocompleteState {
    pub trigger: char,
    pub query: String,
}

#[derive(Clone, Debug)]
pub struct ComposerModel {
    pub conversation_id: ConversationId,
    pub mode: ComposerMode,
    pub draft_text: String,
    pub attachments: Vec<AttachmentSummary>,
    pub autocomplete: Option<AutocompleteState>,
}
