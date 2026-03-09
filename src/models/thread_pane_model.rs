use crate::{
    domain::{attachment::AttachmentSummary, ids::MessageId, message::MessageRecord},
    models::composer_model::AutocompleteState,
};

#[derive(Clone, Debug)]
pub struct ThreadPaneModel {
    pub open: bool,
    pub root_message_id: Option<MessageId>,
    pub width_px: f32,
    pub following: bool,
    pub replies: Vec<MessageRecord>,
    pub reply_draft: String,
    pub reply_attachments: Vec<AttachmentSummary>,
    pub reply_autocomplete: Option<AutocompleteState>,
    pub loading: bool,
}
