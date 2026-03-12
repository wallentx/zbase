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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutocompleteCandidate {
    MentionUser {
        username: String,
        display_name: String,
        avatar_asset: Option<String>,
    },
    MentionBroadcast {
        keyword: String,
        description: String,
    },
    Emoji {
        label: String,
        insert_text: String,
        glyph: Option<String>,
    },
}

impl AutocompleteCandidate {
    pub fn completion_text(&self) -> String {
        match self {
            Self::MentionUser { username, .. } => format!("@{username} "),
            Self::MentionBroadcast { keyword, .. } => format!("@{keyword} "),
            Self::Emoji { insert_text, .. } => format!("{insert_text} "),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutocompleteState {
    pub trigger: char,
    pub query: String,
    pub trigger_offset: usize,
    pub selected_index: usize,
    pub candidates: Vec<AutocompleteCandidate>,
}

#[derive(Clone, Debug)]
pub struct ComposerModel {
    pub conversation_id: ConversationId,
    pub mode: ComposerMode,
    pub draft_text: String,
    pub attachments: Vec<AttachmentSummary>,
    pub autocomplete: Option<AutocompleteState>,
}
