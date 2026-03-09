use crate::domain::{ids::ConversationId, message::MessageRecord, route::Route};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchFilter {
    FromUser(String),
    InChannel(String),
    HasFile,
    HasLink,
    MentionsMe,
}

#[derive(Clone, Debug)]
pub struct SearchResult {
    pub conversation_id: ConversationId,
    pub route: Route,
    pub snippet: String,
    pub snippet_highlight_ranges: Vec<(usize, usize)>,
    pub message: MessageRecord,
}
