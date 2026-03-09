use crate::domain::{
    ids::{ConversationId, MessageId},
    route::Route,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QuickSwitcherResultKind {
    UnreadChannel,
    Channel,
    DirectMessage,
    Message,
}

#[derive(Clone, Debug)]
pub struct QuickSwitcherResult {
    pub label: String,
    pub sublabel: Option<String>,
    pub kind: QuickSwitcherResultKind,
    pub route: Route,
    pub conversation_id: ConversationId,
    pub message_id: Option<MessageId>,
    pub match_ranges: Vec<(usize, usize)>,
}

#[derive(Clone, Debug, Default)]
pub struct QuickSwitcherModel {
    pub query: String,
    pub results: Vec<QuickSwitcherResult>,
    pub selected_index: usize,
    pub loading_messages: bool,
}
