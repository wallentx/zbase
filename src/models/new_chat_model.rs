use crate::domain::user::UserSummary;

#[derive(Clone, Debug, Default)]
pub struct NewChatModel {
    pub open: bool,
    pub search_query: String,
    pub search_results: Vec<UserSummary>,
    pub selected_participants: Vec<UserSummary>,
    pub creating: bool,
    pub error: Option<String>,
}
