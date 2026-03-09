use crate::domain::search::{SearchFilter, SearchResult};

#[derive(Clone, Debug)]
pub struct SearchModel {
    pub query: String,
    pub filters: Vec<SearchFilter>,
    pub results: Vec<SearchResult>,
    pub highlighted_index: Option<usize>,
    pub is_loading: bool,
}
