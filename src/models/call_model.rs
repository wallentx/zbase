use crate::domain::call::CallSessionSummary;

#[derive(Clone, Debug)]
pub struct CallModel {
    pub active_call: Option<CallSessionSummary>,
    pub is_muted: bool,
    pub is_sharing_screen: bool,
}
