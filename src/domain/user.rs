use crate::domain::{ids::UserId, presence::Presence};

#[derive(Clone, Debug)]
pub struct UserSummary {
    pub id: UserId,
    pub display_name: String,
    pub title: String,
    pub avatar_asset: Option<String>,
    pub presence: Presence,
}
