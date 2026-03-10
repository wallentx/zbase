use crate::domain::{
    ids::UserId,
    profile::{SocialGraphListType, UserProfile},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[derive(Default)]
pub enum SocialTab {
    #[default]
    Followers,
    Following,
}

impl SocialTab {
    pub fn as_list_type(self) -> SocialGraphListType {
        match self {
            Self::Followers => SocialGraphListType::Followers,
            Self::Following => SocialGraphListType::Following,
        }
    }
}


#[derive(Clone, Debug, Default)]
pub struct ProfilePanelModel {
    pub user_id: Option<UserId>,
    pub profile: Option<UserProfile>,
    pub loading: bool,
    pub active_social_tab: SocialTab,
    pub loading_social_list: bool,
}
