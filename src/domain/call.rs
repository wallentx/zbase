use crate::domain::{
    ids::{CallId, UserId},
    user::UserSummary,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallStatus {
    Idle,
    Ringing,
    Connecting,
    ActiveAudio,
    ActiveVideo,
    SharingScreen,
    Reconnecting,
}

#[derive(Clone, Debug)]
pub struct ParticipantSummary {
    pub user_id: UserId,
    pub muted: bool,
}

#[derive(Clone, Debug)]
pub struct CallSessionSummary {
    pub call_id: CallId,
    pub status: CallStatus,
    pub participants: Vec<UserSummary>,
}
