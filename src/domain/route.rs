use crate::domain::ids::{CallId, ChannelId, DmId, WorkspaceId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Route {
    WorkspaceHome {
        workspace_id: WorkspaceId,
    },
    Channel {
        workspace_id: WorkspaceId,
        channel_id: ChannelId,
    },
    DirectMessage {
        workspace_id: WorkspaceId,
        dm_id: DmId,
    },
    Search {
        workspace_id: WorkspaceId,
        query: String,
    },
    Activity {
        workspace_id: WorkspaceId,
    },
    Preferences,
    ActiveCall {
        workspace_id: WorkspaceId,
        call_id: CallId,
    },
}

impl Route {
    pub fn label(&self) -> String {
        match self {
            Self::WorkspaceHome { .. } => "workspace-home".to_string(),
            Self::Channel { channel_id, .. } => format!("channel:{}", channel_id.0),
            Self::DirectMessage { dm_id, .. } => format!("dm:{}", dm_id.0),
            Self::Search { query, .. } => format!("search:{query}"),
            Self::Activity { .. } => "activity".to_string(),
            Self::Preferences => "preferences".to_string(),
            Self::ActiveCall { call_id, .. } => format!("call:{}", call_id.0),
        }
    }
}
