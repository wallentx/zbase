use crate::domain::ids::WorkspaceId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Connectivity {
    Online,
    Reconnecting,
    Offline,
}

#[derive(Clone, Debug)]
pub struct AppModel {
    pub open_workspaces: Vec<WorkspaceId>,
    pub active_workspace_id: WorkspaceId,
    pub connectivity: Connectivity,
    pub global_unread_count: u32,
    pub current_user_display_name: String,
    pub current_user_avatar_asset: Option<String>,
}
