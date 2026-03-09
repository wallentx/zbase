use crate::domain::{ids::SidebarSectionId, route::Route};

#[derive(Clone, Debug)]
pub struct SidebarRow {
    pub label: String,
    pub unread_count: u32,
    pub mention_count: u32,
    pub route: Route,
}

#[derive(Clone, Debug)]
pub struct SidebarSection {
    pub id: SidebarSectionId,
    pub title: String,
    pub rows: Vec<SidebarRow>,
    pub collapsed: bool,
}

#[derive(Clone, Debug)]
pub struct SidebarModel {
    pub sections: Vec<SidebarSection>,
    pub filter: String,
    pub highlighted_route: Option<Route>,
}
