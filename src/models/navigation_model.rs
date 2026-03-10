use crate::domain::ids::UserId;
use crate::domain::route::Route;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RightPaneMode {
    Hidden,
    Thread,
    Details,
    Members,
    Files,
    Search,
    Profile(UserId),
}

#[derive(Clone, Debug)]
pub struct NavigationModel {
    pub current: Route,
    pub back_stack: Vec<Route>,
    pub forward_stack: Vec<Route>,
    pub right_pane: RightPaneMode,
}

impl NavigationModel {
    pub fn navigate(&mut self, route: Route) {
        self.back_stack.push(self.current.clone());
        self.current = route;
        self.forward_stack.clear();
    }

    pub fn back(&mut self) -> Option<Route> {
        let route = self.back_stack.pop()?;
        self.forward_stack.push(self.current.clone());
        self.current = route.clone();
        Some(route)
    }

    pub fn forward(&mut self) -> Option<Route> {
        let route = self.forward_stack.pop()?;
        self.back_stack.push(self.current.clone());
        self.current = route.clone();
        Some(route)
    }

    pub fn set_right_pane(&mut self, pane: RightPaneMode) {
        self.right_pane = pane;
    }
}
