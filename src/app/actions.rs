use crate::domain::{ids::MessageId, route::Route};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppAction {
    Navigate(Route),
    OpenThread { root_id: MessageId },
    ToggleRightPane,
    SendMessage,
    OpenPreferences,
    ShowQuickSwitcher,
}
