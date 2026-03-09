use gpui::{Action, actions};

actions!(
    kbui,
    [
        NavigateBack,
        NavigateForward,
        ShowHome,
        ShowActivity,
        OpenPreferences,
        ToggleThreadPane,
        ToggleMembersPane,
        ToggleDetailsPane,
        OpenFilesPane,
        OpenSearchPane,
        OpenSearch,
        ConfirmPrimary,
        SelectPrevious,
        SelectNext,
        SelectSidebarPrevious,
        SelectSidebarNext,
        ActivateSidebarSelection,
        ToggleQuickSwitcher,
        ToggleFindInChat,
        CloseFindInChat,
        FindNextMatch,
        FindPrevMatch,
        ToggleCommandPalette,
        ToggleKeybaseInspector,
        ToggleBenchmarkCapture,
        DismissOverlays,
    ]
);

#[derive(Clone, PartialEq, Debug, Action)]
#[action(namespace = kbui, no_json)]
pub struct OpenUrl {
    pub url: String,
}
