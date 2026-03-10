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
        OpenQuickSwitcherRecent2,
        OpenQuickSwitcherRecent3,
        OpenQuickSwitcherRecent4,
        OpenQuickSwitcherRecent5,
        SelectPrevious,
        SelectNext,
        SelectSidebarPrevious,
        SelectSidebarNext,
        ActivateSidebarSelection,
        ToggleQuickSwitcher,
        OpenNewChat,
        ToggleFindInChat,
        CloseFindInChat,
        FindNextMatch,
        FindPrevMatch,
        ToggleCommandPalette,
        ToggleKeybaseInspector,
        ToggleBenchmarkCapture,
        DismissOverlays,
        EditLastMessage,
        CancelEdit,
        ToggleSplashScreen,
    ]
);

#[derive(Clone, PartialEq, Debug, Action)]
#[action(namespace = kbui, no_json)]
pub struct OpenUrl {
    pub url: String,
}
