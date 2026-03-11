use crate::app::assets::KbuiAssets;
use crate::app::commands;
use crate::app::window::open_main_window;
use crate::views::input::{
    Backspace, Copy, Cut, Delete, Down, End, Home, InsertNewline, Left, Paste, Redo, Right,
    SelectAll, SelectDown, SelectEnd, SelectHome, SelectLeft, SelectRight, SelectUp,
    ShowCharacterPalette, Undo, Up,
};
use gpui::{Application, KeyBinding};

pub fn run() {
    let application = Application::new().with_assets(KbuiAssets {});

    // On macOS, clicking the Dock icon to re-open an already-running app should restore a window.
    // If we have no windows, create one.
    application.on_reopen(|cx| {
        if cfg!(target_os = "macos") && cx.windows().is_empty() {
            open_main_window(cx);
        }
    });

    application.run(|cx| {
        // On platforms without a Dock/Menu affordance, quit when the last window closes.
        if !cfg!(target_os = "macos") {
            cx.on_window_closed(|cx| {
                if cx.windows().is_empty() {
                    cx.quit();
                }
            })
            .detach();
        }

        cx.bind_keys([
            KeyBinding::new("cmd-[", commands::NavigateBack, Some("Workspace")),
            KeyBinding::new("cmd-]", commands::NavigateForward, Some("Workspace")),
            KeyBinding::new("cmd-shift-a", commands::ShowActivity, Some("Workspace")),
            KeyBinding::new("cmd-,", commands::OpenPreferences, Some("Workspace")),
            KeyBinding::new(
                "cmd-shift-\\",
                commands::ToggleThreadPane,
                Some("Workspace"),
            ),
            KeyBinding::new(
                "cmd-shift-m",
                commands::ToggleMembersPane,
                Some("Workspace"),
            ),
            KeyBinding::new(
                "cmd-shift-d",
                commands::ToggleDetailsPane,
                Some("Workspace"),
            ),
            KeyBinding::new("cmd-shift-f", commands::OpenFilesPane, Some("Workspace")),
            KeyBinding::new(
                "cmd-shift-s",
                commands::ToggleSplashScreen,
                Some("Workspace"),
            ),
            KeyBinding::new("cmd-1", commands::ShowHome, Some("Workspace")),
            KeyBinding::new(
                "cmd-2",
                commands::OpenQuickSwitcherRecent2,
                Some("Workspace"),
            ),
            KeyBinding::new(
                "cmd-3",
                commands::OpenQuickSwitcherRecent3,
                Some("Workspace"),
            ),
            KeyBinding::new(
                "cmd-4",
                commands::OpenQuickSwitcherRecent4,
                Some("Workspace"),
            ),
            KeyBinding::new(
                "cmd-5",
                commands::OpenQuickSwitcherRecent5,
                Some("Workspace"),
            ),
            KeyBinding::new("cmd-j", commands::OpenSearch, Some("Workspace")),
            KeyBinding::new("cmd-f", commands::ToggleFindInChat, Some("Workspace")),
            KeyBinding::new("enter", commands::ConfirmPrimary, Some("Workspace")),
            KeyBinding::new("up", commands::SelectPrevious, Some("Workspace")),
            KeyBinding::new("down", commands::SelectNext, Some("Workspace")),
            KeyBinding::new("tab", commands::SelectNext, Some("QuickSwitcherTextField")),
            KeyBinding::new(
                "shift-tab",
                commands::SelectPrevious,
                Some("QuickSwitcherTextField"),
            ),
            KeyBinding::new("backspace", Backspace, Some("QuickSwitcherTextField")),
            KeyBinding::new("delete", Delete, Some("QuickSwitcherTextField")),
            KeyBinding::new("left", Left, Some("QuickSwitcherTextField")),
            KeyBinding::new("right", Right, Some("QuickSwitcherTextField")),
            KeyBinding::new("shift-left", SelectLeft, Some("QuickSwitcherTextField")),
            KeyBinding::new("shift-right", SelectRight, Some("QuickSwitcherTextField")),
            KeyBinding::new("cmd-a", SelectAll, Some("QuickSwitcherTextField")),
            KeyBinding::new("cmd-v", Paste, Some("QuickSwitcherTextField")),
            KeyBinding::new("cmd-c", Copy, Some("QuickSwitcherTextField")),
            KeyBinding::new("cmd-x", Cut, Some("QuickSwitcherTextField")),
            KeyBinding::new("cmd-z", Undo, Some("QuickSwitcherTextField")),
            KeyBinding::new("cmd-shift-z", Redo, Some("QuickSwitcherTextField")),
            KeyBinding::new("home", Home, Some("QuickSwitcherTextField")),
            KeyBinding::new("end", End, Some("QuickSwitcherTextField")),
            KeyBinding::new("shift-home", SelectHome, Some("QuickSwitcherTextField")),
            KeyBinding::new("shift-end", SelectEnd, Some("QuickSwitcherTextField")),
            KeyBinding::new("ctrl-a", Home, Some("QuickSwitcherTextField")),
            KeyBinding::new("ctrl-e", End, Some("QuickSwitcherTextField")),
            KeyBinding::new("ctrl-d", Delete, Some("QuickSwitcherTextField")),
            KeyBinding::new(
                "ctrl-cmd-space",
                ShowCharacterPalette,
                Some("QuickSwitcherTextField"),
            ),
            KeyBinding::new("alt-up", commands::SelectSidebarPrevious, Some("Workspace")),
            KeyBinding::new("alt-down", commands::SelectSidebarNext, Some("Workspace")),
            KeyBinding::new(
                "alt-enter",
                commands::ActivateSidebarSelection,
                Some("Workspace"),
            ),
            KeyBinding::new("cmd-n", commands::OpenNewChat, Some("Workspace")),
            KeyBinding::new("cmd-k", commands::ToggleQuickSwitcher, Some("Workspace")),
            KeyBinding::new(
                "cmd-shift-p",
                commands::ToggleCommandPalette,
                Some("Workspace"),
            ),
            KeyBinding::new(
                "cmd-shift-i",
                commands::ToggleKeybaseInspector,
                Some("Workspace"),
            ),
            KeyBinding::new(
                "cmd-shift-b",
                commands::ToggleBenchmarkCapture,
                Some("Workspace"),
            ),
            KeyBinding::new("escape", commands::DismissOverlays, Some("Workspace")),
            KeyBinding::new(
                "enter",
                commands::FindNextMatch,
                Some("FindInChatTextField"),
            ),
            KeyBinding::new(
                "shift-enter",
                commands::FindPrevMatch,
                Some("FindInChatTextField"),
            ),
            KeyBinding::new(
                "escape",
                commands::CloseFindInChat,
                Some("FindInChatTextField"),
            ),
            KeyBinding::new("backspace", Backspace, Some("TextField")),
            KeyBinding::new("delete", Delete, Some("TextField")),
            KeyBinding::new("left", Left, Some("TextField")),
            KeyBinding::new("right", Right, Some("TextField")),
            KeyBinding::new("shift-left", SelectLeft, Some("TextField")),
            KeyBinding::new("shift-right", SelectRight, Some("TextField")),
            KeyBinding::new("cmd-a", SelectAll, Some("TextField")),
            KeyBinding::new("cmd-v", Paste, Some("TextField")),
            KeyBinding::new("cmd-c", Copy, Some("TextField")),
            KeyBinding::new("cmd-x", Cut, Some("TextField")),
            KeyBinding::new("cmd-z", Undo, Some("TextField")),
            KeyBinding::new("cmd-shift-z", Redo, Some("TextField")),
            KeyBinding::new("home", Home, Some("TextField")),
            KeyBinding::new("end", End, Some("TextField")),
            KeyBinding::new("shift-home", SelectHome, Some("TextField")),
            KeyBinding::new("shift-end", SelectEnd, Some("TextField")),
            KeyBinding::new("ctrl-a", Home, Some("TextField")),
            KeyBinding::new("ctrl-e", End, Some("TextField")),
            KeyBinding::new("ctrl-d", Delete, Some("TextField")),
            KeyBinding::new("ctrl-cmd-space", ShowCharacterPalette, Some("TextField")),
            KeyBinding::new("backspace", Backspace, Some("MultilineTextField")),
            KeyBinding::new("delete", Delete, Some("MultilineTextField")),
            KeyBinding::new("left", Left, Some("MultilineTextField")),
            KeyBinding::new("right", Right, Some("MultilineTextField")),
            KeyBinding::new("up", Up, Some("MultilineTextField")),
            KeyBinding::new("down", Down, Some("MultilineTextField")),
            KeyBinding::new("shift-left", SelectLeft, Some("MultilineTextField")),
            KeyBinding::new("shift-right", SelectRight, Some("MultilineTextField")),
            KeyBinding::new("shift-up", SelectUp, Some("MultilineTextField")),
            KeyBinding::new("shift-down", SelectDown, Some("MultilineTextField")),
            KeyBinding::new("cmd-a", SelectAll, Some("MultilineTextField")),
            KeyBinding::new("cmd-v", Paste, Some("MultilineTextField")),
            KeyBinding::new("cmd-c", Copy, Some("MultilineTextField")),
            KeyBinding::new("cmd-x", Cut, Some("MultilineTextField")),
            KeyBinding::new("cmd-z", Undo, Some("MultilineTextField")),
            KeyBinding::new("cmd-shift-z", Redo, Some("MultilineTextField")),
            KeyBinding::new("home", Home, Some("MultilineTextField")),
            KeyBinding::new("end", End, Some("MultilineTextField")),
            KeyBinding::new("shift-home", SelectHome, Some("MultilineTextField")),
            KeyBinding::new("shift-end", SelectEnd, Some("MultilineTextField")),
            KeyBinding::new("ctrl-a", Home, Some("MultilineTextField")),
            KeyBinding::new("ctrl-e", End, Some("MultilineTextField")),
            KeyBinding::new("ctrl-d", Delete, Some("MultilineTextField")),
            KeyBinding::new("shift-enter", InsertNewline, Some("MultilineTextField")),
            KeyBinding::new(
                "ctrl-cmd-space",
                ShowCharacterPalette,
                Some("MultilineTextField"),
            ),
            KeyBinding::new("left", Left, Some("SelectableText")),
            KeyBinding::new("right", Right, Some("SelectableText")),
            KeyBinding::new("up", Up, Some("SelectableText")),
            KeyBinding::new("down", Down, Some("SelectableText")),
            KeyBinding::new("shift-left", SelectLeft, Some("SelectableText")),
            KeyBinding::new("shift-right", SelectRight, Some("SelectableText")),
            KeyBinding::new("shift-up", SelectUp, Some("SelectableText")),
            KeyBinding::new("shift-down", SelectDown, Some("SelectableText")),
            KeyBinding::new("cmd-a", SelectAll, Some("SelectableText")),
            KeyBinding::new("cmd-c", Copy, Some("SelectableText")),
            KeyBinding::new("home", Home, Some("SelectableText")),
            KeyBinding::new("end", End, Some("SelectableText")),
            KeyBinding::new("shift-home", SelectHome, Some("SelectableText")),
            KeyBinding::new("shift-end", SelectEnd, Some("SelectableText")),
        ]);
        open_main_window(cx);
    });
}
