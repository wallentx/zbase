use crate::domain::{ids::MessageId, route::Route};

#[derive(Clone, Debug)]
pub enum ToastAction {
    OpenPreferences,
    FocusComposer,
    OpenThread,
    FocusThreadReply,
    OpenCurrentConversation,
    OpenActiveCall,
}

impl ToastAction {
    pub fn label(&self) -> &'static str {
        match self {
            Self::OpenPreferences => "Open preferences",
            Self::FocusComposer => "Jump to composer",
            Self::OpenThread => "Open thread",
            Self::FocusThreadReply => "Reply in thread",
            Self::OpenCurrentConversation => "View conversation",
            Self::OpenActiveCall => "Open call",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ToastNotification {
    pub title: String,
    pub action: Option<ToastAction>,
}

#[derive(Clone, Debug)]
pub enum ActivityKind {
    Mention,
    ThreadReply,
    Reaction,
    Reminder,
}

#[derive(Clone, Debug)]
pub struct ActivityItem {
    pub kind: ActivityKind,
    pub title: String,
    pub detail: String,
    pub route: Route,
    pub message_id: Option<MessageId>,
    pub unread: bool,
}

#[derive(Clone, Debug)]
pub struct NotificationsModel {
    pub toasts: Vec<ToastNotification>,
    pub notification_center_count: u32,
    pub activity_items: Vec<ActivityItem>,
    pub highlighted_index: Option<usize>,
}
