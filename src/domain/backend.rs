#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BackendId(pub String);

impl BackendId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl Default for BackendId {
    fn default() -> Self {
        Self::new("backend")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AccountId(pub String);

impl AccountId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl Default for AccountId {
    fn default() -> Self {
        Self::new("account")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProviderWorkspaceRef(pub String);

impl ProviderWorkspaceRef {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProviderConversationRef(pub String);

impl ProviderConversationRef {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProviderMessageRef(pub String);

impl ProviderMessageRef {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendCapabilities {
    pub supports_threads: bool,
    pub supports_message_edit: bool,
    pub supports_message_delete: bool,
    pub supports_reactions: bool,
    pub supports_pins: bool,
    pub supports_uploads: bool,
    pub supports_presence: bool,
    pub supports_typing: bool,
    pub supports_calls: bool,
    pub supports_global_search: bool,
    pub supports_conversation_search: bool,
    pub supports_mark_unread: bool,
    pub supports_scheduled_send: bool,
}

impl BackendCapabilities {
    pub fn keybase_defaults() -> Self {
        Self {
            supports_threads: true,
            supports_message_edit: true,
            supports_message_delete: true,
            supports_reactions: true,
            supports_pins: true,
            supports_uploads: true,
            supports_presence: true,
            supports_typing: true,
            supports_calls: true,
            supports_global_search: true,
            supports_conversation_search: true,
            supports_mark_unread: true,
            supports_scheduled_send: false,
        }
    }
}

impl Default for BackendCapabilities {
    fn default() -> Self {
        Self {
            supports_threads: false,
            supports_message_edit: false,
            supports_message_delete: false,
            supports_reactions: false,
            supports_pins: false,
            supports_uploads: false,
            supports_presence: false,
            supports_typing: false,
            supports_calls: false,
            supports_global_search: false,
            supports_conversation_search: false,
            supports_mark_unread: false,
            supports_scheduled_send: false,
        }
    }
}
