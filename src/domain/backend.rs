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

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct BackendCapabilities {
    pub supports_threads: bool,
    pub supports_message_edit: bool,
    pub supports_message_delete: bool,
    pub supports_reactions: bool,
    pub supports_custom_emoji: bool,
    pub supports_user_affinity: bool,
    pub supports_pins: bool,
    pub supports_uploads: bool,
    pub supports_presence: bool,
    pub supports_typing: bool,
    pub supports_calls: bool,
    pub supports_global_search: bool,
    pub supports_conversation_search: bool,
    pub supports_user_search: bool,
    pub supports_mark_unread: bool,
    pub supports_scheduled_send: bool,
    pub supports_create_conversation: bool,
    pub supports_user_profiles: bool,
    pub supports_identity_proofs: bool,
    pub supports_social_graph: bool,
}

impl BackendCapabilities {
    pub fn keybase_defaults() -> Self {
        Self {
            supports_threads: true,
            supports_message_edit: true,
            supports_message_delete: true,
            supports_reactions: true,
            supports_custom_emoji: true,
            supports_user_affinity: true,
            supports_pins: true,
            supports_uploads: true,
            supports_presence: true,
            supports_typing: true,
            supports_calls: true,
            supports_global_search: true,
            supports_conversation_search: true,
            supports_user_search: true,
            supports_mark_unread: true,
            supports_scheduled_send: false,
            supports_create_conversation: true,
            supports_user_profiles: true,
            supports_identity_proofs: true,
            supports_social_graph: true,
        }
    }
}
