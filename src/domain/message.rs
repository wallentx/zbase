use crate::domain::{
    attachment::AttachmentSummary,
    backend::BackendId,
    ids::{ConversationId, MessageId, UserId},
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LinkPreview {
    pub url: String,
    pub video_url: Option<String>,
    pub title: Option<String>,
    pub site: Option<String>,
    pub description: Option<String>,
    pub thumbnail_asset: Option<String>,
    pub is_media: bool,
    pub media_width: Option<u32>,
    pub media_height: Option<u32>,
    pub is_video: bool,
}

#[derive(Clone, Debug)]
pub enum BroadcastKind {
    Here,
    All,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EmojiSourceRef {
    pub backend_id: BackendId,
    pub ref_key: String,
}

impl EmojiSourceRef {
    pub fn cache_key(&self) -> String {
        format!("{}:{}", self.backend_id.0, self.ref_key)
    }
}

#[derive(Clone, Debug)]
pub enum MessageFragment {
    Text(String),
    InlineCode(String),
    Emoji {
        alias: String,
        source_ref: Option<EmojiSourceRef>,
    },
    Mention(UserId),
    ChannelMention {
        name: String,
    },
    BroadcastMention(BroadcastKind),
    Link {
        url: String,
        display: String,
    },
    Code(String),
    Quote(String),
}

#[derive(Clone, Debug)]
pub enum ChatEvent {
    MemberJoined,
    MemberLeft,
    MembersAdded {
        user_ids: Vec<UserId>,
        role: Option<String>,
    },
    MembersRemoved {
        user_ids: Vec<UserId>,
    },
    DescriptionChanged {
        description: Option<String>,
    },
    ChannelRenamed {
        new_name: String,
    },
    AvatarChanged,
    MessagePinned {
        target_message_id: Option<MessageId>,
    },
    MessageDeleted {
        target_message_id: Option<MessageId>,
    },
    HistoryCleared,
    RetentionChanged {
        summary: String,
    },
    Other {
        text: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessageSendState {
    Sent,
    Pending,
    Failed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MessageReaction {
    pub emoji: String,
    pub source_ref: Option<EmojiSourceRef>,
    pub actor_ids: Vec<UserId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EditMeta {
    pub edit_id: MessageId,
    pub edited_at_ms: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct MessageRecord {
    pub id: MessageId,
    pub conversation_id: ConversationId,
    pub author_id: UserId,
    pub reply_to: Option<MessageId>,
    pub thread_root_id: Option<MessageId>,
    pub timestamp_ms: Option<i64>,
    pub event: Option<ChatEvent>,
    pub link_previews: Vec<LinkPreview>,
    pub permalink: String,
    pub fragments: Vec<MessageFragment>,
    pub source_text: Option<String>,
    pub attachments: Vec<AttachmentSummary>,
    pub reactions: Vec<MessageReaction>,
    pub thread_reply_count: u32,
    pub send_state: MessageSendState,
    pub edited: Option<EditMeta>,
}
