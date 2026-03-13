use serde::{Deserialize, Serialize};

use crate::{
    domain::{
        attachment::{AttachmentKind, AttachmentPreview, AttachmentSource, AttachmentSummary},
        conversation::{ConversationGroup, ConversationKind, ConversationSummary},
        ids::{ConversationId, MessageId, UserId, WorkspaceId},
        message::{
            BroadcastKind, ChatEvent, EditMeta, LinkPreview, MessageFragment, MessageRecord,
            MessageSendState,
        },
    },
    state::bindings::{ConversationBinding, MessageBinding},
};

pub const SCHEMA_VERSION: u32 = 4;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedConversationGroup {
    pub id: String,
    pub display_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedConversationSummary {
    pub id: String,
    pub title: String,
    pub kind: String,
    pub topic: String,
    pub group: Option<CachedConversationGroup>,
    pub unread_count: u32,
    pub mention_count: u32,
    pub muted: bool,
    pub last_activity_time: i64,
}

impl CachedConversationSummary {
    pub fn from_domain(summary: &ConversationSummary, last_activity_time: i64) -> Self {
        Self {
            id: summary.id.0.clone(),
            title: summary.title.clone(),
            kind: conversation_kind_label(&summary.kind).to_string(),
            topic: summary.topic.clone(),
            group: summary.group.as_ref().map(|group| CachedConversationGroup {
                id: group.id.clone(),
                display_name: group.display_name.clone(),
            }),
            unread_count: summary.unread_count,
            mention_count: summary.mention_count,
            muted: summary.muted,
            last_activity_time,
        }
    }

    pub fn to_domain(&self) -> ConversationSummary {
        ConversationSummary {
            id: ConversationId::new(self.id.clone()),
            title: self.title.clone(),
            kind: parse_conversation_kind(&self.kind),
            topic: self.topic.clone(),
            group: self.group.as_ref().map(|group| ConversationGroup {
                id: group.id.clone(),
                display_name: group.display_name.clone(),
            }),
            unread_count: self.unread_count,
            mention_count: self.mention_count,
            muted: self.muted,
            last_activity_ms: self.last_activity_time,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedLinkPreview {
    pub url: String,
    #[serde(default)]
    pub video_url: Option<String>,
    pub title: Option<String>,
    pub site: Option<String>,
    pub description: Option<String>,
    pub thumbnail_asset: Option<String>,
    #[serde(default)]
    pub is_media: bool,
    #[serde(default)]
    pub media_width: Option<u32>,
    #[serde(default)]
    pub media_height: Option<u32>,
    #[serde(default)]
    pub is_video: bool,
}

impl CachedLinkPreview {
    pub fn from_domain(preview: &LinkPreview) -> Self {
        Self {
            url: preview.url.clone(),
            video_url: preview.video_url.clone(),
            title: preview.title.clone(),
            site: preview.site.clone(),
            description: preview.description.clone(),
            thumbnail_asset: preview.thumbnail_asset.clone(),
            is_media: preview.is_media,
            media_width: preview.media_width,
            media_height: preview.media_height,
            is_video: preview.is_video,
        }
    }

    pub fn to_domain(&self) -> LinkPreview {
        LinkPreview {
            url: self.url.clone(),
            video_url: self.video_url.clone(),
            title: self.title.clone(),
            site: self.site.clone(),
            description: self.description.clone(),
            thumbnail_asset: self.thumbnail_asset.clone(),
            is_media: self.is_media,
            media_width: self.media_width,
            media_height: self.media_height,
            is_video: self.is_video,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "fragment_type", rename_all = "snake_case")]
pub enum CachedMessageFragment {
    Text {
        text: String,
    },
    InlineCode {
        text: String,
    },
    Emoji {
        alias: String,
        #[serde(default)]
        source_ref: Option<CachedEmojiSourceRef>,
    },
    Mention {
        user_id: String,
    },
    ChannelMention {
        name: String,
    },
    BroadcastMention {
        kind: String,
    },
    Link {
        url: String,
        display: String,
    },
    Code {
        text: String,
        #[serde(default)]
        lang: Option<String>,
    },
    Quote {
        text: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedEmojiSourceRef {
    pub backend_id: String,
    pub ref_key: String,
}

impl CachedMessageFragment {
    pub fn from_domain(fragment: &MessageFragment) -> Self {
        match fragment {
            MessageFragment::Text(text) => Self::Text { text: text.clone() },
            MessageFragment::InlineCode(text) => Self::InlineCode { text: text.clone() },
            MessageFragment::Emoji { alias, source_ref } => Self::Emoji {
                alias: alias.clone(),
                source_ref: source_ref.as_ref().map(|source_ref| CachedEmojiSourceRef {
                    backend_id: source_ref.backend_id.0.clone(),
                    ref_key: source_ref.ref_key.clone(),
                }),
            },
            MessageFragment::Mention(user_id) => Self::Mention {
                user_id: user_id.0.clone(),
            },
            MessageFragment::ChannelMention { name } => Self::ChannelMention { name: name.clone() },
            MessageFragment::BroadcastMention(kind) => Self::BroadcastMention {
                kind: broadcast_kind_label(kind).to_string(),
            },
            MessageFragment::Link { url, display } => Self::Link {
                url: url.clone(),
                display: display.clone(),
            },
            MessageFragment::Code { text, lang } => Self::Code {
                text: text.clone(),
                lang: lang.clone(),
            },
            MessageFragment::Quote(text) => Self::Quote { text: text.clone() },
        }
    }

    pub fn to_domain(&self) -> Option<MessageFragment> {
        match self {
            Self::Text { text } => Some(MessageFragment::Text(text.clone())),
            Self::InlineCode { text } => Some(MessageFragment::InlineCode(text.clone())),
            Self::Emoji { alias, source_ref } => Some(MessageFragment::Emoji {
                alias: alias.clone(),
                source_ref: source_ref.as_ref().map(|source_ref| {
                    crate::domain::message::EmojiSourceRef {
                        backend_id: crate::domain::backend::BackendId::new(
                            source_ref.backend_id.clone(),
                        ),
                        ref_key: source_ref.ref_key.clone(),
                    }
                }),
            }),
            Self::Mention { user_id } => {
                Some(MessageFragment::Mention(UserId::new(user_id.clone())))
            }
            Self::ChannelMention { name } => {
                Some(MessageFragment::ChannelMention { name: name.clone() })
            }
            Self::BroadcastMention { kind } => {
                parse_cached_broadcast_kind(kind).map(MessageFragment::BroadcastMention)
            }
            Self::Link { url, display } => Some(MessageFragment::Link {
                url: url.clone(),
                display: display.clone(),
            }),
            Self::Code { text, lang } => Some(MessageFragment::Code {
                text: text.clone(),
                lang: lang.clone(),
            }),
            Self::Quote { text } => Some(MessageFragment::Quote(text.clone())),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum CachedChatEvent {
    MemberJoined,
    MemberLeft,
    MembersAdded {
        #[serde(default)]
        user_ids: Vec<String>,
        role: Option<String>,
    },
    MembersRemoved {
        #[serde(default)]
        user_ids: Vec<String>,
    },
    DescriptionChanged {
        description: Option<String>,
    },
    ChannelRenamed {
        new_name: String,
    },
    AvatarChanged,
    MessagePinned {
        target_message_id: Option<String>,
    },
    MessageDeleted {
        target_message_id: Option<String>,
    },
    HistoryCleared,
    RetentionChanged {
        summary: String,
    },
    Other {
        text: String,
    },
    #[serde(other)]
    Unknown,
}

impl CachedChatEvent {
    pub fn from_domain(event: &ChatEvent) -> Self {
        match event {
            ChatEvent::MemberJoined => Self::MemberJoined,
            ChatEvent::MemberLeft => Self::MemberLeft,
            ChatEvent::MembersAdded { user_ids, role } => Self::MembersAdded {
                user_ids: user_ids.iter().map(|user_id| user_id.0.clone()).collect(),
                role: role.clone(),
            },
            ChatEvent::MembersRemoved { user_ids } => Self::MembersRemoved {
                user_ids: user_ids.iter().map(|user_id| user_id.0.clone()).collect(),
            },
            ChatEvent::DescriptionChanged { description } => Self::DescriptionChanged {
                description: description.clone(),
            },
            ChatEvent::ChannelRenamed { new_name } => Self::ChannelRenamed {
                new_name: new_name.clone(),
            },
            ChatEvent::AvatarChanged => Self::AvatarChanged,
            ChatEvent::MessagePinned { target_message_id } => Self::MessagePinned {
                target_message_id: target_message_id
                    .as_ref()
                    .map(|message_id| message_id.0.clone()),
            },
            ChatEvent::MessageDeleted { target_message_id } => Self::MessageDeleted {
                target_message_id: target_message_id
                    .as_ref()
                    .map(|message_id| message_id.0.clone()),
            },
            ChatEvent::HistoryCleared => Self::HistoryCleared,
            ChatEvent::RetentionChanged { summary } => Self::RetentionChanged {
                summary: summary.clone(),
            },
            ChatEvent::Other { text } => Self::Other { text: text.clone() },
        }
    }

    pub fn to_domain(&self) -> Option<ChatEvent> {
        match self {
            Self::MemberJoined => Some(ChatEvent::MemberJoined),
            Self::MemberLeft => Some(ChatEvent::MemberLeft),
            Self::MembersAdded { user_ids, role } => Some(ChatEvent::MembersAdded {
                user_ids: user_ids.iter().cloned().map(UserId::new).collect(),
                role: role.clone(),
            }),
            Self::MembersRemoved { user_ids } => Some(ChatEvent::MembersRemoved {
                user_ids: user_ids.iter().cloned().map(UserId::new).collect(),
            }),
            Self::DescriptionChanged { description } => Some(ChatEvent::DescriptionChanged {
                description: description.clone(),
            }),
            Self::ChannelRenamed { new_name } => Some(ChatEvent::ChannelRenamed {
                new_name: new_name.clone(),
            }),
            Self::AvatarChanged => Some(ChatEvent::AvatarChanged),
            Self::MessagePinned { target_message_id } => Some(ChatEvent::MessagePinned {
                target_message_id: target_message_id.clone().map(MessageId::new),
            }),
            Self::MessageDeleted { target_message_id } => Some(ChatEvent::MessageDeleted {
                target_message_id: target_message_id.clone().map(MessageId::new),
            }),
            Self::HistoryCleared => Some(ChatEvent::HistoryCleared),
            Self::RetentionChanged { summary } => Some(ChatEvent::RetentionChanged {
                summary: summary.clone(),
            }),
            Self::Other { text } => Some(ChatEvent::Other { text: text.clone() }),
            Self::Unknown => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct CachedAttachment {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub size_bytes: u64,
    #[serde(default)]
    pub width: Option<u32>,
    #[serde(default)]
    pub height: Option<u32>,
    #[serde(default)]
    pub preview_url: Option<String>,
    #[serde(default)]
    pub preview_width: Option<u32>,
    #[serde(default)]
    pub preview_height: Option<u32>,
    #[serde(default)]
    pub duration_ms: Option<u64>,
}

impl CachedAttachment {
    pub fn from_domain(attachment: &AttachmentSummary) -> Self {
        let (preview_url, preview_width, preview_height) =
            if let Some(preview) = attachment.preview.as_ref() {
                (
                    Some(cached_attachment_source_to_string(&preview.source)),
                    preview.width,
                    preview.height,
                )
            } else {
                (None, None, None)
            };
        Self {
            name: attachment.name.clone(),
            kind: attachment_kind_label(&attachment.kind).to_string(),
            mime_type: attachment.mime_type.clone(),
            size_bytes: attachment.size_bytes,
            width: attachment.width,
            height: attachment.height,
            preview_url,
            preview_width,
            preview_height,
            duration_ms: attachment.duration_ms,
        }
    }

    pub fn to_domain(&self) -> AttachmentSummary {
        let preview = self
            .preview_url
            .as_deref()
            .and_then(cached_attachment_source_from_string)
            .map(|source| AttachmentPreview {
                source,
                width: self.preview_width,
                height: self.preview_height,
            });
        AttachmentSummary {
            name: self.name.clone(),
            kind: parse_attachment_kind(&self.kind),
            mime_type: self.mime_type.clone(),
            size_bytes: self.size_bytes,
            width: self.width,
            height: self.height,
            preview,
            duration_ms: self.duration_ms,
            waveform: None,
            source: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedMessageRecord {
    pub id: String,
    pub conversation_id: String,
    pub author_id: String,
    #[serde(default)]
    pub reply_to: Option<String>,
    #[serde(default)]
    pub thread_root_id: Option<String>,
    #[serde(default)]
    pub timestamp_ms: Option<i64>,
    #[serde(default)]
    pub event: Option<CachedChatEvent>,
    pub permalink: String,
    pub body: String,
    #[serde(default)]
    pub fragments: Vec<CachedMessageFragment>,
    #[serde(default)]
    pub attachments: Vec<CachedAttachment>,
    #[serde(default)]
    pub attachment_filenames: Vec<String>,
    #[serde(default)]
    pub link_previews: Vec<CachedLinkPreview>,
    pub thread_reply_count: u32,
    pub send_state: String,
    #[serde(default)]
    pub source_text: Option<String>,
    #[serde(default)]
    pub edit_id: Option<String>,
    #[serde(default)]
    pub edited_at_ms: Option<i64>,
}

impl CachedMessageRecord {
    pub fn from_domain(message: &MessageRecord) -> Self {
        let body = message
            .fragments
            .iter()
            .map(message_fragment_body_text)
            .collect::<Vec<_>>()
            .join("\n");

        Self {
            id: message.id.0.clone(),
            conversation_id: message.conversation_id.0.clone(),
            author_id: message.author_id.0.clone(),
            reply_to: message.reply_to.as_ref().map(|id| id.0.clone()),
            thread_root_id: message.thread_root_id.as_ref().map(|id| id.0.clone()),
            timestamp_ms: message.timestamp_ms,
            event: message.event.as_ref().map(CachedChatEvent::from_domain),
            permalink: message.permalink.clone(),
            body,
            fragments: message
                .fragments
                .iter()
                .map(CachedMessageFragment::from_domain)
                .collect(),
            attachments: message
                .attachments
                .iter()
                .map(CachedAttachment::from_domain)
                .collect(),
            attachment_filenames: Vec::new(),
            link_previews: message
                .link_previews
                .iter()
                .map(CachedLinkPreview::from_domain)
                .collect(),
            thread_reply_count: message.thread_reply_count,
            send_state: message_send_state_label(&message.send_state).to_string(),
            source_text: message.source_text.clone(),
            edit_id: message
                .edited
                .as_ref()
                .map(|edited| edited.edit_id.0.clone()),
            edited_at_ms: message
                .edited
                .as_ref()
                .and_then(|edited| edited.edited_at_ms),
        }
    }

    pub fn to_domain(&self) -> MessageRecord {
        let fragments = if self.fragments.is_empty() {
            vec![MessageFragment::Text(self.body.clone())]
        } else {
            let restored = self
                .fragments
                .iter()
                .filter_map(CachedMessageFragment::to_domain)
                .collect::<Vec<_>>();
            if restored.is_empty() {
                vec![MessageFragment::Text(self.body.clone())]
            } else {
                restored
            }
        };

        let attachments = if self.attachments.is_empty() {
            self.attachment_filenames
                .iter()
                .map(|name| AttachmentSummary {
                    name: name.clone(),
                    ..AttachmentSummary::default()
                })
                .collect()
        } else {
            self.attachments
                .iter()
                .map(CachedAttachment::to_domain)
                .collect()
        };

        MessageRecord {
            id: MessageId::new(self.id.clone()),
            conversation_id: ConversationId::new(self.conversation_id.clone()),
            author_id: UserId::new(self.author_id.clone()),
            reply_to: self.reply_to.clone().map(MessageId::new),
            thread_root_id: self.thread_root_id.clone().map(MessageId::new),
            timestamp_ms: self.timestamp_ms,
            event: self.event.as_ref().and_then(CachedChatEvent::to_domain),
            link_previews: self
                .link_previews
                .iter()
                .map(CachedLinkPreview::to_domain)
                .collect(),
            permalink: self.permalink.clone(),
            fragments,
            source_text: self.source_text.clone(),
            attachments,
            reactions: Vec::new(),
            thread_reply_count: self.thread_reply_count,
            send_state: parse_message_send_state(&self.send_state),
            edited: self.edit_id.as_ref().map(|edit_id| EditMeta {
                edit_id: MessageId::new(edit_id.clone()),
                edited_at_ms: self.edited_at_ms,
            }),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedConversationBinding {
    pub conversation_id: String,
    pub provider_ref: String,
}

impl CachedConversationBinding {
    pub fn from_domain(binding: &ConversationBinding) -> Self {
        Self {
            conversation_id: binding.conversation_id.0.clone(),
            provider_ref: binding.provider_conversation_ref.0.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedMessageBinding {
    pub message_id: String,
    pub provider_ref: String,
}

impl CachedMessageBinding {
    pub fn from_domain(binding: &MessageBinding) -> Self {
        Self {
            message_id: binding.message_id.0.clone(),
            provider_ref: binding.provider_message_ref.0.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedMeta {
    pub schema_version: u32,
    pub workspace_name: String,
    pub active_workspace_id: String,
    pub account_display_name: Option<String>,
    pub selected_conversation_id: Option<String>,
    pub unread_marker: Option<String>,
}

impl Default for CachedMeta {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            workspace_name: "Keybase".to_string(),
            active_workspace_id: WorkspaceId::new("ws_primary").0,
            account_display_name: None,
            selected_conversation_id: None,
            unread_marker: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedUserProfile {
    pub username: String,
    pub display_name: String,
    pub avatar_url: Option<String>,
    pub avatar_path: Option<String>,
    pub updated_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedConversationEmoji {
    pub alias: String,
    pub unicode: Option<String>,
    pub source_url: Option<String>,
    pub asset_path: Option<String>,
    pub updated_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedMessageReaction {
    pub message_id: String,
    pub emoji: String,
    #[serde(default)]
    pub source_ref: Option<CachedEmojiSourceRef>,
    pub actor_id: String,
    pub updated_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedReactionOp {
    pub op_message_id: String,
    pub target_message_id: String,
    pub emoji: String,
    pub actor_id: String,
    pub updated_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedTeamRoleEntry {
    pub user_id: String,
    pub role: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedTeamRoleMap {
    pub team_id: String,
    pub updated_ms: i64,
    pub roles: Vec<CachedTeamRoleEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedConversationTeamBinding {
    pub conversation_id: String,
    pub team_id: String,
    pub updated_ms: i64,
}

fn conversation_kind_label(kind: &ConversationKind) -> &'static str {
    match kind {
        ConversationKind::Channel => "channel",
        ConversationKind::DirectMessage => "direct_message",
        ConversationKind::GroupDirectMessage => "group_direct_message",
    }
}

fn parse_conversation_kind(label: &str) -> ConversationKind {
    match label {
        "direct_message" => ConversationKind::DirectMessage,
        "group_direct_message" => ConversationKind::GroupDirectMessage,
        _ => ConversationKind::Channel,
    }
}

fn message_send_state_label(state: &MessageSendState) -> &'static str {
    match state {
        MessageSendState::Sent => "sent",
        MessageSendState::Pending => "pending",
        MessageSendState::Failed => "failed",
    }
}

fn parse_message_send_state(label: &str) -> MessageSendState {
    match label {
        "pending" => MessageSendState::Pending,
        "failed" => MessageSendState::Failed,
        _ => MessageSendState::Sent,
    }
}

fn attachment_kind_label(kind: &AttachmentKind) -> &'static str {
    match kind {
        AttachmentKind::Image => "image",
        AttachmentKind::Video => "video",
        AttachmentKind::Audio => "audio",
        AttachmentKind::File => "file",
    }
}

fn parse_attachment_kind(label: &str) -> AttachmentKind {
    match label.trim().to_ascii_lowercase().as_str() {
        "image" => AttachmentKind::Image,
        "video" => AttachmentKind::Video,
        "audio" => AttachmentKind::Audio,
        _ => AttachmentKind::File,
    }
}

fn cached_attachment_source_to_string(source: &AttachmentSource) -> String {
    match source {
        AttachmentSource::Url(url) | AttachmentSource::LocalPath(url) => url.clone(),
    }
}

fn cached_attachment_source_from_string(raw: &str) -> Option<AttachmentSource> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }
    if value.starts_with("http://") || value.starts_with("https://") {
        return Some(AttachmentSource::Url(value.to_string()));
    }
    if value.starts_with("file://") {
        return Some(AttachmentSource::LocalPath(
            value.trim_start_matches("file://").to_string(),
        ));
    }
    Some(AttachmentSource::LocalPath(value.to_string()))
}

fn message_fragment_body_text(fragment: &MessageFragment) -> String {
    match fragment {
        MessageFragment::Text(text)
        | MessageFragment::Code { text, .. }
        | MessageFragment::Quote(text) => text.clone(),
        MessageFragment::InlineCode(text) => format!("`{text}`"),
        MessageFragment::Emoji { alias, .. } => format!(":{alias}:"),
        MessageFragment::Mention(user_id) => format!("@{}", user_id.0),
        MessageFragment::ChannelMention { name } => format!("#{name}"),
        MessageFragment::BroadcastMention(BroadcastKind::Here) => "@here".to_string(),
        MessageFragment::BroadcastMention(BroadcastKind::All) => "@channel".to_string(),
        MessageFragment::Link { display, .. } => display.clone(),
    }
}

fn broadcast_kind_label(kind: &BroadcastKind) -> &'static str {
    match kind {
        BroadcastKind::Here => "here",
        BroadcastKind::All => "all",
    }
}

fn parse_cached_broadcast_kind(label: &str) -> Option<BroadcastKind> {
    match label.trim().to_ascii_lowercase().as_str() {
        "here" => Some(BroadcastKind::Here),
        "all" | "channel" | "everyone" => Some(BroadcastKind::All),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_message_with_fragments() -> MessageRecord {
        MessageRecord {
            id: MessageId::new("1"),
            conversation_id: ConversationId::new("kb_conv:test"),
            author_id: UserId::new("alice"),
            reply_to: Some(MessageId::new("0")),
            thread_root_id: Some(MessageId::new("0")),
            timestamp_ms: Some(1_711_000_000_000),
            event: None,
            link_previews: Vec::new(),
            permalink: "keybase://chat/team#chat/1".to_string(),
            fragments: vec![
                MessageFragment::Text("hello ".to_string()),
                MessageFragment::Mention(UserId::new("bob")),
                MessageFragment::Text(" ".to_string()),
                MessageFragment::ChannelMention {
                    name: "general".to_string(),
                },
                MessageFragment::Text(" ".to_string()),
                MessageFragment::BroadcastMention(BroadcastKind::All),
                MessageFragment::Text(" ".to_string()),
                MessageFragment::InlineCode(":lol:".to_string()),
                MessageFragment::Text(" ".to_string()),
                MessageFragment::Link {
                    url: "https://example.com".to_string(),
                    display: "example".to_string(),
                },
                MessageFragment::Code {
                    text: "let x = 1;".to_string(),
                    lang: None,
                },
                MessageFragment::Quote("quoted".to_string()),
            ],
            source_text: None,
            attachments: Vec::new(),
            reactions: Vec::new(),
            thread_reply_count: 2,
            send_state: MessageSendState::Sent,
            edited: Some(EditMeta {
                edit_id: MessageId::new("2"),
                edited_at_ms: Some(1_711_000_123_000),
            }),
        }
    }

    #[test]
    fn cached_message_record_roundtrip_preserves_structured_fragments() {
        let original = sample_message_with_fragments();
        let cached = CachedMessageRecord::from_domain(&original);
        assert!(
            !cached.fragments.is_empty(),
            "serialized fragments should be persisted"
        );

        let restored = cached.to_domain();
        assert_eq!(restored.id.0, original.id.0);
        assert_eq!(restored.conversation_id.0, original.conversation_id.0);
        assert_eq!(restored.author_id.0, original.author_id.0);
        assert_eq!(
            restored
                .edited
                .as_ref()
                .map(|edited| edited.edit_id.0.as_str()),
            Some("2")
        );
        assert_eq!(
            restored
                .edited
                .as_ref()
                .and_then(|edited| edited.edited_at_ms),
            Some(1_711_000_123_000)
        );
        assert!(restored.fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Mention(user_id) if user_id.0 == "bob"
        )));
        assert!(restored.fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::ChannelMention { name } if name == "general"
        )));
        assert!(restored.fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::BroadcastMention(BroadcastKind::All)
        )));
        assert!(restored.fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Link { url, display }
                if url == "https://example.com" && display == "example"
        )));
        assert!(restored.fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::InlineCode(code) if code == ":lol:"
        )));
    }

    #[test]
    fn cached_message_record_roundtrip_preserves_event() {
        let mut original = sample_message_with_fragments();
        original.event = Some(ChatEvent::MessageDeleted {
            target_message_id: Some(MessageId::new("99")),
        });
        let cached = CachedMessageRecord::from_domain(&original);
        let restored = cached.to_domain();

        match restored.event {
            Some(ChatEvent::MessageDeleted {
                target_message_id: Some(target_message_id),
            }) => assert_eq!(target_message_id.0, "99"),
            _ => panic!("expected deleted-message chat event to round-trip"),
        }
    }

    #[test]
    fn cached_message_record_legacy_fallback_uses_body_text() {
        let cached = CachedMessageRecord {
            id: "2".to_string(),
            conversation_id: "kb_conv:test".to_string(),
            author_id: "alice".to_string(),
            reply_to: None,
            thread_root_id: None,
            timestamp_ms: None,
            event: None,
            permalink: String::new(),
            body: "legacy body".to_string(),
            fragments: Vec::new(),
            attachments: Vec::new(),
            attachment_filenames: Vec::new(),
            link_previews: Vec::new(),
            thread_reply_count: 0,
            send_state: "sent".to_string(),
            source_text: None,
            edit_id: None,
            edited_at_ms: None,
        };

        let restored = cached.to_domain();
        assert_eq!(restored.fragments.len(), 1);
        assert!(restored.edited.is_none());
        let MessageFragment::Text(text) = &restored.fragments[0] else {
            panic!("legacy records should load as text fragment");
        };
        assert_eq!(text, "legacy body");
    }
}
