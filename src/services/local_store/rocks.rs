use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode,
    MultiThreaded, Options, WriteBatch,
};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    domain::{
        backend::{ProviderConversationRef, ProviderMessageRef},
        conversation::{ConversationKind, ConversationSummary},
        ids::{ConversationId, MessageId, UserId, WorkspaceId},
        message::{EmojiSourceRef, LinkPreview, MessageReaction, MessageRecord},
    },
    state::event::BootstrapPayload,
};

use super::{
    paths,
    schema::{
        CachedConversationBinding, CachedConversationEmoji, CachedConversationSummary,
        CachedConversationTeamBinding, CachedEmojiSourceRef, CachedMessageBinding,
        CachedLinkPreview, CachedMessageReaction, CachedMessageRecord, CachedMeta, CachedReactionOp,
        CachedTeamRoleEntry, CachedTeamRoleMap, CachedUserProfile, SCHEMA_VERSION,
    },
};

const CF_META: &str = "meta";
const CF_CONVERSATIONS: &str = "conversations";
const CF_MESSAGES: &str = "messages";
const CF_ACTIVITY: &str = "activity";
const CF_BINDINGS: &str = "bindings";
const CF_USERS: &str = "users";
const CF_EMOJIS: &str = "emojis";
const CF_REACTIONS: &str = "reactions";
const CF_REACTION_OPS: &str = "reaction_ops";
const CF_TEAMS: &str = "teams";
const CF_OG: &str = "og";

const META_KEY: &[u8] = b"bootstrap_meta";
const USER_PROFILE_PREFIX: &str = "user:";
const EMOJI_PREFIX: &str = "emoji:";
const REACTION_PREFIX: &str = "rxn:";
const REACTION_OP_PREFIX: &str = "rxnop:";
const TEAM_ROLE_PREFIX: &str = "team_roles:";
const CONVERSATION_TEAM_PREFIX: &str = "conv_team:";
const CRAWL_CHECKPOINT_PREFIX: &str = "crawl:conv:";
const THREAD_INDEX_PREFIX: &str = "thread_idx:";
const THREAD_EDGE_PARENT_PREFIX: &str = "thread_edge_p2c:";
const THREAD_EDGE_CHILD_PREFIX: &str = "thread_edge_c2p:";
const THREAD_EDGE_MIGRATION_COMPLETE_KEY: &[u8] = b"thread_edge_migration_complete_v1";
const THREAD_EDGE_MIGRATION_CONVERSATION_PREFIX: &str = "thread_edge_migration_conv:";

#[derive(Clone, Debug)]
pub struct CachedBootstrapSeed {
    pub workspace_name: String,
    pub active_workspace_id: WorkspaceId,
    pub account_display_name: Option<String>,
    pub channels: Vec<ConversationSummary>,
    pub direct_messages: Vec<ConversationSummary>,
    pub selected_conversation_id: Option<ConversationId>,
    pub selected_messages: Vec<MessageRecord>,
    pub unread_marker: Option<MessageId>,
    pub conversation_bindings: Vec<(ConversationId, ProviderConversationRef)>,
    pub message_bindings: Vec<(MessageId, ProviderMessageRef)>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CrawlCheckpoint {
    pub conversation_id: ConversationId,
    pub next_cursor: Option<Vec<u8>>,
    pub completed: bool,
    pub pages_crawled: u64,
    pub messages_crawled: u64,
    pub updated_ms: i64,
}

#[derive(Clone, Debug, Serialize, serde::Deserialize)]
struct StoredCrawlCheckpoint {
    conversation_id: String,
    #[serde(default)]
    next_cursor: Option<Vec<u8>>,
    #[serde(default)]
    completed: bool,
    #[serde(default)]
    pages_crawled: u64,
    #[serde(default)]
    messages_crawled: u64,
    #[serde(default)]
    updated_ms: i64,
}

#[derive(Clone, Debug, Serialize, serde::Deserialize)]
struct StoredThreadEdgeParent {
    parent_id: String,
    #[serde(default)]
    updated_ms: i64,
}

impl From<&CrawlCheckpoint> for StoredCrawlCheckpoint {
    fn from(value: &CrawlCheckpoint) -> Self {
        Self {
            conversation_id: value.conversation_id.0.clone(),
            next_cursor: value.next_cursor.clone(),
            completed: value.completed,
            pages_crawled: value.pages_crawled,
            messages_crawled: value.messages_crawled,
            updated_ms: value.updated_ms,
        }
    }
}

impl StoredCrawlCheckpoint {
    fn into_domain(self) -> CrawlCheckpoint {
        CrawlCheckpoint {
            conversation_id: ConversationId::new(self.conversation_id),
            next_cursor: self.next_cursor,
            completed: self.completed,
            pages_crawled: self.pages_crawled,
            messages_crawled: self.messages_crawled,
            updated_ms: self.updated_ms,
        }
    }
}

pub struct LocalStore {
    db: DBWithThreadMode<MultiThreaded>,
}

impl LocalStore {
    pub fn open() -> io::Result<Self> {
        Self::open_at(paths::rocksdb_path())
    }

    pub fn open_at(path: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(path.parent().unwrap_or(Path::new(".")))?;

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        // Keep RocksDB fd usage bounded under heavy background activity.
        db_opts.set_max_open_files(256);
        db_opts.set_max_background_jobs(2);

        let mut cf_names = [
            CF_META,
            CF_CONVERSATIONS,
            CF_MESSAGES,
            CF_ACTIVITY,
            CF_BINDINGS,
            CF_USERS,
            CF_EMOJIS,
            CF_REACTIONS,
            CF_REACTION_OPS,
            CF_TEAMS,
            CF_OG,
        ]
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();

        // Existing local stores can carry forward column families from older or
        // newer builds. Open all existing families to avoid startup failures when
        // RocksDB reports "Column families not opened: <name>".
        if path.exists()
            && let Ok(existing_families) =
                DBWithThreadMode::<MultiThreaded>::list_cf(&db_opts, &path)
        {
            for name in existing_families {
                if !cf_names.iter().any(|known| known == &name) {
                    cf_names.push(name);
                }
            }
        }

        let open_with_names = |names: &[String]| {
            let descriptors = names
                .iter()
                .map(|name| ColumnFamilyDescriptor::new(name.clone(), Options::default()))
                .collect::<Vec<_>>();
            DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&db_opts, &path, descriptors)
        };

        let db = match open_with_names(&cf_names) {
            Ok(db) => db,
            Err(error) => {
                let mut retry_names = cf_names.clone();
                let mut added = false;
                for name in parse_missing_cf_names(error.as_ref()) {
                    if !retry_names.iter().any(|known| known == &name) {
                        retry_names.push(name);
                        added = true;
                    }
                }
                if !added {
                    return Err(io::Error::other(error));
                }
                open_with_names(&retry_names).map_err(io::Error::other)?
            }
        };
        Ok(Self { db })
    }

    pub fn load_bootstrap_seed(
        &self,
        conversation_limit: usize,
        selected_message_limit: usize,
    ) -> io::Result<Option<CachedBootstrapSeed>> {
        let meta_cf = self.cf(CF_META)?;
        let Some(meta_bytes) = self
            .db
            .get_cf(&meta_cf, META_KEY)
            .map_err(io::Error::other)?
        else {
            return Ok(None);
        };
        let meta: CachedMeta = decode(&meta_bytes)?;
        if meta.schema_version != SCHEMA_VERSION {
            self.clear_cache_for_schema_refresh()?;
            return Ok(None);
        }

        let ordered_ids = self.load_ordered_conversation_ids(conversation_limit)?;
        if ordered_ids.is_empty() {
            return Ok(None);
        }

        let mut channels = Vec::new();
        let mut direct_messages = Vec::new();
        let mut conversation_bindings = Vec::new();
        for conversation_id in &ordered_ids {
            if let Some(summary) = self.get_conversation(conversation_id)? {
                match summary.kind {
                    ConversationKind::Channel => channels.push(summary.clone()),
                    ConversationKind::DirectMessage | ConversationKind::GroupDirectMessage => {
                        direct_messages.push(summary.clone())
                    }
                }
                if let Some(binding) = self.get_conversation_binding(conversation_id)? {
                    conversation_bindings.push((conversation_id.clone(), binding));
                }
            }
        }

        let selected_conversation_id = meta
            .selected_conversation_id
            .as_ref()
            .map(|value| ConversationId::new(value.clone()))
            .or_else(|| ordered_ids.first().cloned());

        let selected_messages = if let Some(conversation_id) = selected_conversation_id.as_ref() {
            self.load_recent_messages(conversation_id, selected_message_limit)?
        } else {
            Vec::new()
        };

        let mut message_bindings = Vec::new();
        for message in &selected_messages {
            if let Some(binding) = self.get_message_binding(&message.id)? {
                message_bindings.push((message.id.clone(), binding));
            }
        }

        Ok(Some(CachedBootstrapSeed {
            workspace_name: meta.workspace_name,
            active_workspace_id: WorkspaceId::new(meta.active_workspace_id),
            account_display_name: meta.account_display_name,
            channels,
            direct_messages,
            selected_conversation_id,
            selected_messages,
            unread_marker: meta.unread_marker.map(MessageId::new),
            conversation_bindings,
            message_bindings,
        }))
    }

    pub fn persist_bootstrap_payload(&self, payload: &BootstrapPayload) -> io::Result<()> {
        let mut batch = WriteBatch::default();
        let conversations_cf = self.cf(CF_CONVERSATIONS)?;
        let activity_cf = self.cf(CF_ACTIVITY)?;
        let messages_cf = self.cf(CF_MESSAGES)?;
        let bindings_cf = self.cf(CF_BINDINGS)?;
        let meta_cf = self.cf(CF_META)?;

        // Replace conversation/activity snapshots atomically so stale channels from
        // earlier bootstraps do not linger in the sidebar cache.
        {
            let iter = self.db.iterator_cf(&conversations_cf, IteratorMode::Start);
            for item in iter {
                let (key, _) = item.map_err(io::Error::other)?;
                batch.delete_cf(&conversations_cf, key);
            }
        }
        {
            let iter = self.db.iterator_cf(&activity_cf, IteratorMode::Start);
            for item in iter {
                let (key, _) = item.map_err(io::Error::other)?;
                batch.delete_cf(&activity_cf, key);
            }
        }

        let mut ordered_conversations = Vec::new();
        ordered_conversations.extend(payload.channels.iter().cloned());
        ordered_conversations.extend(payload.direct_messages.iter().cloned());

        let now_ms = now_unix_ms();
        for (index, summary) in ordered_conversations.iter().enumerate() {
            let synthetic_activity = now_ms.saturating_sub(index as i64);
            let cached = CachedConversationSummary::from_domain(summary, synthetic_activity);
            batch.put_cf(
                &conversations_cf,
                conversation_key(&summary.id),
                encode(&cached)?,
            );
            batch.put_cf(
                &activity_cf,
                activity_key(&summary.id, synthetic_activity),
                Vec::<u8>::new(),
            );
        }

        for message in &payload.selected_messages {
            let cached = CachedMessageRecord::from_domain(message);
            batch.put_cf(
                &messages_cf,
                message_key(&message.conversation_id, &message.id),
                encode(&cached)?,
            );
            write_thread_index_entries(&mut batch, &messages_cf, message)?;
        }

        for binding in &payload.conversation_bindings {
            let cached = CachedConversationBinding::from_domain(binding);
            batch.put_cf(
                &bindings_cf,
                conversation_binding_key(&binding.conversation_id),
                encode(&cached)?,
            );
        }

        for binding in &payload.message_bindings {
            let cached = CachedMessageBinding::from_domain(binding);
            batch.put_cf(
                &bindings_cf,
                message_binding_key(&binding.message_id),
                encode(&cached)?,
            );
        }

        let meta = CachedMeta {
            workspace_name: payload.workspace_name.clone(),
            active_workspace_id: payload
                .active_workspace_id
                .clone()
                .unwrap_or_else(|| WorkspaceId::new("ws_primary"))
                .0,
            account_display_name: payload.account_display_name.clone(),
            selected_conversation_id: payload
                .selected_conversation_id
                .as_ref()
                .map(|value| value.0.clone()),
            unread_marker: payload.unread_marker.as_ref().map(|value| value.0.clone()),
            ..CachedMeta::default()
        };
        batch.put_cf(&meta_cf, META_KEY, encode(&meta)?);

        self.db.write(batch).map_err(io::Error::other)
    }

    pub fn clear_unread_marker(&self) -> io::Result<()> {
        let meta_cf = self.cf(CF_META)?;
        let Some(meta_bytes) = self
            .db
            .get_cf(&meta_cf, META_KEY)
            .map_err(io::Error::other)?
        else {
            return Ok(());
        };
        let mut meta: CachedMeta = decode(&meta_bytes)?;
        if meta.unread_marker.is_none() {
            return Ok(());
        }
        meta.unread_marker = None;
        self.db
            .put_cf(&meta_cf, META_KEY, encode(&meta)?)
            .map_err(io::Error::other)
    }

    pub fn persist_message(&self, message: &MessageRecord) -> io::Result<()> {
        let messages_cf = self.cf(CF_MESSAGES)?;
        let cached = CachedMessageRecord::from_domain(message);
        let existing = self.get_message(&message.conversation_id, &message.id)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(
            &messages_cf,
            message_key(&message.conversation_id, &message.id),
            encode(&cached)?,
        );
        if let Some(previous) = existing.as_ref() {
            if previous.thread_root_id != message.thread_root_id
                && let Some(previous_root) = previous.thread_root_id.as_ref()
            {
                batch.delete_cf(
                    &messages_cf,
                    thread_index_key(&message.conversation_id, previous_root, &message.id),
                );
            }
            if previous.reply_to != message.reply_to {
                if let Some(previous_parent) = previous.reply_to.as_ref() {
                    batch.delete_cf(
                        &messages_cf,
                        thread_edge_parent_key(
                            &message.conversation_id,
                            previous_parent,
                            &message.id,
                        ),
                    );
                }
                batch.delete_cf(
                    &messages_cf,
                    thread_edge_child_key(&message.conversation_id, &message.id),
                );
            }
        }
        write_thread_index_entries(&mut batch, &messages_cf, message)?;
        self.db.write(batch).map_err(io::Error::other)
    }

    pub fn persist_thread_index_entry(
        &self,
        conversation_id: &ConversationId,
        root_id: &MessageId,
        member_id: &MessageId,
    ) -> io::Result<()> {
        let messages_cf = self.cf(CF_MESSAGES)?;
        self.db
            .put_cf(
                &messages_cf,
                thread_index_key(conversation_id, root_id, member_id),
                Vec::<u8>::new(),
            )
            .map_err(io::Error::other)
    }

    pub fn delete_message(
        &self,
        conversation_id: &ConversationId,
        message_id: &MessageId,
    ) -> io::Result<()> {
        let messages_cf = self.cf(CF_MESSAGES)?;
        let existing = self.get_message(conversation_id, message_id)?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(&messages_cf, message_key(conversation_id, message_id));
        batch.delete_cf(
            &messages_cf,
            thread_edge_child_key(conversation_id, message_id),
        );
        if let Some(message) = existing {
            if let Some(root_id) = message.thread_root_id.as_ref() {
                batch.delete_cf(
                    &messages_cf,
                    thread_index_key(conversation_id, root_id, message_id),
                );
            }
            if let Some(parent_id) = message.reply_to.as_ref() {
                batch.delete_cf(
                    &messages_cf,
                    thread_edge_parent_key(conversation_id, parent_id, message_id),
                );
            }
        }
        self.db.write(batch).map_err(io::Error::other)
    }

    pub fn persist_conversation(
        &self,
        summary: &ConversationSummary,
        activity_time: i64,
    ) -> io::Result<()> {
        let conversations_cf = self.cf(CF_CONVERSATIONS)?;
        let activity_cf = self.cf(CF_ACTIVITY)?;
        let cached = CachedConversationSummary::from_domain(summary, activity_time);
        self.db
            .put_cf(
                &conversations_cf,
                conversation_key(&summary.id),
                encode(&cached)?,
            )
            .map_err(io::Error::other)?;
        self.db
            .put_cf(
                &activity_cf,
                activity_key(&summary.id, activity_time),
                Vec::<u8>::new(),
            )
            .map_err(io::Error::other)
    }

    pub fn persist_conversation_binding(
        &self,
        conversation_id: &ConversationId,
        provider_ref: &ProviderConversationRef,
    ) -> io::Result<()> {
        let bindings_cf = self.cf(CF_BINDINGS)?;
        let cached = CachedConversationBinding {
            conversation_id: conversation_id.0.clone(),
            provider_ref: provider_ref.0.clone(),
        };
        self.db
            .put_cf(
                &bindings_cf,
                conversation_binding_key(conversation_id),
                encode(&cached)?,
            )
            .map_err(io::Error::other)
    }

    pub fn get_message(
        &self,
        conversation_id: &ConversationId,
        message_id: &MessageId,
    ) -> io::Result<Option<MessageRecord>> {
        let messages_cf = self.cf(CF_MESSAGES)?;
        if let Some(bytes) = self
            .db
            .get_cf(&messages_cf, message_key(conversation_id, message_id))
            .map_err(io::Error::other)?
        {
            let cached: CachedMessageRecord = decode(&bytes)?;
            return Ok(Some(cached.to_domain()));
        }

        // Fallback for non-standard message keys.
        let prefix = message_prefix(conversation_id);
        let iter = self.db.iterator_cf(
            &messages_cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (key, value) = item.map_err(io::Error::other)?;
            let key_text = String::from_utf8_lossy(&key);
            if !key_text.starts_with(&prefix) {
                break;
            }
            let cached: CachedMessageRecord = decode(&value)?;
            if cached.id == message_id.0 {
                return Ok(Some(cached.to_domain()));
            }
        }
        Ok(None)
    }

    pub fn get_conversation(
        &self,
        conversation_id: &ConversationId,
    ) -> io::Result<Option<ConversationSummary>> {
        let conversations_cf = self.cf(CF_CONVERSATIONS)?;
        let Some(bytes) = self
            .db
            .get_cf(&conversations_cf, conversation_key(conversation_id))
            .map_err(io::Error::other)?
        else {
            return Ok(None);
        };
        let cached: CachedConversationSummary = decode(&bytes)?;
        Ok(Some(cached.to_domain()))
    }

    pub fn load_recent_messages_for_conversation(
        &self,
        conversation_id: &ConversationId,
        limit: usize,
    ) -> io::Result<Vec<MessageRecord>> {
        self.load_recent_messages(conversation_id, limit)
    }

    pub fn load_messages_before(
        &self,
        conversation_id: &ConversationId,
        before_message_id: Option<&MessageId>,
        limit: usize,
    ) -> io::Result<Vec<MessageRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let messages_cf = self.cf(CF_MESSAGES)?;
        let prefix = message_prefix(conversation_id);
        let from = before_message_id
            .map(|message_id| message_key(conversation_id, message_id))
            .unwrap_or_else(|| format!("{prefix}~"));

        let mut messages = Vec::new();
        let iter = self.db.iterator_cf(
            &messages_cf,
            IteratorMode::From(from.as_bytes(), Direction::Reverse),
        );
        for item in iter {
            let (key, value) = item.map_err(io::Error::other)?;
            let key_text = String::from_utf8_lossy(&key);
            if !key_text.starts_with(&prefix) {
                break;
            }
            let cached: CachedMessageRecord = decode(&value)?;
            if before_message_id
                .as_ref()
                .is_some_and(|anchor| cached.id == anchor.0)
            {
                continue;
            }
            messages.push(cached.to_domain());
            if messages.len() >= limit {
                break;
            }
        }
        messages.reverse();
        self.hydrate_message_reactions(conversation_id, &mut messages)?;
        Ok(messages)
    }

    pub fn load_messages_after(
        &self,
        conversation_id: &ConversationId,
        after_message_id: &MessageId,
        limit: usize,
    ) -> io::Result<Vec<MessageRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let messages_cf = self.cf(CF_MESSAGES)?;
        let prefix = message_prefix(conversation_id);
        let from = message_key(conversation_id, after_message_id);

        let mut messages = Vec::new();
        let iter = self.db.iterator_cf(
            &messages_cf,
            IteratorMode::From(from.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (key, value) = item.map_err(io::Error::other)?;
            let key_text = String::from_utf8_lossy(&key);
            if !key_text.starts_with(&prefix) {
                break;
            }
            let cached: CachedMessageRecord = decode(&value)?;
            if cached.id == after_message_id.0 {
                continue;
            }
            messages.push(cached.to_domain());
            if messages.len() >= limit {
                break;
            }
        }
        self.hydrate_message_reactions(conversation_id, &mut messages)?;
        Ok(messages)
    }

    pub fn load_thread_messages(
        &self,
        conversation_id: &ConversationId,
        root_id: &MessageId,
    ) -> io::Result<Vec<MessageRecord>> {
        let mut message_ids = Vec::new();
        let mut seen = HashSet::new();
        if seen.insert(root_id.clone()) {
            message_ids.push(root_id.clone());
        }

        // Primary path: traverse explicit reply graph so nested descendants are
        // loaded even if legacy root materialization was incomplete.
        for member_id in self.load_thread_descendants_bfs(conversation_id, root_id)? {
            if seen.insert(member_id.clone()) {
                message_ids.push(member_id);
            }
        }

        // Compatibility fallback while edge migration is still in progress.
        if message_ids.len() <= 1 {
            for member_id in
                self.load_thread_member_ids_from_legacy_index(conversation_id, root_id)?
            {
                if seen.insert(member_id.clone()) {
                    message_ids.push(member_id);
                }
            }
        }

        let mut messages = Vec::new();
        for message_id in message_ids {
            if let Some(message) = self.get_message(conversation_id, &message_id)? {
                messages.push(message);
            }
        }
        messages.sort_by(|left, right| compare_message_ids(&left.id, &right.id));
        self.hydrate_message_reactions(conversation_id, &mut messages)?;
        Ok(messages)
    }

    fn load_thread_member_ids_from_legacy_index(
        &self,
        conversation_id: &ConversationId,
        root_id: &MessageId,
    ) -> io::Result<Vec<MessageId>> {
        let messages_cf = self.cf(CF_MESSAGES)?;
        let prefix = thread_index_prefix(conversation_id, root_id);
        let mut seen = HashSet::new();
        let mut message_ids = Vec::new();
        let iter = self.db.iterator_cf(
            &messages_cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (key, _) = item.map_err(io::Error::other)?;
            let key_text = String::from_utf8_lossy(&key);
            if !key_text.starts_with(&prefix) {
                break;
            }
            if let Some(member_id) = parse_thread_index_member_id(&key_text)
                && seen.insert(member_id.clone())
            {
                message_ids.push(member_id);
            }
        }
        message_ids.sort_by(compare_message_ids);
        Ok(message_ids)
    }

    pub fn load_direct_reply_ids(
        &self,
        conversation_id: &ConversationId,
        parent_id: &MessageId,
    ) -> io::Result<Vec<MessageId>> {
        let messages_cf = self.cf(CF_MESSAGES)?;
        let prefix = thread_edge_parent_prefix(conversation_id, parent_id);
        let mut seen = HashSet::new();
        let mut direct_replies = Vec::new();
        let iter = self.db.iterator_cf(
            &messages_cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (key, _) = item.map_err(io::Error::other)?;
            let key_text = String::from_utf8_lossy(&key);
            if !key_text.starts_with(&prefix) {
                break;
            }
            if let Some(child_id) = parse_thread_edge_child_id(&key_text)
                && seen.insert(child_id.clone())
            {
                direct_replies.push(child_id);
            }
        }
        direct_replies.sort_by(compare_message_ids);
        Ok(direct_replies)
    }

    pub fn load_direct_replies(
        &self,
        conversation_id: &ConversationId,
        parent_id: &MessageId,
    ) -> io::Result<Vec<MessageRecord>> {
        let mut replies = Vec::new();
        for child_id in self.load_direct_reply_ids(conversation_id, parent_id)? {
            if let Some(message) = self.get_message(conversation_id, &child_id)? {
                replies.push(message);
            }
        }
        replies.sort_by(|left, right| compare_message_ids(&left.id, &right.id));
        self.hydrate_message_reactions(conversation_id, &mut replies)?;
        Ok(replies)
    }

    pub fn load_ancestor_chain(
        &self,
        conversation_id: &ConversationId,
        message_id: &MessageId,
    ) -> io::Result<Vec<MessageId>> {
        let messages_cf = self.cf(CF_MESSAGES)?;
        let mut chain = Vec::new();
        let mut seen = HashSet::new();
        let mut current = message_id.clone();
        while seen.insert(current.clone()) {
            let key = thread_edge_child_key(conversation_id, &current);
            let parent_id = if let Some(bytes) = self
                .db
                .get_cf(&messages_cf, key)
                .map_err(io::Error::other)?
            {
                let stored: StoredThreadEdgeParent = decode(&bytes)?;
                Some(MessageId::new(stored.parent_id))
            } else {
                self.get_message(conversation_id, &current)?
                    .and_then(|message| message.reply_to)
            };
            let Some(parent_id) = parent_id else {
                break;
            };
            chain.push(parent_id.clone());
            current = parent_id;
        }
        Ok(chain)
    }

    pub fn load_thread_descendants_bfs(
        &self,
        conversation_id: &ConversationId,
        root_id: &MessageId,
    ) -> io::Result<Vec<MessageId>> {
        let mut descendants = Vec::new();
        let mut queue = VecDeque::new();
        let mut seen = HashSet::new();
        seen.insert(root_id.clone());
        queue.push_back(root_id.clone());

        while let Some(parent_id) = queue.pop_front() {
            for child_id in self.load_direct_reply_ids(conversation_id, &parent_id)? {
                if seen.insert(child_id.clone()) {
                    descendants.push(child_id.clone());
                    queue.push_back(child_id);
                }
            }
        }
        descendants.sort_by(compare_message_ids);
        Ok(descendants)
    }

    pub fn repair_thread_edges_for_conversation(
        &self,
        conversation_id: &ConversationId,
        scan_limit: usize,
    ) -> io::Result<usize> {
        let messages_cf = self.cf(CF_MESSAGES)?;
        let mut batch = WriteBatch::default();
        let mut rewritten_edges = 0usize;
        let messages = self.load_messages_before(conversation_id, None, scan_limit)?;
        for message in messages {
            write_thread_index_entries(&mut batch, &messages_cf, &message)?;
            if message.reply_to.is_some() {
                rewritten_edges = rewritten_edges.saturating_add(1);
            }
        }
        self.db.write(batch).map_err(io::Error::other)?;
        Ok(rewritten_edges)
    }

    pub fn load_cached_conversation_ids(&self, limit: usize) -> io::Result<Vec<ConversationId>> {
        self.load_ordered_conversation_ids(limit)
    }

    pub fn is_thread_edge_conversation_migrated(
        &self,
        conversation_id: &ConversationId,
    ) -> io::Result<bool> {
        let meta_cf = self.cf(CF_META)?;
        self.db
            .get_cf(
                &meta_cf,
                thread_edge_migration_conversation_key(conversation_id),
            )
            .map(|value| value.is_some())
            .map_err(io::Error::other)
    }

    pub fn mark_thread_edge_conversation_migrated(
        &self,
        conversation_id: &ConversationId,
    ) -> io::Result<()> {
        let meta_cf = self.cf(CF_META)?;
        self.db
            .put_cf(
                &meta_cf,
                thread_edge_migration_conversation_key(conversation_id),
                [1u8],
            )
            .map_err(io::Error::other)
    }

    pub fn is_thread_edge_migration_complete(&self) -> io::Result<bool> {
        let meta_cf = self.cf(CF_META)?;
        self.db
            .get_cf(&meta_cf, THREAD_EDGE_MIGRATION_COMPLETE_KEY)
            .map(|value| value.is_some())
            .map_err(io::Error::other)
    }

    pub fn mark_thread_edge_migration_complete(&self) -> io::Result<()> {
        let meta_cf = self.cf(CF_META)?;
        self.db
            .put_cf(&meta_cf, THREAD_EDGE_MIGRATION_COMPLETE_KEY, [1u8])
            .map_err(io::Error::other)
    }

    fn hydrate_message_reactions(
        &self,
        conversation_id: &ConversationId,
        messages: &mut [MessageRecord],
    ) -> io::Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        let message_ids = messages
            .iter()
            .map(|message| message.id.clone())
            .collect::<Vec<_>>();
        let loaded = self.load_message_reactions_for_messages(conversation_id, &message_ids)?;
        for message in messages {
            message.reactions = aggregate_cached_message_reactions(loaded.get(&message.id));
        }
        Ok(())
    }

    pub fn has_messages_before(
        &self,
        conversation_id: &ConversationId,
        before_message_id: &MessageId,
    ) -> io::Result<bool> {
        Ok(!self
            .load_messages_before(conversation_id, Some(before_message_id), 1)?
            .is_empty())
    }

    pub fn get_user_profile(&self, user_id: &UserId) -> io::Result<Option<CachedUserProfile>> {
        let users_cf = self.cf(CF_USERS)?;
        let key = user_profile_key(user_id);
        let Some(bytes) = self.db.get_cf(&users_cf, key).map_err(io::Error::other)? else {
            return Ok(None);
        };
        decode(&bytes).map(Some)
    }

    pub fn upsert_user_profile(
        &self,
        user_id: &UserId,
        display_name: String,
        avatar_url: Option<String>,
        avatar_path: Option<String>,
        updated_ms: i64,
    ) -> io::Result<()> {
        let users_cf = self.cf(CF_USERS)?;
        let profile = CachedUserProfile {
            username: user_id.0.clone(),
            display_name,
            avatar_url,
            avatar_path,
            updated_ms,
        };
        self.db
            .put_cf(&users_cf, user_profile_key(user_id), encode(&profile)?)
            .map_err(io::Error::other)
    }

    pub fn get_og_preview(&self, url: &str) -> io::Result<Option<LinkPreview>> {
        let og_cf = self.cf(CF_OG)?;
        let Some(bytes) = self
            .db
            .get_cf(&og_cf, url.as_bytes())
            .map_err(io::Error::other)?
        else {
            return Ok(None);
        };
        let cached: CachedLinkPreview = decode(&bytes)?;
        Ok(Some(cached.to_domain()))
    }

    pub fn upsert_og_preview(&self, url: &str, preview: &LinkPreview) -> io::Result<()> {
        let og_cf = self.cf(CF_OG)?;
        let cached = CachedLinkPreview::from_domain(preview);
        self.db
            .put_cf(&og_cf, url.as_bytes(), encode(&cached)?)
            .map_err(io::Error::other)
    }

    pub fn load_conversation_emojis(
        &self,
        conversation_id: &ConversationId,
    ) -> io::Result<Vec<CachedConversationEmoji>> {
        self.load_emojis_with_prefix(&conversation_emoji_prefix(conversation_id))
    }

    pub fn load_team_emojis(&self, team_id: &str) -> io::Result<Vec<CachedConversationEmoji>> {
        self.load_emojis_with_prefix(&team_emoji_prefix(team_id))
    }

    fn load_emojis_with_prefix(&self, prefix: &str) -> io::Result<Vec<CachedConversationEmoji>> {
        let emojis_cf = self.cf(CF_EMOJIS)?;
        let mut emojis = Vec::new();
        let iter = self.db.iterator_cf(
            &emojis_cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (key, value) = item.map_err(io::Error::other)?;
            let key_text = String::from_utf8_lossy(&key);
            if !key_text.starts_with(prefix) {
                break;
            }
            let emoji: CachedConversationEmoji = decode(&value)?;
            emojis.push(emoji);
        }
        Ok(emojis)
    }

    pub fn replace_conversation_emojis(
        &self,
        conversation_id: &ConversationId,
        emojis: &[CachedConversationEmoji],
    ) -> io::Result<()> {
        self.replace_emojis_with_prefix(
            &conversation_emoji_prefix(conversation_id),
            emojis,
            |emoji| conversation_emoji_key(conversation_id, &emoji.alias),
        )
    }

    pub fn replace_team_emojis(
        &self,
        team_id: &str,
        emojis: &[CachedConversationEmoji],
    ) -> io::Result<()> {
        self.replace_emojis_with_prefix(&team_emoji_prefix(team_id), emojis, |emoji| {
            team_emoji_key(team_id, &emoji.alias)
        })
    }

    pub fn clear_conversation_emojis(&self, conversation_id: &ConversationId) -> io::Result<()> {
        self.clear_emojis_with_prefix(&conversation_emoji_prefix(conversation_id))
    }

    fn replace_emojis_with_prefix<F>(
        &self,
        prefix: &str,
        emojis: &[CachedConversationEmoji],
        key_for_emoji: F,
    ) -> io::Result<()>
    where
        F: Fn(&CachedConversationEmoji) -> String,
    {
        let mut batch = WriteBatch::default();
        let emojis_cf = self.cf(CF_EMOJIS)?;
        self.delete_emoji_keys_with_prefix(prefix, &emojis_cf, &mut batch)?;

        for emoji in emojis {
            batch.put_cf(&emojis_cf, key_for_emoji(emoji), encode(emoji)?);
        }

        self.db.write(batch).map_err(io::Error::other)
    }

    fn clear_emojis_with_prefix(&self, prefix: &str) -> io::Result<()> {
        let mut batch = WriteBatch::default();
        let emojis_cf = self.cf(CF_EMOJIS)?;
        self.delete_emoji_keys_with_prefix(prefix, &emojis_cf, &mut batch)?;
        self.db.write(batch).map_err(io::Error::other)
    }

    fn delete_emoji_keys_with_prefix(
        &self,
        prefix: &str,
        emojis_cf: &Arc<BoundColumnFamily<'_>>,
        batch: &mut WriteBatch,
    ) -> io::Result<()> {
        let existing_iter = self.db.iterator_cf(
            emojis_cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in existing_iter {
            let (key, _) = item.map_err(io::Error::other)?;
            let key_text = String::from_utf8_lossy(&key);
            if !key_text.starts_with(prefix) {
                break;
            }
            batch.delete_cf(emojis_cf, key);
        }
        Ok(())
    }

    pub fn upsert_message_reaction(
        &self,
        conversation_id: &ConversationId,
        message_id: &MessageId,
        emoji: &str,
        source_ref: Option<&EmojiSourceRef>,
        actor_id: &UserId,
        updated_ms: i64,
    ) -> io::Result<()> {
        let reactions_cf = self.cf(CF_REACTIONS)?;
        let key = message_reaction_key(conversation_id, message_id, emoji, actor_id);
        let mut cached_source_ref = source_ref.map(|value| CachedEmojiSourceRef {
            backend_id: value.backend_id.0.clone(),
            ref_key: value.ref_key.clone(),
        });
        if cached_source_ref.is_none()
            && let Some(existing_bytes) = self
                .db
                .get_cf(&reactions_cf, &key)
                .map_err(io::Error::other)?
            && let Ok(existing) = decode::<CachedMessageReaction>(&existing_bytes)
        {
            cached_source_ref = existing.source_ref;
        }
        let record = CachedMessageReaction {
            message_id: message_id.0.clone(),
            emoji: emoji.to_string(),
            source_ref: cached_source_ref,
            actor_id: actor_id.0.clone(),
            updated_ms,
        };
        self.db
            .put_cf(&reactions_cf, key, encode(&record)?)
            .map_err(io::Error::other)
    }

    pub fn delete_message_reaction(
        &self,
        conversation_id: &ConversationId,
        message_id: &MessageId,
        emoji: &str,
        actor_id: &UserId,
    ) -> io::Result<()> {
        let reactions_cf = self.cf(CF_REACTIONS)?;
        self.db
            .delete_cf(
                &reactions_cf,
                message_reaction_key(conversation_id, message_id, emoji, actor_id),
            )
            .map_err(io::Error::other)
    }

    pub fn upsert_message_reaction_op(
        &self,
        conversation_id: &ConversationId,
        op_message_id: &MessageId,
        target_message_id: &MessageId,
        emoji: &str,
        actor_id: &UserId,
        updated_ms: i64,
    ) -> io::Result<()> {
        let reaction_ops_cf = self.cf(CF_REACTION_OPS)?;
        let record = CachedReactionOp {
            op_message_id: op_message_id.0.clone(),
            target_message_id: target_message_id.0.clone(),
            emoji: emoji.to_string(),
            actor_id: actor_id.0.clone(),
            updated_ms,
        };
        self.db
            .put_cf(
                &reaction_ops_cf,
                message_reaction_op_key(conversation_id, op_message_id),
                encode(&record)?,
            )
            .map_err(io::Error::other)
    }

    pub fn delete_message_reaction_op(
        &self,
        conversation_id: &ConversationId,
        op_message_id: &MessageId,
    ) -> io::Result<()> {
        let reaction_ops_cf = self.cf(CF_REACTION_OPS)?;
        self.db
            .delete_cf(
                &reaction_ops_cf,
                message_reaction_op_key(conversation_id, op_message_id),
            )
            .map_err(io::Error::other)
    }

    pub fn take_message_reaction_op(
        &self,
        conversation_id: &ConversationId,
        op_message_id: &MessageId,
    ) -> io::Result<Option<(MessageId, String, UserId)>> {
        let reaction_ops_cf = self.cf(CF_REACTION_OPS)?;
        let key = message_reaction_op_key(conversation_id, op_message_id);
        let Some(bytes) = self
            .db
            .get_cf(&reaction_ops_cf, key.as_bytes())
            .map_err(io::Error::other)?
        else {
            return Ok(None);
        };
        self.db
            .delete_cf(&reaction_ops_cf, key.as_bytes())
            .map_err(io::Error::other)?;
        let stored: CachedReactionOp = decode(&bytes)?;
        Ok(Some((
            MessageId::new(stored.target_message_id),
            stored.emoji,
            UserId::new(stored.actor_id),
        )))
    }

    pub fn get_message_reaction_op(
        &self,
        conversation_id: &ConversationId,
        op_message_id: &MessageId,
    ) -> io::Result<Option<(MessageId, String, UserId)>> {
        let reaction_ops_cf = self.cf(CF_REACTION_OPS)?;
        let key = message_reaction_op_key(conversation_id, op_message_id);
        let Some(bytes) = self
            .db
            .get_cf(&reaction_ops_cf, key.as_bytes())
            .map_err(io::Error::other)?
        else {
            return Ok(None);
        };
        let stored: CachedReactionOp = decode(&bytes)?;
        Ok(Some((
            MessageId::new(stored.target_message_id),
            stored.emoji,
            UserId::new(stored.actor_id),
        )))
    }

    pub fn load_message_reactions_for_messages(
        &self,
        conversation_id: &ConversationId,
        message_ids: &[MessageId],
    ) -> io::Result<HashMap<MessageId, Vec<CachedMessageReaction>>> {
        let reactions_cf = self.cf(CF_REACTIONS)?;
        let mut by_message = HashMap::new();

        for message_id in message_ids {
            let prefix = message_reaction_prefix(conversation_id, message_id);
            let mut reactions = Vec::new();
            let iter = self.db.iterator_cf(
                &reactions_cf,
                IteratorMode::From(prefix.as_bytes(), Direction::Forward),
            );
            for item in iter {
                let (key, value) = item.map_err(io::Error::other)?;
                let key_text = String::from_utf8_lossy(&key);
                if !key_text.starts_with(&prefix) {
                    break;
                }
                let reaction: CachedMessageReaction = decode(&value)?;
                reactions.push(reaction);
            }
            if !reactions.is_empty() {
                by_message.insert(message_id.clone(), reactions);
            }
        }

        Ok(by_message)
    }

    pub fn get_conversation_team_binding(
        &self,
        conversation_id: &ConversationId,
    ) -> io::Result<Option<CachedConversationTeamBinding>> {
        let teams_cf = self.cf(CF_TEAMS)?;
        let key = conversation_team_key(conversation_id);
        let Some(bytes) = self.db.get_cf(&teams_cf, key).map_err(io::Error::other)? else {
            return Ok(None);
        };
        decode(&bytes).map(Some)
    }

    pub fn upsert_conversation_team_binding(
        &self,
        conversation_id: &ConversationId,
        team_id: &str,
        updated_ms: i64,
    ) -> io::Result<()> {
        let teams_cf = self.cf(CF_TEAMS)?;
        let binding = CachedConversationTeamBinding {
            conversation_id: conversation_id.0.clone(),
            team_id: normalize_team_id_for_key(team_id),
            updated_ms,
        };
        self.db
            .put_cf(
                &teams_cf,
                conversation_team_key(conversation_id),
                encode(&binding)?,
            )
            .map_err(io::Error::other)
    }

    pub fn load_conversation_ids_for_team(&self, team_id: &str) -> io::Result<Vec<ConversationId>> {
        let teams_cf = self.cf(CF_TEAMS)?;
        let prefix = CONVERSATION_TEAM_PREFIX;
        let mut conversation_ids = Vec::new();
        let normalized_team_id = normalize_team_id_for_key(team_id);
        let iter = self.db.iterator_cf(
            &teams_cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (key, value) = item.map_err(io::Error::other)?;
            let key_text = String::from_utf8_lossy(&key);
            if !key_text.starts_with(prefix) {
                break;
            }
            let binding: CachedConversationTeamBinding = decode(&value)?;
            if binding.team_id == normalized_team_id {
                conversation_ids.push(ConversationId::new(binding.conversation_id));
            }
        }
        conversation_ids.sort_by(|left, right| left.0.cmp(&right.0));
        conversation_ids.dedup_by(|left, right| left.0 == right.0);
        Ok(conversation_ids)
    }

    pub fn get_team_role_map(&self, team_id: &str) -> io::Result<Option<CachedTeamRoleMap>> {
        let teams_cf = self.cf(CF_TEAMS)?;
        let key = team_role_key(team_id);
        let Some(bytes) = self.db.get_cf(&teams_cf, key).map_err(io::Error::other)? else {
            return Ok(None);
        };
        decode(&bytes).map(Some)
    }

    pub fn upsert_team_role_map(
        &self,
        team_id: &str,
        roles: &HashMap<UserId, i64>,
        updated_ms: i64,
    ) -> io::Result<()> {
        let teams_cf = self.cf(CF_TEAMS)?;
        let mut ordered_roles = roles
            .iter()
            .map(|(user_id, role)| CachedTeamRoleEntry {
                user_id: user_id.0.clone(),
                role: *role,
            })
            .collect::<Vec<_>>();
        ordered_roles.sort_by(|left, right| left.user_id.cmp(&right.user_id));
        let team_role_map = CachedTeamRoleMap {
            team_id: normalize_team_id_for_key(team_id),
            updated_ms,
            roles: ordered_roles,
        };
        self.db
            .put_cf(&teams_cf, team_role_key(team_id), encode(&team_role_map)?)
            .map_err(io::Error::other)
    }

    pub fn load_crawl_checkpoint(
        &self,
        conversation_id: &ConversationId,
    ) -> io::Result<Option<CrawlCheckpoint>> {
        let meta_cf = self.cf(CF_META)?;
        let key = crawl_checkpoint_key(conversation_id);
        let Some(bytes) = self.db.get_cf(&meta_cf, key).map_err(io::Error::other)? else {
            return Ok(None);
        };
        let stored: StoredCrawlCheckpoint = decode(&bytes)?;
        Ok(Some(stored.into_domain()))
    }

    pub fn upsert_crawl_checkpoint(&self, checkpoint: &CrawlCheckpoint) -> io::Result<()> {
        let meta_cf = self.cf(CF_META)?;
        self.db
            .put_cf(
                &meta_cf,
                crawl_checkpoint_key(&checkpoint.conversation_id),
                encode(&StoredCrawlCheckpoint::from(checkpoint))?,
            )
            .map_err(io::Error::other)
    }

    pub fn clear_crawl_checkpoint(&self, conversation_id: &ConversationId) -> io::Result<()> {
        let meta_cf = self.cf(CF_META)?;
        self.db
            .delete_cf(&meta_cf, crawl_checkpoint_key(conversation_id))
            .map_err(io::Error::other)
    }

    fn get_conversation_binding(
        &self,
        conversation_id: &ConversationId,
    ) -> io::Result<Option<ProviderConversationRef>> {
        let bindings_cf = self.cf(CF_BINDINGS)?;
        let Some(bytes) = self
            .db
            .get_cf(&bindings_cf, conversation_binding_key(conversation_id))
            .map_err(io::Error::other)?
        else {
            return Ok(None);
        };
        let cached: CachedConversationBinding = decode(&bytes)?;
        Ok(Some(ProviderConversationRef::new(cached.provider_ref)))
    }

    fn get_message_binding(
        &self,
        message_id: &MessageId,
    ) -> io::Result<Option<ProviderMessageRef>> {
        let bindings_cf = self.cf(CF_BINDINGS)?;
        let Some(bytes) = self
            .db
            .get_cf(&bindings_cf, message_binding_key(message_id))
            .map_err(io::Error::other)?
        else {
            return Ok(None);
        };
        let cached: CachedMessageBinding = decode(&bytes)?;
        Ok(Some(ProviderMessageRef::new(cached.provider_ref)))
    }

    fn load_ordered_conversation_ids(&self, limit: usize) -> io::Result<Vec<ConversationId>> {
        let activity_cf = self.cf(CF_ACTIVITY)?;
        let mut ordered = Vec::new();
        let mut seen = HashSet::new();

        let iter = self.db.iterator_cf(&activity_cf, IteratorMode::Start);
        for item in iter {
            let (key, _) = item.map_err(io::Error::other)?;
            let key_text = String::from_utf8_lossy(&key);
            let Some(conversation_id) = parse_activity_conversation_id(&key_text) else {
                continue;
            };
            if seen.insert(conversation_id.clone()) {
                ordered.push(ConversationId::new(conversation_id));
                if ordered.len() >= limit {
                    break;
                }
            }
        }

        if !ordered.is_empty() {
            return Ok(ordered);
        }

        let conversations_cf = self.cf(CF_CONVERSATIONS)?;
        let iter = self.db.iterator_cf(&conversations_cf, IteratorMode::Start);
        for item in iter {
            let (key, _) = item.map_err(io::Error::other)?;
            let key_text = String::from_utf8_lossy(&key);
            if let Some(conversation_id) = key_text.strip_prefix("conv:") {
                ordered.push(ConversationId::new(conversation_id.to_string()));
                if ordered.len() >= limit {
                    break;
                }
            }
        }

        Ok(ordered)
    }

    fn load_recent_messages(
        &self,
        conversation_id: &ConversationId,
        limit: usize,
    ) -> io::Result<Vec<MessageRecord>> {
        self.load_messages_before(conversation_id, None, limit)
    }

    fn clear_cache_for_schema_refresh(&self) -> io::Result<()> {
        let mut batch = WriteBatch::default();
        for cf_name in [
            CF_META,
            CF_CONVERSATIONS,
            CF_MESSAGES,
            CF_ACTIVITY,
            CF_BINDINGS,
            CF_USERS,
            CF_EMOJIS,
            CF_REACTIONS,
            CF_REACTION_OPS,
            CF_TEAMS,
            CF_OG,
        ] {
            let cf = self.cf(cf_name)?;
            let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
            for item in iter {
                let (key, _) = item.map_err(io::Error::other)?;
                batch.delete_cf(&cf, key);
            }
        }
        self.db.write(batch).map_err(io::Error::other)
    }

    fn cf(&self, name: &str) -> io::Result<Arc<BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| io::Error::other(format!("missing column family: {name}")))
    }
}

fn parse_missing_cf_names(error: &str) -> Vec<String> {
    let marker = "Column families not opened:";
    let Some((_, tail)) = error.split_once(marker) else {
        return Vec::new();
    };
    tail.split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn conversation_key(conversation_id: &ConversationId) -> String {
    format!("conv:{}", conversation_id.0)
}

fn activity_key(conversation_id: &ConversationId, activity_time: i64) -> String {
    let normalized = activity_time.max(0) as u64;
    let reverse = u64::MAX - normalized;
    format!("act:{reverse:020}:{}", conversation_id.0)
}

fn parse_activity_conversation_id(key: &str) -> Option<String> {
    let mut parts = key.splitn(3, ':');
    if parts.next()? != "act" {
        return None;
    }
    let _ = parts.next()?;
    Some(parts.next()?.to_string())
}

fn message_prefix(conversation_id: &ConversationId) -> String {
    format!("msg:{}:", conversation_id.0)
}

fn message_key(conversation_id: &ConversationId, message_id: &MessageId) -> String {
    let sort_component = message_sort_component(message_id);
    format!(
        "{}{}:{}",
        message_prefix(conversation_id),
        sort_component,
        message_id.0
    )
}

fn thread_index_prefix(conversation_id: &ConversationId, root_id: &MessageId) -> String {
    format!("{THREAD_INDEX_PREFIX}{}:{}:", conversation_id.0, root_id.0)
}

fn thread_index_key(
    conversation_id: &ConversationId,
    root_id: &MessageId,
    member_id: &MessageId,
) -> String {
    let sort_component = message_sort_component(member_id);
    format!(
        "{}{}:{}",
        thread_index_prefix(conversation_id, root_id),
        sort_component,
        member_id.0
    )
}

fn parse_thread_index_member_id(key: &str) -> Option<MessageId> {
    let member = key.rsplit(':').next()?;
    if member.is_empty() {
        return None;
    }
    Some(MessageId::new(member.to_string()))
}

fn thread_edge_parent_prefix(conversation_id: &ConversationId, parent_id: &MessageId) -> String {
    format!(
        "{THREAD_EDGE_PARENT_PREFIX}{}:{}:",
        conversation_id.0, parent_id.0
    )
}

fn thread_edge_parent_key(
    conversation_id: &ConversationId,
    parent_id: &MessageId,
    child_id: &MessageId,
) -> String {
    format!(
        "{}{}:{}",
        thread_edge_parent_prefix(conversation_id, parent_id),
        message_sort_component(child_id),
        child_id.0
    )
}

fn thread_edge_child_key(conversation_id: &ConversationId, child_id: &MessageId) -> String {
    format!(
        "{THREAD_EDGE_CHILD_PREFIX}{}:{}",
        conversation_id.0, child_id.0
    )
}

fn parse_thread_edge_child_id(key: &str) -> Option<MessageId> {
    let child = key.rsplit(':').next()?;
    if child.is_empty() {
        return None;
    }
    Some(MessageId::new(child.to_string()))
}

fn message_sort_component(message_id: &MessageId) -> String {
    message_id
        .0
        .parse::<u64>()
        .map(|value| format!("{value:020}"))
        .unwrap_or_else(|_| format!("z{}", message_id.0))
}

fn compare_message_ids(left: &MessageId, right: &MessageId) -> std::cmp::Ordering {
    let left_num = left.0.parse::<u64>().ok();
    let right_num = right.0.parse::<u64>().ok();

    match (left_num, right_num) {
        (Some(left_num), Some(right_num)) => {
            left_num.cmp(&right_num).then_with(|| left.0.cmp(&right.0))
        }
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.0.cmp(&right.0),
    }
}

fn write_thread_index_entries(
    batch: &mut WriteBatch,
    messages_cf: &Arc<BoundColumnFamily<'_>>,
    message: &MessageRecord,
) -> io::Result<()> {
    if let Some(root_id) = message.thread_root_id.as_ref() {
        batch.put_cf(
            messages_cf,
            thread_index_key(&message.conversation_id, root_id, &message.id),
            Vec::<u8>::new(),
        );
    }
    if let Some(parent_id) = message.reply_to.as_ref() {
        batch.put_cf(
            messages_cf,
            thread_edge_parent_key(&message.conversation_id, parent_id, &message.id),
            Vec::<u8>::new(),
        );
        let stored = StoredThreadEdgeParent {
            parent_id: parent_id.0.clone(),
            updated_ms: now_unix_ms(),
        };
        batch.put_cf(
            messages_cf,
            thread_edge_child_key(&message.conversation_id, &message.id),
            encode(&stored)?,
        );
    } else {
        batch.delete_cf(
            messages_cf,
            thread_edge_child_key(&message.conversation_id, &message.id),
        );
    }
    Ok(())
}

fn thread_edge_migration_conversation_key(conversation_id: &ConversationId) -> String {
    format!(
        "{THREAD_EDGE_MIGRATION_CONVERSATION_PREFIX}{}",
        conversation_id.0
    )
}

fn conversation_binding_key(conversation_id: &ConversationId) -> String {
    format!("bind:conv:{}", conversation_id.0)
}

fn message_binding_key(message_id: &MessageId) -> String {
    format!("bind:msg:{}", message_id.0)
}

fn conversation_emoji_prefix(conversation_id: &ConversationId) -> String {
    format!("{EMOJI_PREFIX}{}:", conversation_id.0)
}

fn conversation_emoji_key(conversation_id: &ConversationId, alias: &str) -> String {
    format!(
        "{}{}",
        conversation_emoji_prefix(conversation_id),
        alias.to_ascii_lowercase()
    )
}

fn team_emoji_prefix(team_id: &str) -> String {
    format!("{EMOJI_PREFIX}team:{}:", normalize_team_id_for_key(team_id))
}

fn team_emoji_key(team_id: &str, alias: &str) -> String {
    format!(
        "{}{}",
        team_emoji_prefix(team_id),
        alias.to_ascii_lowercase()
    )
}

fn message_reaction_prefix(conversation_id: &ConversationId, message_id: &MessageId) -> String {
    format!("{REACTION_PREFIX}{}:{}:", conversation_id.0, message_id.0)
}

fn message_reaction_key(
    conversation_id: &ConversationId,
    message_id: &MessageId,
    emoji: &str,
    actor_id: &UserId,
) -> String {
    format!(
        "{}{}:{}",
        message_reaction_prefix(conversation_id, message_id),
        encode_reaction_key_component(emoji),
        encode_reaction_key_component(&actor_id.0)
    )
}

fn message_reaction_op_key(conversation_id: &ConversationId, op_message_id: &MessageId) -> String {
    format!(
        "{REACTION_OP_PREFIX}{}:{}",
        conversation_id.0, op_message_id.0
    )
}

fn encode_reaction_key_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn aggregate_cached_message_reactions(
    records: Option<&Vec<CachedMessageReaction>>,
) -> Vec<MessageReaction> {
    let mut by_emoji: HashMap<String, (HashSet<UserId>, Option<EmojiSourceRef>)> = HashMap::new();
    for record in records.into_iter().flatten() {
        let entry = by_emoji
            .entry(record.emoji.clone())
            .or_insert_with(|| (HashSet::new(), None));
        entry.0.insert(UserId::new(record.actor_id.clone()));
        if entry.1.is_none() {
            entry.1 = record.source_ref.as_ref().map(|source_ref| EmojiSourceRef {
                backend_id: crate::domain::backend::BackendId::new(source_ref.backend_id.clone()),
                ref_key: source_ref.ref_key.clone(),
            });
        }
    }

    let mut reactions = by_emoji
        .into_iter()
        .map(|(emoji, (actor_ids, source_ref))| {
            let mut actor_ids = actor_ids.into_iter().collect::<Vec<_>>();
            actor_ids.sort_by(|left, right| left.0.cmp(&right.0));
            MessageReaction {
                emoji,
                source_ref,
                actor_ids,
            }
        })
        .collect::<Vec<_>>();
    reactions.sort_by(|left, right| left.emoji.cmp(&right.emoji));
    reactions
}

fn user_profile_key(user_id: &UserId) -> String {
    format!("{USER_PROFILE_PREFIX}{}", user_id.0)
}

fn team_role_key(team_id: &str) -> String {
    format!("{TEAM_ROLE_PREFIX}{}", normalize_team_id_for_key(team_id))
}

fn conversation_team_key(conversation_id: &ConversationId) -> String {
    format!("{CONVERSATION_TEAM_PREFIX}{}", conversation_id.0)
}

fn normalize_team_id_for_key(team_id: &str) -> String {
    team_id.trim().to_ascii_lowercase()
}

fn crawl_checkpoint_key(conversation_id: &ConversationId) -> String {
    format!("{CRAWL_CHECKPOINT_PREFIX}{}", conversation_id.0)
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn encode<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    rmp_serde::to_vec(value).map_err(io::Error::other)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    rmp_serde::from_slice(bytes).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        attachment::AttachmentKind,
        attachment::AttachmentSummary,
        ids::UserId,
        message::{BroadcastKind, MessageFragment, MessageSendState},
    };

    fn temp_rocks_path(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let unique = format!(
            "zbase-local-store-test-{label}-{}-{}",
            std::process::id(),
            now_unix_ms()
        );
        path.push(unique);
        path
    }

    fn sample_message(conversation_id: &ConversationId, id: u64) -> MessageRecord {
        MessageRecord {
            id: MessageId::new(id.to_string()),
            conversation_id: conversation_id.clone(),
            author_id: UserId::new("alice"),
            reply_to: None,
            thread_root_id: None,
            timestamp_ms: Some(now_unix_ms()),
            event: None,
            link_previews: Vec::new(),
            permalink: String::new(),
            fragments: vec![MessageFragment::Text(format!("message-{id}"))],
            source_text: None,
            attachments: vec![AttachmentSummary {
                name: format!("file-{id}.txt"),
                kind: AttachmentKind::File,
                size_bytes: id,
                ..AttachmentSummary::default()
            }],
            reactions: Vec::new(),
            thread_reply_count: 0,
            send_state: MessageSendState::Sent,
            edited: None,
        }
    }

    fn sample_structured_message(conversation_id: &ConversationId, id: u64) -> MessageRecord {
        MessageRecord {
            id: MessageId::new(id.to_string()),
            conversation_id: conversation_id.clone(),
            author_id: UserId::new("alice"),
            reply_to: None,
            thread_root_id: None,
            timestamp_ms: Some(now_unix_ms()),
            event: None,
            link_previews: Vec::new(),
            permalink: String::new(),
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
                MessageFragment::Link {
                    url: "https://example.com".to_string(),
                    display: "example".to_string(),
                },
            ],
            source_text: None,
            attachments: Vec::new(),
            reactions: Vec::new(),
            thread_reply_count: 0,
            send_state: MessageSendState::Sent,
            edited: None,
        }
    }

    #[test]
    fn load_thread_messages_uses_secondary_index() {
        let path = temp_rocks_path("thread-index");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let conversation_id = ConversationId::new("kb_conv:test");
        let root_id = MessageId::new("100");

        let mut root = sample_message(&conversation_id, 100);
        root.thread_root_id = Some(root_id.clone());
        root.thread_reply_count = 2;
        let mut reply_one = sample_message(&conversation_id, 101);
        reply_one.reply_to = Some(root_id.clone());
        reply_one.thread_root_id = Some(root_id.clone());
        let mut reply_two = sample_message(&conversation_id, 102);
        reply_two.reply_to = Some(reply_one.id.clone());
        reply_two.thread_root_id = Some(root_id.clone());

        store.persist_message(&root).expect("persist root");
        store
            .persist_message(&reply_one)
            .expect("persist first reply");
        store
            .persist_message(&reply_two)
            .expect("persist second reply");

        let messages = store
            .load_thread_messages(&conversation_id, &root_id)
            .expect("load thread messages");
        assert_eq!(
            messages
                .iter()
                .map(|message| message.id.0.as_str())
                .collect::<Vec<_>>(),
            vec!["100", "101", "102"]
        );

        drop(store);
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn load_thread_messages_graph_includes_nested_descendants_without_root_materialization() {
        let path = temp_rocks_path("thread-graph");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let conversation_id = ConversationId::new("kb_conv:test");
        let root_id = MessageId::new("200");

        let mut root = sample_message(&conversation_id, 200);
        root.thread_root_id = Some(root_id.clone());
        let mut reply_one = sample_message(&conversation_id, 201);
        reply_one.reply_to = Some(root_id.clone());
        reply_one.thread_root_id = None;
        let mut reply_two = sample_message(&conversation_id, 202);
        reply_two.reply_to = Some(reply_one.id.clone());
        reply_two.thread_root_id = None;

        store.persist_message(&root).expect("persist root");
        store.persist_message(&reply_one).expect("persist child");
        store
            .persist_message(&reply_two)
            .expect("persist grandchild");

        let messages = store
            .load_thread_messages(&conversation_id, &root_id)
            .expect("load thread messages");
        assert_eq!(
            messages
                .iter()
                .map(|message| message.id.0.as_str())
                .collect::<Vec<_>>(),
            vec!["200", "201", "202"]
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn persist_message_rewrites_parent_edge_when_reply_target_changes() {
        let path = temp_rocks_path("edge-rewrite");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let conversation_id = ConversationId::new("kb_conv:test");

        let root_a = sample_message(&conversation_id, 300);
        let root_b = sample_message(&conversation_id, 301);
        let mut child = sample_message(&conversation_id, 302);
        child.reply_to = Some(root_a.id.clone());
        child.thread_root_id = Some(root_a.id.clone());

        store.persist_message(&root_a).expect("persist root a");
        store.persist_message(&root_b).expect("persist root b");
        store.persist_message(&child).expect("persist child");

        child.reply_to = Some(root_b.id.clone());
        child.thread_root_id = Some(root_b.id.clone());
        store.persist_message(&child).expect("re-parent child");

        let children_of_a = store
            .load_direct_reply_ids(&conversation_id, &root_a.id)
            .expect("load children of a");
        let children_of_b = store
            .load_direct_reply_ids(&conversation_id, &root_b.id)
            .expect("load children of b");
        assert!(
            children_of_a.is_empty(),
            "old parent edge should be removed"
        );
        assert_eq!(children_of_b, vec![child.id.clone()]);

        let ancestors = store
            .load_ancestor_chain(&conversation_id, &child.id)
            .expect("load ancestor chain");
        assert_eq!(ancestors.first().cloned(), Some(root_b.id.clone()));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn delete_message_removes_thread_edges() {
        let path = temp_rocks_path("delete-edges");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let conversation_id = ConversationId::new("kb_conv:test");
        let root = sample_message(&conversation_id, 400);
        let mut child = sample_message(&conversation_id, 401);
        child.reply_to = Some(root.id.clone());
        child.thread_root_id = Some(root.id.clone());
        store.persist_message(&root).expect("persist root");
        store.persist_message(&child).expect("persist child");

        store
            .delete_message(&conversation_id, &child.id)
            .expect("delete child");

        let children = store
            .load_direct_reply_ids(&conversation_id, &root.id)
            .expect("load direct replies");
        assert!(children.is_empty(), "delete should clean p2c edge");
        let ancestors = store
            .load_ancestor_chain(&conversation_id, &child.id)
            .expect("load ancestor chain after delete");
        assert!(
            ancestors.is_empty(),
            "delete should clean c2p edge lookup state"
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn repair_thread_edges_for_conversation_backfills_missing_edges() {
        let path = temp_rocks_path("edge-repair");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let conversation_id = ConversationId::new("kb_conv:test");
        let root = sample_message(&conversation_id, 500);
        let mut child = sample_message(&conversation_id, 501);
        child.reply_to = Some(root.id.clone());
        child.thread_root_id = Some(root.id.clone());

        let messages_cf = store.cf(CF_MESSAGES).expect("messages cf");
        store
            .db
            .put_cf(
                &messages_cf,
                message_key(&root.conversation_id, &root.id),
                encode(&CachedMessageRecord::from_domain(&root)).expect("encode root"),
            )
            .expect("write root directly");
        store
            .db
            .put_cf(
                &messages_cf,
                message_key(&child.conversation_id, &child.id),
                encode(&CachedMessageRecord::from_domain(&child)).expect("encode child"),
            )
            .expect("write child directly");

        let before = store
            .load_thread_descendants_bfs(&conversation_id, &root.id)
            .expect("load descendants before repair");
        assert!(before.is_empty(), "manual write should bypass edge indexes");

        let repaired = store
            .repair_thread_edges_for_conversation(&conversation_id, 100)
            .expect("repair edges");
        assert_eq!(repaired, 1);

        let after = store
            .load_thread_descendants_bfs(&conversation_id, &root.id)
            .expect("load descendants after repair");
        assert_eq!(after, vec![child.id.clone()]);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn thread_edge_migration_markers_roundtrip() {
        let path = temp_rocks_path("edge-migration-markers");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let conversation_id = ConversationId::new("kb_conv:migrate");

        assert!(
            !store
                .is_thread_edge_migration_complete()
                .expect("read migration complete marker")
        );
        assert!(
            !store
                .is_thread_edge_conversation_migrated(&conversation_id)
                .expect("read conversation marker")
        );

        store
            .mark_thread_edge_conversation_migrated(&conversation_id)
            .expect("mark conversation migrated");
        store
            .mark_thread_edge_migration_complete()
            .expect("mark migration complete");

        assert!(
            store
                .is_thread_edge_conversation_migrated(&conversation_id)
                .expect("read conversation marker after update")
        );
        assert!(
            store
                .is_thread_edge_migration_complete()
                .expect("read migration marker after update")
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn paging_before_after_uses_ordered_message_keys() {
        let path = temp_rocks_path("paging");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let conversation_id = ConversationId::new("kb_conv:test");

        for id in 1..=10 {
            store
                .persist_message(&sample_message(&conversation_id, id))
                .expect("persist message");
        }

        let recent = store
            .load_recent_messages_for_conversation(&conversation_id, 3)
            .expect("load recent");
        let recent_ids = recent
            .into_iter()
            .map(|message| message.id.0)
            .collect::<Vec<_>>();
        assert_eq!(recent_ids, vec!["8", "9", "10"]);

        let before = store
            .load_messages_before(&conversation_id, Some(&MessageId::new("8")), 3)
            .expect("load before");
        let before_ids = before
            .into_iter()
            .map(|message| message.id.0)
            .collect::<Vec<_>>();
        assert_eq!(before_ids, vec!["5", "6", "7"]);

        let after = store
            .load_messages_after(&conversation_id, &MessageId::new("7"), 2)
            .expect("load after");
        let after_ids = after
            .into_iter()
            .map(|message| message.id.0)
            .collect::<Vec<_>>();
        assert_eq!(after_ids, vec!["8", "9"]);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn structured_fragments_roundtrip_through_message_cache() {
        let path = temp_rocks_path("fragments");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let conversation_id = ConversationId::new("kb_conv:fragments");
        let message = sample_structured_message(&conversation_id, 42);

        store.persist_message(&message).expect("persist message");
        let loaded = store
            .load_recent_messages_for_conversation(&conversation_id, 5)
            .expect("load messages");
        assert_eq!(loaded.len(), 1);
        let restored = &loaded[0];
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

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn load_bootstrap_seed_clears_cache_on_schema_mismatch() {
        let path = temp_rocks_path("schema-mismatch");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let workspace_id = WorkspaceId::new("ws_primary");
        let conversation_id = ConversationId::new("kb_conv:schema");
        let payload = BootstrapPayload {
            workspace_ids: vec![workspace_id.clone()],
            active_workspace_id: Some(workspace_id),
            workspace_name: "Keybase".to_string(),
            channels: vec![ConversationSummary {
                id: conversation_id.clone(),
                title: "general".to_string(),
                kind: ConversationKind::Channel,
                topic: "chat".to_string(),
                group: None,
                unread_count: 0,
                mention_count: 0,
                muted: false,
                last_activity_ms: 0,
            }],
            direct_messages: Vec::new(),
            workspace_bindings: Vec::new(),
            conversation_bindings: Vec::new(),
            message_bindings: Vec::new(),
            selected_conversation_id: Some(conversation_id.clone()),
            selected_messages: vec![sample_structured_message(&conversation_id, 1)],
            unread_marker: None,
            account_display_name: Some("alice".to_string()),
        };
        store
            .persist_bootstrap_payload(&payload)
            .expect("persist bootstrap payload");

        let meta_cf = store.cf(CF_META).expect("meta cf");
        let meta_bytes = store
            .db
            .get_cf(&meta_cf, META_KEY)
            .expect("read meta")
            .expect("meta present");
        let mut meta: CachedMeta = decode(&meta_bytes).expect("decode meta");
        meta.schema_version = SCHEMA_VERSION.saturating_sub(1);
        store
            .db
            .put_cf(&meta_cf, META_KEY, encode(&meta).expect("encode meta"))
            .expect("write stale schema meta");

        let seed = store
            .load_bootstrap_seed(20, 20)
            .expect("load bootstrap seed");
        assert!(
            seed.is_none(),
            "stale schema should force cache reset and rehydration"
        );
        assert!(
            store
                .db
                .get_cf(&meta_cf, META_KEY)
                .expect("read meta after reset")
                .is_none(),
            "meta entry should be cleared after schema mismatch"
        );
        let ordered = store
            .load_ordered_conversation_ids(10)
            .expect("ordered conversations");
        assert!(ordered.is_empty(), "conversation cache should be cleared");
        let remaining_messages = store
            .load_recent_messages_for_conversation(&conversation_id, 5)
            .expect("remaining messages");
        assert!(
            remaining_messages.is_empty(),
            "message cache should be cleared"
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn crawl_checkpoint_roundtrip_and_clear() {
        let path = temp_rocks_path("checkpoint");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let conversation_id = ConversationId::new("kb_conv:test2");

        let checkpoint = CrawlCheckpoint {
            conversation_id: conversation_id.clone(),
            next_cursor: Some(vec![0x01, 0x02, 0x03]),
            completed: false,
            pages_crawled: 7,
            messages_crawled: 1400,
            updated_ms: 42,
        };
        store
            .upsert_crawl_checkpoint(&checkpoint)
            .expect("upsert checkpoint");

        let loaded = store
            .load_crawl_checkpoint(&conversation_id)
            .expect("load checkpoint")
            .expect("checkpoint exists");
        assert_eq!(loaded, checkpoint);

        store
            .clear_crawl_checkpoint(&conversation_id)
            .expect("clear checkpoint");
        let cleared = store
            .load_crawl_checkpoint(&conversation_id)
            .expect("load checkpoint after clear");
        assert!(cleared.is_none());

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn message_reaction_roundtrip_for_multiple_messages() {
        let path = temp_rocks_path("reactions");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let conversation_id = ConversationId::new("kb_conv:rxn");
        let message_one = MessageId::new("10");
        let message_two = MessageId::new("11");

        store
            .upsert_message_reaction(
                &conversation_id,
                &message_one,
                ":thumbsup:",
                None,
                &UserId::new("alice"),
                100,
            )
            .expect("upsert reaction one");
        store
            .upsert_message_reaction(
                &conversation_id,
                &message_one,
                ":thumbsup:",
                None,
                &UserId::new("bob"),
                101,
            )
            .expect("upsert reaction two");
        store
            .upsert_message_reaction(
                &conversation_id,
                &message_two,
                ":eyes:",
                None,
                &UserId::new("carol"),
                102,
            )
            .expect("upsert reaction three");

        let loaded = store
            .load_message_reactions_for_messages(
                &conversation_id,
                &[message_one.clone(), message_two.clone()],
            )
            .expect("load reactions");

        assert_eq!(
            loaded.get(&message_one).map(|items| items.len()),
            Some(2),
            "message one should have two actors"
        );
        assert_eq!(
            loaded.get(&message_two).map(|items| items.len()),
            Some(1),
            "message two should have one actor"
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn message_reaction_op_mapping_roundtrip() {
        let path = temp_rocks_path("reaction-op-map");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let conversation_id = ConversationId::new("kb_conv:rxn-op");
        let op_message_id = MessageId::new("9001");
        let target_message_id = MessageId::new("42");

        store
            .upsert_message_reaction_op(
                &conversation_id,
                &op_message_id,
                &target_message_id,
                ":thumbsup:",
                &UserId::new("alice"),
                123,
            )
            .expect("upsert reaction op");

        let resolved = store
            .take_message_reaction_op(&conversation_id, &op_message_id)
            .expect("take reaction op");
        assert_eq!(
            resolved,
            Some((
                target_message_id.clone(),
                ":thumbsup:".to_string(),
                UserId::new("alice")
            ))
        );

        let empty = store
            .take_message_reaction_op(&conversation_id, &op_message_id)
            .expect("take reaction op again");
        assert!(empty.is_none(), "reaction op should be consumed");

        store
            .upsert_message_reaction_op(
                &conversation_id,
                &op_message_id,
                &target_message_id,
                ":eyes:",
                &UserId::new("bob"),
                124,
            )
            .expect("reinsert reaction op");
        store
            .delete_message_reaction_op(&conversation_id, &op_message_id)
            .expect("delete reaction op");

        let missing = store
            .take_message_reaction_op(&conversation_id, &op_message_id)
            .expect("take after delete");
        assert!(missing.is_none(), "deleted reaction op should not resolve");

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn team_role_cache_roundtrip_and_conversation_binding_lookup() {
        let path = temp_rocks_path("team-roles");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let team_id = "kb_team:aa55";
        let conversation_one = ConversationId::new("kb_conv:team-one");
        let conversation_two = ConversationId::new("kb_conv:team-two");

        store
            .upsert_conversation_team_binding(&conversation_one, team_id, 100)
            .expect("upsert team binding one");
        store
            .upsert_conversation_team_binding(&conversation_two, team_id, 101)
            .expect("upsert team binding two");

        let mut roles = std::collections::HashMap::new();
        roles.insert(UserId::new("alice"), 3);
        roles.insert(UserId::new("bob"), 4);
        store
            .upsert_team_role_map(team_id, &roles, 222)
            .expect("upsert team role map");

        let binding = store
            .get_conversation_team_binding(&conversation_one)
            .expect("load team binding")
            .expect("binding exists");
        assert_eq!(binding.team_id, team_id);
        assert_eq!(binding.conversation_id, conversation_one.0);

        let conversation_ids = store
            .load_conversation_ids_for_team(team_id)
            .expect("load conversation ids for team")
            .into_iter()
            .map(|conversation_id| conversation_id.0)
            .collect::<Vec<_>>();
        assert_eq!(
            conversation_ids,
            vec![conversation_one.0.clone(), conversation_two.0.clone()]
        );

        let loaded_roles = store
            .get_team_role_map(team_id)
            .expect("load role map")
            .expect("role map exists");
        assert_eq!(loaded_roles.team_id, team_id);
        assert_eq!(loaded_roles.updated_ms, 222);

        let loaded_by_user = loaded_roles
            .roles
            .iter()
            .map(|entry| (entry.user_id.clone(), entry.role))
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(loaded_by_user.get("alice"), Some(&3));
        assert_eq!(loaded_by_user.get("bob"), Some(&4));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn team_emoji_cache_roundtrip_keeps_conversation_compatibility() {
        let path = temp_rocks_path("team-emojis");
        let store = LocalStore::open_at(path.clone()).expect("open rocks");
        let conversation_id = ConversationId::new("kb_conv:team-emoji");
        let team_id = "kb_team:emoji";

        let conversation_emojis = vec![CachedConversationEmoji {
            alias: "sbx".to_string(),
            unicode: None,
            source_url: Some("https://cdn.example.com/sbx.png".to_string()),
            asset_path: Some("/tmp/sbx.png".to_string()),
            updated_ms: 11,
        }];
        let team_emojis = vec![CachedConversationEmoji {
            alias: "sbx".to_string(),
            unicode: None,
            source_url: Some("https://cdn.example.com/sbx-team.png".to_string()),
            asset_path: Some("/tmp/sbx-team.png".to_string()),
            updated_ms: 22,
        }];

        store
            .replace_conversation_emojis(&conversation_id, &conversation_emojis)
            .expect("write conversation emojis");
        store
            .replace_team_emojis(team_id, &team_emojis)
            .expect("write team emojis");

        let loaded_conversation = store
            .load_conversation_emojis(&conversation_id)
            .expect("load conversation emojis");
        let loaded_team = store.load_team_emojis(team_id).expect("load team emojis");
        assert_eq!(loaded_conversation.len(), 1);
        assert_eq!(loaded_team.len(), 1);
        assert_eq!(
            loaded_conversation[0].asset_path.as_deref(),
            Some("/tmp/sbx.png")
        );
        assert_eq!(
            loaded_team[0].asset_path.as_deref(),
            Some("/tmp/sbx-team.png")
        );

        store
            .clear_conversation_emojis(&conversation_id)
            .expect("clear conversation emojis");
        let cleared_conversation = store
            .load_conversation_emojis(&conversation_id)
            .expect("load cleared conversation emojis");
        let still_loaded_team = store
            .load_team_emojis(team_id)
            .expect("load team emojis again");
        assert!(cleared_conversation.is_empty());
        assert_eq!(still_loaded_team.len(), 1);

        let _ = std::fs::remove_dir_all(path);
    }
}
