use std::{
    io,
    path::Path,
    sync::mpsc::{self, Receiver, Sender},
    thread,
};

use tantivy::{
    Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term,
    collector::TopDocs,
    directory::MmapDirectory,
    doc,
    query::{AllQuery, BooleanQuery, Occur, QueryParser, TermQuery},
    schema::{Field, IndexRecordOption, STORED, STRING, Schema, TEXT, Value},
    snippet::SnippetGenerator,
};

use crate::{
    domain::ids::{ConversationId, MessageId},
    services::local_store::paths,
    util::interactive_qos::search_index_commit_interval,
};

#[derive(Clone, Debug)]
pub struct SearchDocument {
    pub workspace_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub author: String,
    pub body: String,
    pub filename_tokens: String,
    pub timestamp: i64,
}

#[derive(Clone, Debug)]
pub struct SearchHit {
    pub conversation_id: ConversationId,
    pub message_id: MessageId,
    pub snippet: String,
    pub highlight_ranges: Vec<(usize, usize)>,
}

#[derive(Copy, Clone)]
struct SearchFields {
    doc_id: Field,
    workspace_id: Field,
    conversation_id: Field,
    message_id: Field,
    author: Field,
    body: Field,
    filenames: Field,
    timestamp: Field,
}

enum IndexCommand {
    Upsert(SearchDocument),
    Shutdown,
}

pub struct SearchIndex {
    index: Index,
    reader: IndexReader,
    fields: SearchFields,
    sender: Sender<IndexCommand>,
}

impl SearchIndex {
    pub fn open() -> io::Result<Self> {
        Self::open_at(paths::tantivy_path())
    }

    pub fn open_at(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;

        let schema = build_schema();
        let index = open_or_rebuild_index(path, schema)?;

        let active_schema = index.schema();
        let fields = SearchFields {
            doc_id: active_schema
                .get_field("doc_id")
                .map_err(io::Error::other)?,
            workspace_id: active_schema
                .get_field("workspace_id")
                .map_err(io::Error::other)?,
            conversation_id: active_schema
                .get_field("conversation_id")
                .map_err(io::Error::other)?,
            message_id: active_schema
                .get_field("message_id")
                .map_err(io::Error::other)?,
            author: active_schema
                .get_field("author")
                .map_err(io::Error::other)?,
            body: active_schema.get_field("body").map_err(io::Error::other)?,
            filenames: active_schema
                .get_field("filenames")
                .map_err(io::Error::other)?,
            timestamp: active_schema
                .get_field("timestamp")
                .map_err(io::Error::other)?,
        };

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .map_err(io::Error::other)?;
        let writer = index.writer(50_000_000).map_err(io::Error::other)?;

        let (sender, receiver) = mpsc::channel::<IndexCommand>();
        let writer_reader = reader.clone();
        thread::spawn(move || run_writer_loop(writer, writer_reader, fields, receiver));

        Ok(Self {
            index,
            reader,
            fields,
            sender,
        })
    }

    pub fn enqueue_upsert(&self, document: SearchDocument) -> io::Result<()> {
        self.sender
            .send(IndexCommand::Upsert(document))
            .map_err(io::Error::other)
    }

    pub fn search_workspace(
        &self,
        workspace_id: &str,
        query: &str,
        limit: usize,
    ) -> io::Result<Vec<SearchHit>> {
        self.search_scoped_term(
            Term::from_field_text(self.fields.workspace_id, workspace_id),
            query,
            limit,
        )
    }

    pub fn search_conversation(
        &self,
        conversation_id: &str,
        query: &str,
        limit: usize,
    ) -> io::Result<Vec<SearchHit>> {
        self.search_scoped_term(
            Term::from_field_text(self.fields.conversation_id, conversation_id),
            query,
            limit,
        )
    }

    fn search_scoped_term(
        &self,
        scope_term: Term,
        query: &str,
        limit: usize,
    ) -> io::Result<Vec<SearchHit>> {
        self.reader.reload().map_err(io::Error::other)?;
        let searcher = self.reader.searcher();

        let parser = QueryParser::for_index(
            &self.index,
            vec![self.fields.body, self.fields.author, self.fields.filenames],
        );
        let text_query = if query.trim().is_empty() {
            Box::new(AllQuery) as Box<dyn tantivy::query::Query>
        } else {
            parser.parse_query(query).map_err(io::Error::other)?
        };
        let mut snippet_generator = if query.trim().is_empty() {
            None
        } else {
            let mut generator =
                SnippetGenerator::create(&searcher, text_query.as_ref(), self.fields.body)
                    .map_err(io::Error::other)?;
            generator.set_max_num_chars(180);
            Some(generator)
        };
        let workspace_query = TermQuery::new(scope_term, IndexRecordOption::Basic);
        let combined = BooleanQuery::new(vec![
            (Occur::Must, Box::new(workspace_query)),
            (Occur::Must, text_query),
        ]);

        let mut hits = Vec::new();
        let top_docs = searcher
            .search(&combined, &TopDocs::with_limit(limit))
            .map_err(io::Error::other)?;
        for (_score, address) in top_docs {
            let document = searcher
                .doc::<TantivyDocument>(address)
                .map_err(io::Error::other)?;
            let conversation_id = document
                .get_first(self.fields.conversation_id)
                .and_then(|value| value.as_str())
                .map(str::to_string);
            let message_id = document
                .get_first(self.fields.message_id)
                .and_then(|value| value.as_str())
                .map(str::to_string);
            let body = document
                .get_first(self.fields.body)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            let filenames = document
                .get_first(self.fields.filenames)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            let (snippet_text, highlight_ranges) =
                if let Some(generator) = snippet_generator.as_mut() {
                    let snippet = generator.snippet_from_doc(&document);
                    let fragment = snippet.fragment().to_string();
                    if fragment.trim().is_empty() {
                        (
                            truncate_snippet(
                                if body.trim().is_empty() {
                                    &filenames
                                } else {
                                    &body
                                },
                                180,
                            ),
                            Vec::new(),
                        )
                    } else {
                        (
                            fragment,
                            snippet
                                .highlighted()
                                .iter()
                                .map(|range| (range.start, range.end))
                                .collect::<Vec<_>>(),
                        )
                    }
                } else {
                    (
                        truncate_snippet(
                            if body.trim().is_empty() {
                                &filenames
                            } else {
                                &body
                            },
                            180,
                        ),
                        Vec::new(),
                    )
                };
            if let (Some(conversation_id), Some(message_id)) = (conversation_id, message_id) {
                hits.push(SearchHit {
                    conversation_id: ConversationId::new(conversation_id),
                    message_id: MessageId::new(message_id),
                    snippet: snippet_text,
                    highlight_ranges,
                });
            }
        }

        Ok(hits)
    }
}

impl Drop for SearchIndex {
    fn drop(&mut self) {
        let _ = self.sender.send(IndexCommand::Shutdown);
    }
}

fn build_schema() -> Schema {
    let mut builder = Schema::builder();
    builder.add_text_field("doc_id", STRING | STORED);
    builder.add_text_field("workspace_id", STRING | STORED);
    builder.add_text_field("conversation_id", STRING | STORED);
    builder.add_text_field("message_id", STRING | STORED);
    builder.add_text_field("author", TEXT | STORED);
    builder.add_text_field("body", TEXT | STORED);
    builder.add_text_field("filenames", TEXT | STORED);
    builder.add_i64_field("timestamp", STORED);
    builder.build()
}

fn open_or_rebuild_index(path: &Path, schema: Schema) -> io::Result<Index> {
    let directory = MmapDirectory::open(path).map_err(io::Error::other)?;
    match Index::open_or_create(directory, schema.clone()) {
        Ok(index) => {
            // Rebuild if the on-disk schema predates filename indexing support.
            if index.schema().get_field("filenames").is_ok() {
                Ok(index)
            } else {
                drop(index);
                rebuild_index(path, schema)
            }
        }
        Err(error) => {
            // Tantivy refuses opening when schema changed. Since the search index is
            // a cache, recover by rebuilding in-place instead of crashing startup.
            let text = error.to_string().to_ascii_lowercase();
            if text.contains("schema") && text.contains("does not match") {
                rebuild_index(path, schema)
            } else {
                Err(io::Error::other(error))
            }
        }
    }
}

fn rebuild_index(path: &Path, schema: Schema) -> io::Result<Index> {
    let _ = std::fs::remove_dir_all(path);
    std::fs::create_dir_all(path)?;
    let directory = MmapDirectory::open(path).map_err(io::Error::other)?;
    Index::open_or_create(directory, schema).map_err(io::Error::other)
}

fn run_writer_loop(
    mut writer: IndexWriter,
    reader: IndexReader,
    fields: SearchFields,
    receiver: Receiver<IndexCommand>,
) {
    let mut dirty = false;
    loop {
        match receiver.recv_timeout(search_index_commit_interval()) {
            Ok(IndexCommand::Upsert(document)) => {
                let doc_id = format!(
                    "{}|{}|{}",
                    document.workspace_id, document.conversation_id, document.message_id
                );
                writer.delete_term(Term::from_field_text(fields.doc_id, &doc_id));
                let _ = writer.add_document(doc!(
                    fields.doc_id => doc_id,
                    fields.workspace_id => document.workspace_id,
                    fields.conversation_id => document.conversation_id,
                    fields.message_id => document.message_id,
                    fields.author => document.author,
                    fields.body => document.body,
                    fields.filenames => document.filename_tokens,
                    fields.timestamp => document.timestamp,
                ));
                dirty = true;
            }
            Ok(IndexCommand::Shutdown) => {
                if dirty {
                    let _ = writer.commit();
                    let _ = reader.reload();
                }
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if dirty {
                    let _ = writer.commit();
                    let _ = reader.reload();
                    dirty = false;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn truncate_snippet(body: &str, max_len: usize) -> String {
    if body.chars().count() <= max_len {
        return body.to_string();
    }
    let truncated = body.chars().take(max_len).collect::<String>();
    format!("{truncated}…")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::Duration;
    use tantivy::{Index, directory::MmapDirectory, schema::Schema};

    fn temp_index_path(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let unique = format!(
            "kbui-search-index-test-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis())
                .unwrap_or(0)
        );
        path.push(unique);
        path
    }

    #[test]
    fn filename_tokens_are_searchable_within_workspace() {
        let path = temp_index_path("filenames");
        let index = SearchIndex::open_at(&path).expect("open index");
        index
            .enqueue_upsert(SearchDocument {
                workspace_id: "ws_primary".to_string(),
                conversation_id: "conv-1".to_string(),
                message_id: "42".to_string(),
                author: "alice".to_string(),
                body: String::new(),
                filename_tokens: "roadmap.pdf q2-plan.docx".to_string(),
                timestamp: 42,
            })
            .expect("enqueue upsert");

        std::thread::sleep(Duration::from_millis(700));

        let hits = index
            .search_workspace("ws_primary", "roadmap", 10)
            .expect("search by filename");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].conversation_id.0, "conv-1");
        assert_eq!(hits[0].message_id.0, "42");

        let scoped_miss = index
            .search_workspace("ws_other", "roadmap", 10)
            .expect("search scoped workspace");
        assert!(scoped_miss.is_empty());

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn conversation_scope_filters_hits() {
        let path = temp_index_path("conversation-scope");
        let index = SearchIndex::open_at(&path).expect("open index");
        index
            .enqueue_upsert(SearchDocument {
                workspace_id: "ws_primary".to_string(),
                conversation_id: "conv-a".to_string(),
                message_id: "100".to_string(),
                author: "alice".to_string(),
                body: "release checklist ready".to_string(),
                filename_tokens: String::new(),
                timestamp: 100,
            })
            .expect("enqueue conv a");
        index
            .enqueue_upsert(SearchDocument {
                workspace_id: "ws_primary".to_string(),
                conversation_id: "conv-b".to_string(),
                message_id: "101".to_string(),
                author: "bob".to_string(),
                body: "release checklist draft".to_string(),
                filename_tokens: String::new(),
                timestamp: 101,
            })
            .expect("enqueue conv b");

        std::thread::sleep(Duration::from_millis(700));

        let conv_a_hits = index
            .search_conversation("conv-a", "release", 10)
            .expect("search conv a");
        assert_eq!(conv_a_hits.len(), 1);
        assert_eq!(conv_a_hits[0].conversation_id.0, "conv-a");
        assert_eq!(conv_a_hits[0].message_id.0, "100");

        let conv_b_hits = index
            .search_conversation("conv-b", "release", 10)
            .expect("search conv b");
        assert_eq!(conv_b_hits.len(), 1);
        assert_eq!(conv_b_hits[0].conversation_id.0, "conv-b");
        assert_eq!(conv_b_hits[0].message_id.0, "101");

        let conv_none = index
            .search_conversation("conv-missing", "release", 10)
            .expect("search missing conversation");
        assert!(conv_none.is_empty());

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn schema_mismatch_is_rebuilt_automatically() {
        let path = temp_index_path("schema-mismatch");
        std::fs::create_dir_all(&path).expect("create index dir");

        // Seed disk with a legacy schema (without `filenames`) to simulate
        // pre-upgrade user data.
        let legacy_schema = legacy_schema_without_filenames();
        let legacy_dir = MmapDirectory::open(&path).expect("open legacy dir");
        let legacy_index = Index::open_or_create(legacy_dir, legacy_schema).expect("legacy index");
        drop(legacy_index);

        let index = SearchIndex::open_at(&path).expect("open with rebuild");
        index
            .enqueue_upsert(SearchDocument {
                workspace_id: "ws_primary".to_string(),
                conversation_id: "conv-legacy".to_string(),
                message_id: "7".to_string(),
                author: "bob".to_string(),
                body: String::new(),
                filename_tokens: "legacy-upgraded.txt".to_string(),
                timestamp: 7,
            })
            .expect("enqueue upsert after rebuild");
        std::thread::sleep(Duration::from_millis(700));
        let hits = index
            .search_workspace("ws_primary", "legacy-upgraded", 10)
            .expect("search after rebuild");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].conversation_id.0, "conv-legacy");
        assert_eq!(hits[0].message_id.0, "7");

        let _ = std::fs::remove_dir_all(path);
    }

    fn legacy_schema_without_filenames() -> Schema {
        let mut builder = Schema::builder();
        builder.add_text_field("doc_id", STRING | STORED);
        builder.add_text_field("workspace_id", STRING | STORED);
        builder.add_text_field("conversation_id", STRING | STORED);
        builder.add_text_field("message_id", STRING | STORED);
        builder.add_text_field("author", TEXT | STORED);
        builder.add_text_field("body", TEXT | STORED);
        builder.add_i64_field("timestamp", STORED);
        builder.build()
    }
}
