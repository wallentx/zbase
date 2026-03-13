use crate::{
    app::theme::ThemeVariant,
    views::{
        accent, mention, selectable_text::StyledRange, success, text_primary, text_secondary,
        warning,
    },
};
use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::Arc,
};
use tree_sitter_highlight::{HighlightConfiguration, HighlightEvent, Highlighter};

const MAX_CODE_BYTES_FOR_HIGHLIGHT: usize = 40_000;
const CODE_HIGHLIGHT_CACHE_MAX_ENTRIES: usize = 256;
const CODE_HIGHLIGHT_VERSION: u32 = 1;
const HIGHLIGHT_NAMES: &[&str] = &[
    "attribute",
    "boolean",
    "comment",
    "comment.documentation",
    "constant",
    "constant.builtin",
    "constructor",
    "embedded",
    "escape",
    "function",
    "function.builtin",
    "function.macro",
    "function.method",
    "keyword",
    "label",
    "number",
    "operator",
    "property",
    "punctuation.bracket",
    "punctuation.delimiter",
    "punctuation.special",
    "string",
    "string.special",
    "string.special.key",
    "type",
    "type.builtin",
    "variable",
    "variable.builtin",
    "variable.parameter",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CodeLanguage {
    Rust,
    Python,
    JavaScript,
    TypeScript,
    Json,
    Yaml,
    Toml,
    Bash,
    Sql,
    Go,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CodeHighlightKey {
    pub theme: ThemeVariant,
    pub lang: CodeLanguage,
    pub code_hash: u64,
    pub version: u32,
}

#[derive(Default)]
pub struct CodeHighlightCache {
    entries: HashMap<CodeHighlightKey, CodeHighlightCacheEntry>,
    inflight: HashSet<CodeHighlightKey>,
    use_tick: u64,
}

struct CodeHighlightCacheEntry {
    last_used_tick: u64,
    ranges: Arc<Vec<StyledRange>>,
}

impl CodeHighlightCache {
    pub fn get(&mut self, key: &CodeHighlightKey) -> Option<Arc<Vec<StyledRange>>> {
        self.use_tick = self.use_tick.wrapping_add(1);
        let tick = self.use_tick;
        let entry = self.entries.get_mut(key)?;
        entry.last_used_tick = tick;
        Some(entry.ranges.clone())
    }

    pub fn mark_inflight(&mut self, key: CodeHighlightKey) -> bool {
        self.inflight.insert(key)
    }

    pub fn finish_inflight(&mut self, key: &CodeHighlightKey) {
        self.inflight.remove(key);
    }

    pub fn insert(&mut self, key: CodeHighlightKey, ranges: Vec<StyledRange>) {
        self.use_tick = self.use_tick.wrapping_add(1);
        let tick = self.use_tick;
        self.inflight.remove(&key);
        self.entries.insert(
            key,
            CodeHighlightCacheEntry {
                last_used_tick: tick,
                ranges: Arc::new(ranges),
            },
        );
        self.prune();
    }

    fn prune(&mut self) {
        if self.entries.len() <= CODE_HIGHLIGHT_CACHE_MAX_ENTRIES {
            return;
        }
        let mut by_last_used = self
            .entries
            .iter()
            .map(|(key, entry)| (*key, entry.last_used_tick))
            .collect::<Vec<_>>();
        by_last_used.sort_by_key(|(_, tick)| *tick);
        let target_len = CODE_HIGHLIGHT_CACHE_MAX_ENTRIES;
        let remove_count = by_last_used.len().saturating_sub(target_len);
        for (key, _) in by_last_used.into_iter().take(remove_count) {
            self.entries.remove(&key);
            self.inflight.remove(&key);
        }
    }
}

impl CodeLanguage {
    pub fn from_tag(tag: &str) -> Option<Self> {
        match tag.trim().to_ascii_lowercase().as_str() {
            "rs" | "rust" => Some(Self::Rust),
            "py" | "python" => Some(Self::Python),
            "js" | "javascript" => Some(Self::JavaScript),
            "ts" | "typescript" => Some(Self::TypeScript),
            "json" => Some(Self::Json),
            "yml" | "yaml" => Some(Self::Yaml),
            "toml" => Some(Self::Toml),
            "sh" | "bash" | "shell" | "zsh" => Some(Self::Bash),
            "sql" => Some(Self::Sql),
            "go" | "golang" => Some(Self::Go),
            _ => None,
        }
    }
}

pub fn detect_language(code: &str) -> Option<CodeLanguage> {
    let trimmed = code.trim_start();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        if is_probably_json(trimmed) {
            return Some(CodeLanguage::Json);
        }
    }
    if trimmed.starts_with("---") || trimmed.contains("\n---") {
        return Some(CodeLanguage::Yaml);
    }
    if trimmed.starts_with("#!") {
        return Some(CodeLanguage::Bash);
    }
    if is_probably_toml(trimmed) {
        return Some(CodeLanguage::Toml);
    }
    // Keyword scoring fallback for common languages.
    let lower = trimmed.to_ascii_lowercase();
    let mut scored = [
        (
            CodeLanguage::Rust,
            score_keywords(
                &lower,
                &[
                    "fn ", "let ", "impl ", "match ", "use ", "pub ", "crate::", "self",
                ],
            ),
        ),
        (
            CodeLanguage::Python,
            score_keywords(
                &lower,
                &[
                    "def ", "import ", "from ", "self", "elif ", "none", "true", "false",
                ],
            ),
        ),
        (
            CodeLanguage::JavaScript,
            score_keywords(
                &lower,
                &[
                    "function ",
                    "const ",
                    "let ",
                    "=>",
                    "import ",
                    "export ",
                    "console.",
                ],
            ),
        ),
        (
            CodeLanguage::TypeScript,
            score_keywords(
                &lower,
                &[
                    "interface ",
                    "type ",
                    "implements ",
                    "readonly ",
                    ": ",
                    "as const",
                ],
            ),
        ),
        (
            CodeLanguage::Go,
            score_keywords(
                &lower,
                &["func ", "package ", "import ", ":=", "defer ", "go "],
            ),
        ),
        (
            CodeLanguage::Sql,
            score_keywords(
                &lower,
                &[
                    "select ", "from ", "where ", "join ", "group by", "order by", "insert ",
                    "update ", "delete ",
                ],
            ),
        ),
    ];
    scored.sort_by(|a, b| b.1.cmp(&a.1));
    (scored[0].1 > 0).then_some(scored[0].0)
}

pub fn resolve_language(explicit: Option<&str>, code: &str) -> Option<CodeLanguage> {
    explicit
        .and_then(CodeLanguage::from_tag)
        .or_else(|| detect_language(code))
}

pub fn highlight_key(theme: ThemeVariant, lang: CodeLanguage, code: &str) -> CodeHighlightKey {
    CodeHighlightKey {
        theme,
        lang,
        code_hash: stable_code_hash(code),
        version: CODE_HIGHLIGHT_VERSION,
    }
}

pub fn highlight(code: &str, lang: CodeLanguage, theme: ThemeVariant) -> Vec<StyledRange> {
    if code.is_empty() || code.len() > MAX_CODE_BYTES_FOR_HIGHLIGHT {
        return Vec::new();
    }
    let (language, name, highlights_query) = language_and_query(lang);
    let mut config =
        match HighlightConfiguration::new(language.clone(), name, highlights_query, "", "") {
            Ok(config) => config,
            Err(_) => HighlightConfiguration::new(language, name, "", "", "")
                .expect("empty highlight config"),
        };
    config.configure(HIGHLIGHT_NAMES);

    let mut highlighter = Highlighter::new();
    let Ok(events) = highlighter.highlight(&config, code.as_bytes(), None, |_| None) else {
        return Vec::new();
    };

    let mut active: Vec<usize> = Vec::new();
    let mut out: Vec<StyledRange> = Vec::new();

    for event in events {
        match event {
            Ok(HighlightEvent::HighlightStart(s)) => active.push(s.0),
            Ok(HighlightEvent::HighlightEnd) => {
                let _ = active.pop();
            }
            Ok(HighlightEvent::Source { start, end }) => {
                if start >= end || end > code.len() {
                    continue;
                }
                let Some(&highlight_ix) = active.last() else {
                    continue;
                };
                let Some(name) = HIGHLIGHT_NAMES.get(highlight_ix).copied() else {
                    continue;
                };
                let Some(style) = style_for_highlight(name, theme) else {
                    continue;
                };
                push_merged(
                    &mut out,
                    StyledRange {
                        byte_range: start..end,
                        color: Some(style.color),
                        background_color: None,
                        bold: style.bold,
                        italic: style.italic,
                        strikethrough: false,
                    },
                );
            }
            Err(_) => {}
        }
    }

    out
}

fn language_and_query(lang: CodeLanguage) -> (tree_sitter::Language, &'static str, &'static str) {
    match lang {
        CodeLanguage::Rust => (
            tree_sitter_rust::LANGUAGE.into(),
            "rust",
            include_str!("code_highlight/queries/rust.scm"),
        ),
        CodeLanguage::Python => (
            tree_sitter_python::LANGUAGE.into(),
            "python",
            include_str!("code_highlight/queries/python.scm"),
        ),
        CodeLanguage::JavaScript => (
            tree_sitter_javascript::LANGUAGE.into(),
            "javascript",
            include_str!("code_highlight/queries/javascript.scm"),
        ),
        CodeLanguage::TypeScript => (
            tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            "typescript",
            include_str!("code_highlight/queries/typescript.scm"),
        ),
        CodeLanguage::Json => (
            tree_sitter_json::LANGUAGE.into(),
            "json",
            include_str!("code_highlight/queries/json.scm"),
        ),
        CodeLanguage::Yaml => (
            tree_sitter_yaml::LANGUAGE.into(),
            "yaml",
            include_str!("code_highlight/queries/yaml.scm"),
        ),
        CodeLanguage::Toml => (
            tree_sitter_toml_ng::LANGUAGE.into(),
            "toml",
            include_str!("code_highlight/queries/toml.scm"),
        ),
        CodeLanguage::Bash => (
            tree_sitter_bash::LANGUAGE.into(),
            "bash",
            include_str!("code_highlight/queries/bash.scm"),
        ),
        CodeLanguage::Sql => (
            tree_sitter_sequel::LANGUAGE.into(),
            "sql",
            tree_sitter_sequel::HIGHLIGHTS_QUERY,
        ),
        CodeLanguage::Go => (
            tree_sitter_go::LANGUAGE.into(),
            "go",
            include_str!("code_highlight/queries/go.scm"),
        ),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Style {
    color: u32,
    bold: bool,
    italic: bool,
}

fn style_for_highlight(name: &str, _theme: ThemeVariant) -> Option<Style> {
    // Collapse dotted names (e.g. "type.builtin" -> "type") for palette mapping.
    let base = name.split('.').next().unwrap_or(name);
    let (color, bold, italic) = match base {
        "comment" => (text_secondary(), false, true),
        "string" => (success(), false, false),
        "number" => (warning(), false, false),
        "boolean" => (warning(), true, false),
        "constant" => (warning(), true, false),
        "keyword" => (accent(), true, false),
        "type" => (mention(), true, false),
        "function" => (accent(), false, false),
        "property" => (text_primary(), false, false),
        "variable" => (text_primary(), false, false),
        "label" => (accent(), false, false),
        "operator" | "punctuation" | "escape" | "attribute" | "embedded" => {
            (text_secondary(), false, false)
        }
        _ => return None,
    };
    Some(Style {
        color,
        bold,
        italic,
    })
}

fn push_merged(out: &mut Vec<StyledRange>, next: StyledRange) {
    if let Some(last) = out.last_mut() {
        if last.color == next.color
            && last.background_color == next.background_color
            && last.bold == next.bold
            && last.italic == next.italic
            && last.strikethrough == next.strikethrough
            && last.byte_range.end == next.byte_range.start
        {
            last.byte_range.end = next.byte_range.end;
            return;
        }
    }
    out.push(next);
}

fn is_probably_json(text: &str) -> bool {
    // Very cheap heuristic: must contain either ':' (object) or ',' (array/object), and use double quotes for keys.
    let bytes = text.as_bytes();
    let mut saw_double_quote = false;
    let mut saw_colon = false;
    let mut saw_comma = false;
    for &b in bytes.iter().take(2048) {
        match b {
            b'"' => saw_double_quote = true,
            b':' => saw_colon = true,
            b',' => saw_comma = true,
            _ => {}
        }
    }
    saw_double_quote && (saw_colon || saw_comma)
}

fn is_probably_toml(text: &str) -> bool {
    // TOML commonly has `key = value` and section headers like `[section]`.
    let sample = text.lines().take(40).collect::<Vec<_>>();
    let mut saw_assignment = false;
    let mut saw_section = false;
    for line in sample {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            saw_section = true;
        }
        if line.contains('=') {
            saw_assignment = true;
        }
    }
    saw_assignment && (saw_section || text.contains('\n'))
}

fn score_keywords(lower: &str, tokens: &[&str]) -> i32 {
    tokens
        .iter()
        .map(|t| if lower.contains(t) { 1 } else { 0 })
        .sum()
}

fn stable_code_hash(code: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    code.len().hash(&mut hasher);
    code.as_bytes().hash(&mut hasher);
    hasher.finish()
}
