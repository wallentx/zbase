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

const MIN_KEYWORD_SCORE: i32 = 2;

pub fn detect_language(code: &str) -> Option<CodeLanguage> {
    let trimmed = code.trim_start();
    if trimmed.is_empty() {
        return None;
    }
    if looks_like_prose(trimmed) {
        return None;
    }
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        if is_probably_json(trimmed) {
            return Some(CodeLanguage::Json);
        }
    }
    if (trimmed.starts_with("---") || trimmed.contains("\n---"))
        && has_yaml_key_value_lines(trimmed)
    {
        return Some(CodeLanguage::Yaml);
    }
    if trimmed.starts_with("#!") {
        return Some(CodeLanguage::Bash);
    }
    if is_probably_toml(trimmed) {
        return Some(CodeLanguage::Toml);
    }
    let lower = trimmed.to_ascii_lowercase();
    let mut scored = [
        (
            CodeLanguage::Rust,
            score_keywords(
                &lower,
                &[
                    "fn ", "let ", "impl ", "match ", "pub fn", "crate::", "mut ", "&self",
                    "use std::", "-> ", "pub struct", "enum ",
                ],
            ),
        ),
        (
            CodeLanguage::Python,
            score_keywords(
                &lower,
                &[
                    "def ", "import ", "elif ", "print(", "class ", "__init__", "self.",
                    "return ", "lambda ", "except ",
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
                    "=>",
                    "export ",
                    "console.",
                    "require(",
                    "async ",
                    "await ",
                    "document.",
                ],
            ),
        ),
        (
            CodeLanguage::TypeScript,
            score_keywords(
                &lower,
                &[
                    "interface ",
                    "implements ",
                    "readonly ",
                    "as const",
                    "export type",
                    "export interface",
                    ": string",
                    ": number",
                    ": boolean",
                ],
            ),
        ),
        (
            CodeLanguage::Go,
            score_keywords(
                &lower,
                &[
                    "func ", "package ", ":=", "defer ", "go ", "fmt.", "chan ", "goroutine",
                ],
            ),
        ),
        (
            CodeLanguage::Sql,
            score_keywords(
                &lower,
                &[
                    "select ", "where ", "join ", "group by", "order by", "insert into",
                    "create table", "alter table", "drop table",
                ],
            ),
        ),
    ];
    scored.sort_by(|a, b| b.1.cmp(&a.1));
    (scored[0].1 >= MIN_KEYWORD_SCORE).then_some(scored[0].0)
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
    // `crate::views::*` palette helpers read from a thread-local theme. Since highlighting is often
    // computed on a background task, ensure the requested theme is applied on this thread while
    // we map highlight classes to palette colors.
    crate::views::with_theme(theme, || {
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
    })
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

fn style_for_highlight(name: &str, theme: ThemeVariant) -> Option<Style> {
    // Collapse dotted names (e.g. "type.builtin" -> "type") for palette mapping.
    let base = name.split('.').next().unwrap_or(name);
    let (color, bold, italic) = match (theme, base) {
        // Dark mode: prioritize legibility over subtlety.
        (ThemeVariant::Dark, "keyword") => (text_primary(), true, false),

        // Shared mapping (light + dark).
        (_, "comment") => (text_secondary(), false, true),
        (_, "string") => (success(), false, false),
        (_, "number") => (warning(), false, false),
        (_, "boolean") => (warning(), true, false),
        (_, "constant") => (warning(), true, false),
        (_, "keyword") => (accent(), true, false),
        (_, "type") => (mention(), true, false),
        (_, "function") => (accent(), false, false),
        (_, "property") => (text_primary(), false, false),
        (_, "variable") => (text_primary(), false, false),
        (_, "label") => (accent(), false, false),
        (_, "operator" | "punctuation" | "escape" | "attribute" | "embedded") => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_keywords_are_high_contrast_in_dark_mode() {
        let code = "SELECT 1 FROM users;";
        let ranges = highlight(code, CodeLanguage::Sql, ThemeVariant::Dark);
        let select_start = code.find("SELECT").expect("SELECT present");
        let select_end = select_start + "SELECT".len();

        let select_range = ranges
            .iter()
            .find(|r| r.byte_range.start <= select_start && r.byte_range.end >= select_end)
            .expect("expected a styled range covering SELECT");

        let expected = crate::views::with_theme(ThemeVariant::Dark, || text_primary());
        assert_eq!(select_range.color, Some(expected));
    }
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
    let bytes = text.as_bytes();
    let mut quote_count = 0u32;
    let mut colon_count = 0u32;
    let mut brace_bracket_count = 0u32;
    for &b in bytes.iter().take(2048) {
        match b {
            b'"' => quote_count += 1,
            b':' => colon_count += 1,
            b'{' | b'}' | b'[' | b']' => brace_bracket_count += 1,
            _ => {}
        }
    }
    quote_count >= 2 && colon_count >= 1 && brace_bracket_count >= 2
}

fn is_probably_toml(text: &str) -> bool {
    let sample = text.lines().take(40).collect::<Vec<_>>();
    let mut assignment_count = 0;
    let mut saw_section = false;
    for line in &sample {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') && !line.contains(' ') {
            saw_section = true;
        }
        if let Some(eq_pos) = line.find('=') {
            let key = line[..eq_pos].trim();
            if !key.is_empty()
                && !key.contains(' ')
                && key.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
            {
                assignment_count += 1;
            }
        }
    }
    saw_section && assignment_count >= 2
}

fn looks_like_prose(text: &str) -> bool {
    let lines: Vec<&str> = text.lines().take(30).collect();
    if lines.is_empty() {
        return false;
    }
    let mut total_words = 0usize;
    let mut long_words = 0usize;
    for line in &lines {
        for word in line.split_whitespace() {
            let alpha: String = word.chars().filter(|c| c.is_alphabetic()).collect();
            if alpha.len() >= 2 {
                total_words += 1;
                if alpha.len() >= 4 {
                    long_words += 1;
                }
            }
        }
    }
    if total_words < 8 {
        return false;
    }
    let has_sentence_punctuation = text.contains(". ") || text.contains("? ") || text.contains("! ");
    let long_word_ratio = long_words as f64 / total_words as f64;
    let avg_words_per_line = total_words as f64 / lines.len() as f64;
    has_sentence_punctuation && long_word_ratio > 0.6 && avg_words_per_line > 5.0
}

fn has_yaml_key_value_lines(text: &str) -> bool {
    let mut kv_count = 0;
    for line in text.lines().take(30) {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed == "---" {
            continue;
        }
        if trimmed.contains(": ") || trimmed.ends_with(':') {
            kv_count += 1;
        }
    }
    kv_count >= 2
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
