use std::cmp::Reverse;

use emojis::{Emoji, SkinTone};

const DEFAULT_MAX_RESULTS: usize = 50;

pub const SKIN_TONE_OPTIONS: [SkinTone; 6] = [
    SkinTone::Default,
    SkinTone::Light,
    SkinTone::MediumLight,
    SkinTone::Medium,
    SkinTone::MediumDark,
    SkinTone::Dark,
];

#[derive(Clone, Debug)]
pub enum EmojiPickerItem {
    Stock(&'static Emoji),
    Custom {
        alias: String,
        unicode: Option<String>,
        asset_path: Option<String>,
    },
}

#[derive(Clone, Debug, Default)]
pub struct EmojiPickerModel {
    pub query: String,
    pub selected_skin_tone: Option<SkinTone>,
    pub active_group: Option<emojis::Group>,
    pub hovered: Option<EmojiPickerItem>,
    pub recent_aliases: Vec<String>,
    pub skin_tone_expanded: bool,
}

impl EmojiPickerItem {
    pub fn label(&self) -> String {
        match self {
            Self::Stock(emoji) => emoji
                .shortcode()
                .map(|shortcode| format!(":{shortcode}:"))
                .unwrap_or_else(|| emoji.name().to_string()),
            Self::Custom { alias, .. } => format!(":{alias}:"),
        }
    }

    pub fn key(&self) -> String {
        match self {
            Self::Stock(emoji) => emoji
                .shortcode()
                .map(str::to_string)
                .unwrap_or_else(|| normalize_alias(emoji.name())),
            Self::Custom { alias, .. } => alias.to_ascii_lowercase(),
        }
    }

    pub fn search_terms(&self) -> Vec<String> {
        match self {
            Self::Stock(emoji) => {
                let mut terms = vec![emoji.name().to_ascii_lowercase()];
                terms.extend(emoji.shortcodes().map(str::to_ascii_lowercase));
                terms
            }
            Self::Custom { alias, .. } => vec![alias.to_ascii_lowercase()],
        }
    }
}

pub fn selected_emoji_for_tone(emoji: &'static Emoji, tone: Option<SkinTone>) -> &'static Emoji {
    let target = tone.unwrap_or(SkinTone::Default);
    if target == SkinTone::Default {
        return emoji;
    }
    emoji
        .skin_tones()
        .and_then(|mut variants| variants.find(|variant| variant.skin_tone() == Some(target)))
        .unwrap_or(emoji)
}

pub fn search_emoji_items(
    query: &str,
    custom_items: &[EmojiPickerItem],
    max_results: usize,
) -> Vec<EmojiPickerItem> {
    let max_results = if max_results == 0 {
        DEFAULT_MAX_RESULTS
    } else {
        max_results
    };
    let parts = split_search_parts(query);
    if parts.is_empty() {
        return Vec::new();
    }

    let mut scored: Vec<(u32, EmojiPickerItem)> = Vec::new();

    for item in custom_items {
        let score = score_item(item, &parts);
        if score > 0 {
            scored.push((score, item.clone()));
        }
    }

    for emoji in emojis::iter() {
        let item = EmojiPickerItem::Stock(emoji);
        let score = score_item(&item, &parts);
        if score > 0 {
            scored.push((score, item));
        }
    }

    scored.sort_by_key(|(score, item)| (Reverse(*score), item.label()));
    scored.truncate(max_results);
    scored.into_iter().map(|(_, item)| item).collect()
}

pub fn recent_key_for_stock(emoji: &'static Emoji) -> String {
    emoji
        .shortcode()
        .map(str::to_string)
        .unwrap_or_else(|| normalize_alias(emoji.name()))
}

pub fn skin_tone_to_setting_value(tone: Option<SkinTone>) -> Option<String> {
    match tone {
        Some(SkinTone::Default) => Some("1F3FA".to_string()),
        Some(SkinTone::Light) => Some("1F3FB".to_string()),
        Some(SkinTone::MediumLight) => Some("1F3FC".to_string()),
        Some(SkinTone::Medium) => Some("1F3FD".to_string()),
        Some(SkinTone::MediumDark) => Some("1F3FE".to_string()),
        Some(SkinTone::Dark) => Some("1F3FF".to_string()),
        _ => None,
    }
}

pub fn skin_tone_from_setting_value(value: Option<&str>) -> Option<SkinTone> {
    match value.map(str::trim).unwrap_or_default() {
        "1F3FA" => Some(SkinTone::Default),
        "1F3FB" => Some(SkinTone::Light),
        "1F3FC" => Some(SkinTone::MediumLight),
        "1F3FD" => Some(SkinTone::Medium),
        "1F3FE" => Some(SkinTone::MediumDark),
        "1F3FF" => Some(SkinTone::Dark),
        _ => None,
    }
}

pub fn push_recent_alias(recents: &mut Vec<String>, alias: String, max_items: usize) {
    let normalized = alias.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return;
    }
    recents.retain(|entry| entry.to_ascii_lowercase() != normalized);
    recents.insert(0, normalized);
    if recents.len() > max_items {
        recents.truncate(max_items);
    }
}

fn score_item(item: &EmojiPickerItem, parts: &[String]) -> u32 {
    let mut score = 0u32;
    for term in item.search_terms() {
        for part in parts {
            if part.is_empty() {
                continue;
            }
            if let Some(index) = term.find(part) {
                score += if index == 0 { 3 } else { 1 };
            }
        }
    }
    score
}

fn split_search_parts(query: &str) -> Vec<String> {
    query
        .to_ascii_lowercase()
        .split(|ch: char| ch.is_whitespace() || ch == ',' || ch == '-' || ch == '_')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect()
}

fn normalize_alias(alias: &str) -> String {
    let mut out = String::with_capacity(alias.len());
    let mut last_was_sep = false;
    for ch in alias.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            last_was_sep = false;
            continue;
        }
        if !last_was_sep {
            out.push('_');
            last_was_sep = true;
        }
    }
    out.trim_matches('_').to_string()
}
