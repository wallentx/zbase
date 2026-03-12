#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AutocompleteTriggerKind {
    Mention,
    Emoji,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TriggerMatch {
    pub kind: AutocompleteTriggerKind,
    pub trigger_offset: usize,
    pub query: String,
}

const MIN_EMOJI_QUERY_LEN: usize = 2;

pub fn detect_autocomplete_trigger(text: &str, cursor: usize) -> Option<TriggerMatch> {
    let cursor = cursor.min(text.len());
    if cursor_is_in_code(text, cursor) {
        return None;
    }
    let before_cursor = &text[..cursor];
    for (offset, ch) in before_cursor.char_indices().rev() {
        if ch.is_whitespace() {
            return None;
        }
        if ch != '@' && ch != ':' {
            continue;
        }
        if offset > 0 {
            let prev = text[..offset].chars().next_back();
            if prev.is_some_and(|prev| !prev.is_whitespace()) {
                return None;
            }
        }
        let query = text[offset + ch.len_utf8()..cursor].to_string();
        if ch == '@' {
            return Some(TriggerMatch {
                kind: AutocompleteTriggerKind::Mention,
                trigger_offset: offset,
                query,
            });
        }
        if query.len() < MIN_EMOJI_QUERY_LEN || !emoji_query_valid(&query) {
            return None;
        }
        return Some(TriggerMatch {
            kind: AutocompleteTriggerKind::Emoji,
            trigger_offset: offset,
            query,
        });
    }
    None
}

pub fn cursor_is_in_code(text: &str, cursor: usize) -> bool {
    let cursor = cursor.min(text.len());
    let before_cursor = &text[..cursor];
    if in_fenced_code_block(before_cursor) {
        return true;
    }
    let line_start = before_cursor.rfind('\n').map_or(0, |index| index + 1);
    let current_line = &before_cursor[line_start..];
    in_inline_code(current_line)
}

fn in_fenced_code_block(before_cursor: &str) -> bool {
    let mut in_fence = false;
    for line in before_cursor.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") {
            in_fence = !in_fence;
        }
    }
    in_fence
}

fn in_inline_code(current_line: &str) -> bool {
    let mut in_code = false;
    let mut index = 0usize;
    let bytes = current_line.as_bytes();
    while index < bytes.len() {
        if bytes[index] != b'`' {
            index += 1;
            continue;
        }
        while index < bytes.len() && bytes[index] == b'`' {
            index += 1;
        }
        in_code = !in_code;
    }
    in_code
}

fn emoji_query_valid(query: &str) -> bool {
    query
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '+' || ch == '-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_mention_trigger() {
        let matched = detect_autocomplete_trigger("hello @cam", "hello @cam".len());
        assert_eq!(
            matched,
            Some(TriggerMatch {
                kind: AutocompleteTriggerKind::Mention,
                trigger_offset: 6,
                query: "cam".to_string()
            })
        );
    }

    #[test]
    fn detects_emoji_trigger_with_min_length() {
        assert!(detect_autocomplete_trigger(":s", 2).is_none());
        let matched = detect_autocomplete_trigger(":sm", 3);
        assert_eq!(
            matched,
            Some(TriggerMatch {
                kind: AutocompleteTriggerKind::Emoji,
                trigger_offset: 0,
                query: "sm".to_string()
            })
        );
    }

    #[test]
    fn suppresses_inside_inline_code() {
        let text = "typing `@cam` now";
        let cursor = text.find("@cam").unwrap_or_default() + 4;
        assert!(cursor_is_in_code(text, cursor));
        assert!(detect_autocomplete_trigger(text, cursor).is_none());
    }

    #[test]
    fn suppresses_inside_fenced_code_block() {
        let text = "```rust\nlet a = @cam;\n";
        assert!(cursor_is_in_code(text, text.len()));
        assert!(detect_autocomplete_trigger(text, text.len()).is_none());
    }
}
