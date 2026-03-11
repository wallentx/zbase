use crate::views::selectable_text::{LinkRange, StyledRange};
use std::ops::Range;

#[derive(Clone, Copy, Debug, Default)]
pub struct InlineMarkdownConfig {
    pub spoiler_foreground: Option<u32>,
    pub spoiler_background: Option<u32>,
}

pub struct InlineMarkdownParseResult {
    pub text: String,
    pub link_ranges: Vec<LinkRange>,
    pub styled_ranges: Vec<StyledRange>,
    pub source_to_output_mapping: Vec<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MarkdownStyleKind {
    Bold,
    Italic,
    Strike,
    Spoiler,
}

#[derive(Clone, Debug)]
struct MarkdownRange {
    byte_range: Range<usize>,
    kind: MarkdownStyleKind,
}

struct ParseContext<'a> {
    source: &'a str,
    output: String,
    mapping: Vec<usize>,
    markdown_ranges: Vec<MarkdownRange>,
    protected_ranges: Vec<Range<usize>>,
}

pub fn apply_inline_markdown(
    source: &str,
    link_ranges: &[LinkRange],
    styled_ranges: &[StyledRange],
    config: InlineMarkdownConfig,
) -> InlineMarkdownParseResult {
    let mut context = ParseContext {
        source,
        output: String::with_capacity(source.len()),
        mapping: vec![0; source.len().saturating_add(1)],
        markdown_ranges: Vec::new(),
        protected_ranges: collect_protected_ranges(source.len(), link_ranges, styled_ranges),
    };

    parse_range(&mut context, 0, source.len());

    let remapped_links = remap_link_ranges(&context.mapping, source.len(), link_ranges);
    let mut remapped_styles = markdown_ranges_to_styled_ranges(&context.markdown_ranges, config);
    remapped_styles.extend(remap_styled_ranges(
        &context.mapping,
        source.len(),
        styled_ranges,
    ));

    InlineMarkdownParseResult {
        text: context.output,
        link_ranges: remapped_links,
        styled_ranges: remapped_styles,
        source_to_output_mapping: context.mapping,
    }
}

pub fn remap_source_byte_range(
    source_to_output_mapping: &[usize],
    source_len: usize,
    range: &Range<usize>,
) -> Option<Range<usize>> {
    remap_range(source_to_output_mapping, source_len, range)
}

fn parse_range(context: &mut ParseContext<'_>, start: usize, end: usize) {
    if start > end || start > context.source.len() {
        return;
    }
    let end = end.min(context.source.len());
    let mut index = start;
    context.mapping[index] = context.output.len();

    while index < end {
        if let Some(protected_end) = protected_end_at(index, end, &context.protected_ranges) {
            copy_literal(context, index, protected_end);
            index = protected_end;
            continue;
        }

        if let Some(next_index) = try_consume_escaped_char(context, index, end) {
            index = next_index;
            continue;
        }

        if let Some(code_end) = find_inline_code_end(context.source, index, end) {
            copy_literal(context, index, code_end);
            index = code_end;
            continue;
        }

        if let Some(style_match) = try_match_style(context.source, index, end) {
            map_skipped(context, index, style_match.inner_start);

            let styled_start = context.output.len();
            parse_range(context, style_match.inner_start, style_match.close_start);
            let styled_end = context.output.len();
            if styled_end > styled_start {
                context.markdown_ranges.push(MarkdownRange {
                    byte_range: styled_start..styled_end,
                    kind: style_match.kind,
                });
            }

            map_skipped(context, style_match.close_start, style_match.close_end);
            index = style_match.close_end;
            continue;
        }

        let Some(ch) = context.source[index..end].chars().next() else {
            break;
        };
        let next_index = index + ch.len_utf8();
        copy_literal(context, index, next_index);
        index = next_index;
    }

    context.mapping[end] = context.output.len();
}

#[derive(Clone, Copy, Debug)]
struct StyleMatch {
    kind: MarkdownStyleKind,
    inner_start: usize,
    close_start: usize,
    close_end: usize,
}

fn try_match_style(source: &str, index: usize, end: usize) -> Option<StyleMatch> {
    if index >= end || !source.is_char_boundary(index) {
        return None;
    }

    if source[index..end].starts_with("!>") {
        let inner_start = index + 2;
        let close_start = find_spoiler_close(source, inner_start, end)?;
        return Some(StyleMatch {
            kind: MarkdownStyleKind::Spoiler,
            inner_start,
            close_start,
            close_end: close_start + 2,
        });
    }

    let ch = source[index..end].chars().next()?;
    let kind = match ch {
        '*' => MarkdownStyleKind::Bold,
        '_' => MarkdownStyleKind::Italic,
        '~' => MarkdownStyleKind::Strike,
        _ => return None,
    };
    if !opener_prefix_is_valid(source, index) {
        return None;
    }

    let inner_start = index + ch.len_utf8();
    let close_start = find_emphasis_close(source, inner_start, end, ch)?;
    Some(StyleMatch {
        kind,
        inner_start,
        close_start,
        close_end: close_start + ch.len_utf8(),
    })
}

fn find_inline_code_end(source: &str, index: usize, end: usize) -> Option<usize> {
    if index >= end || !source[index..end].starts_with('`') {
        return None;
    }
    let inner_start = index + 1;
    if inner_start >= end {
        return None;
    }

    let mut cursor = inner_start;
    while cursor < end {
        let ch = source[cursor..end].chars().next()?;
        if ch == '`' && cursor > inner_start && !is_escaped(source, cursor) {
            return Some(cursor + 1);
        }
        cursor += ch.len_utf8();
    }
    None
}

fn find_spoiler_close(source: &str, start: usize, end: usize) -> Option<usize> {
    if start >= end {
        return None;
    }
    let mut cursor = start;
    while cursor < end {
        let ch = source[cursor..end].chars().next()?;
        if ch == '\n' {
            return None;
        }
        if ch == '\\' {
            cursor = advance_past_escaped(source, cursor, end);
            continue;
        }
        if source[cursor..end].starts_with("<!") {
            return Some(cursor);
        }
        cursor += ch.len_utf8();
    }
    None
}

fn find_emphasis_close(source: &str, start: usize, end: usize, delimiter: char) -> Option<usize> {
    if start >= end {
        return None;
    }
    let mut cursor = start;
    let mut saw_content = false;

    while cursor < end {
        let ch = source[cursor..end].chars().next()?;
        if ch == '\n' {
            return None;
        }
        if ch == '\\' {
            if cursor + 1 >= end {
                return None;
            }
            cursor = advance_past_escaped(source, cursor, end);
            saw_content = true;
            continue;
        }
        if ch == delimiter && saw_content {
            let next_index = cursor + ch.len_utf8();
            let next_same = source[next_index..end]
                .chars()
                .next()
                .map(|value| value == delimiter)
                .unwrap_or(false);
            if !next_same && !is_escaped(source, cursor) {
                return Some(cursor);
            }
        }
        saw_content = true;
        cursor += ch.len_utf8();
    }
    None
}

fn opener_prefix_is_valid(source: &str, index: usize) -> bool {
    if index == 0 {
        return true;
    }
    source[..index]
        .chars()
        .next_back()
        .map(|ch| !is_word_char(ch))
        .unwrap_or(true)
}

fn is_word_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn is_escaped(source: &str, marker_start: usize) -> bool {
    let bytes = source.as_bytes();
    let mut slash_count = 0usize;
    let mut cursor = marker_start;
    while cursor > 0 && bytes[cursor - 1] == b'\\' {
        slash_count += 1;
        cursor -= 1;
    }
    slash_count % 2 == 1
}

fn try_consume_escaped_char(
    context: &mut ParseContext<'_>,
    index: usize,
    end: usize,
) -> Option<usize> {
    if index >= end || !context.source[index..end].starts_with('\\') {
        return None;
    }
    let slash_end = index + 1;
    let next = context.source[slash_end..end].chars().next()?;
    if !is_markdown_escapable(next) {
        return None;
    }
    let next_end = slash_end + next.len_utf8();
    map_skipped(context, index, slash_end);
    copy_literal(context, slash_end, next_end);
    Some(next_end)
}

fn is_markdown_escapable(ch: char) -> bool {
    matches!(ch, '\\' | '*' | '_' | '~' | '!' | '<' | '>' | '`')
}

fn advance_past_escaped(source: &str, slash_start: usize, end: usize) -> usize {
    let slash_end = slash_start + 1;
    if slash_end >= end {
        return slash_end;
    }
    let Some(next) = source[slash_end..end].chars().next() else {
        return slash_end;
    };
    slash_end + next.len_utf8()
}

fn copy_literal(context: &mut ParseContext<'_>, start: usize, end: usize) {
    if start >= end {
        return;
    }
    let start = start.min(context.source.len());
    let end = end.min(context.source.len());
    if start >= end {
        return;
    }

    let output_start = context.output.len();
    context.output.push_str(&context.source[start..end]);
    for boundary in (start + 1)..=end {
        context.mapping[boundary] = output_start + (boundary - start);
    }
}

fn map_skipped(context: &mut ParseContext<'_>, start: usize, end: usize) {
    if start >= end {
        return;
    }
    let mapped_index = context.output.len();
    let end = end.min(context.source.len());
    for boundary in (start + 1)..=end {
        context.mapping[boundary] = mapped_index;
    }
}

fn collect_protected_ranges(
    text_len: usize,
    link_ranges: &[LinkRange],
    styled_ranges: &[StyledRange],
) -> Vec<Range<usize>> {
    let mut ranges = Vec::with_capacity(link_ranges.len() + styled_ranges.len());
    for range in link_ranges
        .iter()
        .map(|range| &range.byte_range)
        .chain(styled_ranges.iter().map(|range| &range.byte_range))
    {
        let start = range.start.min(text_len);
        let end = range.end.min(text_len);
        if start < end {
            ranges.push(start..end);
        }
    }
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<Range<usize>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(last) = merged.last_mut()
            && range.start <= last.end
        {
            last.end = last.end.max(range.end);
            continue;
        }
        merged.push(range);
    }
    merged
}

fn protected_end_at(index: usize, end: usize, ranges: &[Range<usize>]) -> Option<usize> {
    ranges
        .iter()
        .find(|range| index >= range.start && index < range.end)
        .map(|range| range.end.min(end))
}

fn remap_link_ranges(mapping: &[usize], text_len: usize, ranges: &[LinkRange]) -> Vec<LinkRange> {
    ranges
        .iter()
        .filter_map(|range| {
            remap_range(mapping, text_len, &range.byte_range).map(|byte_range| LinkRange {
                byte_range,
                url: range.url.clone(),
            })
        })
        .collect()
}

fn remap_styled_ranges(
    mapping: &[usize],
    text_len: usize,
    ranges: &[StyledRange],
) -> Vec<StyledRange> {
    ranges
        .iter()
        .filter_map(|range| {
            remap_range(mapping, text_len, &range.byte_range).map(|byte_range| StyledRange {
                byte_range,
                color: range.color,
                background_color: range.background_color,
                bold: range.bold,
                italic: range.italic,
                strikethrough: range.strikethrough,
            })
        })
        .collect()
}

fn remap_range(mapping: &[usize], text_len: usize, range: &Range<usize>) -> Option<Range<usize>> {
    if mapping.is_empty() {
        return None;
    }
    let start = range.start.min(text_len);
    let end = range.end.min(text_len);
    if start >= end {
        return None;
    }
    let mapped_start = *mapping.get(start)?;
    let mapped_end = *mapping.get(end)?;
    (mapped_start < mapped_end).then_some(mapped_start..mapped_end)
}

fn markdown_ranges_to_styled_ranges(
    ranges: &[MarkdownRange],
    config: InlineMarkdownConfig,
) -> Vec<StyledRange> {
    ranges
        .iter()
        .map(|range| match range.kind {
            MarkdownStyleKind::Bold => StyledRange {
                byte_range: range.byte_range.clone(),
                color: None,
                background_color: None,
                bold: true,
                italic: false,
                strikethrough: false,
            },
            MarkdownStyleKind::Italic => StyledRange {
                byte_range: range.byte_range.clone(),
                color: None,
                background_color: None,
                bold: false,
                italic: true,
                strikethrough: false,
            },
            MarkdownStyleKind::Strike => StyledRange {
                byte_range: range.byte_range.clone(),
                color: None,
                background_color: None,
                bold: false,
                italic: false,
                strikethrough: true,
            },
            MarkdownStyleKind::Spoiler => StyledRange {
                byte_range: range.byte_range.clone(),
                color: config.spoiler_foreground,
                background_color: config.spoiler_background,
                bold: false,
                italic: false,
                strikethrough: false,
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_styles_and_removes_delimiters() {
        let parsed = apply_inline_markdown(
            "a *bold* _italic_ ~strike~ !>spoiler<!",
            &[],
            &[],
            InlineMarkdownConfig {
                spoiler_foreground: Some(0x111111),
                spoiler_background: Some(0xeeeeee),
            },
        );
        assert_eq!(parsed.text, "a bold italic strike spoiler");
        assert!(parsed.styled_ranges.iter().any(|range| range.bold));
        assert!(parsed.styled_ranges.iter().any(|range| range.italic));
        assert!(parsed.styled_ranges.iter().any(|range| range.strikethrough));
        assert!(parsed.styled_ranges.iter().any(|range| {
            range.background_color == Some(0xeeeeee) && range.color == Some(0x111111)
        }));
    }

    #[test]
    fn preserves_escaped_delimiters() {
        let parsed = apply_inline_markdown(
            r"\*not bold\* and \_not italic\_",
            &[],
            &[],
            InlineMarkdownConfig::default(),
        );
        assert_eq!(parsed.text, "*not bold* and _not italic_");
        assert!(parsed.styled_ranges.is_empty());
    }

    #[test]
    fn requires_boundary_before_delimiter() {
        let parsed = apply_inline_markdown(
            "foo*bar* baz *qux*",
            &[],
            &[],
            InlineMarkdownConfig::default(),
        );
        assert_eq!(parsed.text, "foo*bar* baz qux");
        let bold_ranges = parsed
            .styled_ranges
            .iter()
            .filter(|range| range.bold)
            .collect::<Vec<_>>();
        assert_eq!(bold_ranges.len(), 1);
        assert_eq!(&parsed.text[bold_ranges[0].byte_range.clone()], "qux");
    }

    #[test]
    fn remaps_link_ranges_after_delimiter_removal() {
        let source = "*go* https://example.com";
        let start = source.find("https://").expect("url start exists");
        let end = source.len();
        let parsed = apply_inline_markdown(
            source,
            &[LinkRange {
                byte_range: start..end,
                url: "https://example.com".to_string(),
            }],
            &[],
            InlineMarkdownConfig::default(),
        );
        assert_eq!(parsed.text, "go https://example.com");
        assert_eq!(parsed.link_ranges.len(), 1);
        assert_eq!(
            &parsed.text[parsed.link_ranges[0].byte_range.clone()],
            "https://example.com"
        );
    }

    #[test]
    fn protects_existing_styled_ranges_from_markdown() {
        let source = "@user_name";
        let parsed = apply_inline_markdown(
            source,
            &[],
            &[StyledRange {
                byte_range: 0..source.len(),
                color: Some(0x123456),
                background_color: Some(0x654321),
                bold: true,
                italic: false,
                strikethrough: false,
            }],
            InlineMarkdownConfig::default(),
        );
        assert_eq!(parsed.text, source);
        assert_eq!(parsed.styled_ranges.len(), 1);
        assert_eq!(parsed.styled_ranges[0].byte_range, 0..source.len());
    }

    #[test]
    fn skips_markdown_inside_inline_code_spans() {
        let parsed = apply_inline_markdown(
            "`*literal*` and *bold*",
            &[],
            &[],
            InlineMarkdownConfig::default(),
        );
        assert_eq!(parsed.text, "`*literal*` and bold");
        let bold_spans = parsed
            .styled_ranges
            .iter()
            .filter(|range| range.bold)
            .collect::<Vec<_>>();
        assert_eq!(bold_spans.len(), 1);
        assert_eq!(&parsed.text[bold_spans[0].byte_range.clone()], "bold");
    }
}
