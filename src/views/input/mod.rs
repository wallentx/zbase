use gpui::{
    App, Bounds, ClipboardItem, Context, CursorStyle, Element, ElementId, ElementInputHandler,
    Entity, EntityInputHandler, FocusHandle, Focusable, GlobalElementId, LayoutId, MouseButton,
    MouseDownEvent, MouseMoveEvent, MouseUpEvent, PaintQuad, Pixels, Point, Style, TextRun,
    UTF16Selection, UnderlineStyle, Window, WrappedLine, actions, div, fill, point, prelude::*, px,
    relative, rgb, rgba, size,
};
use std::ops::Range;
use unicode_segmentation::UnicodeSegmentation;

use crate::views::{accent, selection};

actions!(
    text_field,
    [
        Backspace,
        Delete,
        Left,
        Right,
        Up,
        Down,
        SelectLeft,
        SelectRight,
        SelectAll,
        Home,
        End,
        InsertNewline,
        ShowCharacterPalette,
        Paste,
        Cut,
        Copy,
    ]
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextFieldKind {
    SingleLine,
    SingleLineWithContext { key_context: &'static str },
    Multiline { line_count: usize },
}

impl TextFieldKind {
    fn is_multiline(self) -> bool {
        matches!(self, Self::Multiline { .. })
    }

    fn preferred_height(self, line_height: Pixels) -> Pixels {
        match self {
            Self::SingleLine | Self::SingleLineWithContext { .. } => line_height,
            Self::Multiline { line_count } => line_height * line_count as f32,
        }
    }

    fn key_context(self) -> &'static str {
        match self {
            Self::SingleLine => "TextField",
            Self::SingleLineWithContext { key_context } => key_context,
            Self::Multiline { .. } => "MultilineTextField",
        }
    }
}

pub struct TextField {
    focus_handle: FocusHandle,
    content: String,
    placeholder: String,
    selected_range: Range<usize>,
    selection_reversed: bool,
    marked_range: Option<Range<usize>>,
    last_layout: Option<TextLayoutSnapshot>,
    last_bounds: Option<Bounds<Pixels>>,
    is_selecting: bool,
    field_kind: TextFieldKind,
    scroll_y: Pixels,
}

impl TextField {
    pub fn new(
        focus_handle: FocusHandle,
        placeholder: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self::with_kind(
            focus_handle,
            placeholder,
            content,
            TextFieldKind::SingleLine,
        )
    }

    pub fn new_with_key_context(
        focus_handle: FocusHandle,
        placeholder: impl Into<String>,
        content: impl Into<String>,
        key_context: &'static str,
    ) -> Self {
        Self::with_kind(
            focus_handle,
            placeholder,
            content,
            TextFieldKind::SingleLineWithContext { key_context },
        )
    }

    pub fn new_multiline(
        focus_handle: FocusHandle,
        placeholder: impl Into<String>,
        content: impl Into<String>,
        line_count: usize,
    ) -> Self {
        Self::with_kind(
            focus_handle,
            placeholder,
            content,
            TextFieldKind::Multiline {
                line_count: line_count.max(2),
            },
        )
    }

    fn with_kind(
        focus_handle: FocusHandle,
        placeholder: impl Into<String>,
        content: impl Into<String>,
        field_kind: TextFieldKind,
    ) -> Self {
        let content = content.into();
        let cursor = content.len();

        Self {
            focus_handle,
            content,
            placeholder: placeholder.into(),
            selected_range: cursor..cursor,
            selection_reversed: false,
            marked_range: None,
            last_layout: None,
            last_bounds: None,
            is_selecting: false,
            field_kind,
            scroll_y: px(0.),
        }
    }

    pub fn text(&self) -> String {
        self.content.clone()
    }

    pub fn is_multiline(&self) -> bool {
        self.field_kind.is_multiline()
    }

    pub fn set_text(&mut self, text: impl Into<String>, cx: &mut Context<Self>) {
        let text = text.into();
        if self.content == text {
            return;
        }

        let cursor = text.len();
        self.content = text;
        self.selected_range = cursor..cursor;
        self.selection_reversed = false;
        self.marked_range = None;
        self.scroll_y = px(0.);
        self.clear_layout_cache();
        cx.notify();
    }

    pub fn insert_text(&mut self, text: &str, cx: &mut Context<Self>) {
        let text = if self.is_multiline() {
            text.to_string()
        } else {
            text.replace('\n', " ")
        };

        let range = self
            .marked_range
            .clone()
            .unwrap_or(self.selected_range.clone());
        self.content = self.content[0..range.start].to_owned() + &text + &self.content[range.end..];
        let cursor = range.start + text.len();
        self.selected_range = cursor..cursor;
        self.selection_reversed = false;
        self.marked_range = None;
        self.clear_layout_cache();
        cx.notify();
    }

    fn clear_layout_cache(&mut self) {
        self.last_layout = None;
        self.last_bounds = None;
    }

    fn left(&mut self, _: &Left, _: &mut Window, cx: &mut Context<Self>) {
        if self.selected_range.is_empty() {
            self.move_to(self.previous_boundary(self.cursor_offset()), cx);
        } else {
            self.move_to(self.selected_range.start, cx);
        }
    }

    fn right(&mut self, _: &Right, _: &mut Window, cx: &mut Context<Self>) {
        if self.selected_range.is_empty() {
            self.move_to(self.next_boundary(self.selected_range.end), cx);
        } else {
            self.move_to(self.selected_range.end, cx);
        }
    }

    fn up(&mut self, _: &Up, _: &mut Window, cx: &mut Context<Self>) {
        self.move_vertical(-1.0, cx);
    }

    fn down(&mut self, _: &Down, _: &mut Window, cx: &mut Context<Self>) {
        self.move_vertical(1.0, cx);
    }

    fn select_left(&mut self, _: &SelectLeft, _: &mut Window, cx: &mut Context<Self>) {
        self.select_to(self.previous_boundary(self.cursor_offset()), cx);
    }

    fn select_right(&mut self, _: &SelectRight, _: &mut Window, cx: &mut Context<Self>) {
        self.select_to(self.next_boundary(self.cursor_offset()), cx);
    }

    fn select_all(&mut self, _: &SelectAll, _: &mut Window, cx: &mut Context<Self>) {
        self.move_to(0, cx);
        self.select_to(self.content.len(), cx);
    }

    fn home(&mut self, _: &Home, _: &mut Window, cx: &mut Context<Self>) {
        self.move_to(0, cx);
    }

    fn end(&mut self, _: &End, _: &mut Window, cx: &mut Context<Self>) {
        self.move_to(self.content.len(), cx);
    }

    fn insert_newline(&mut self, _: &InsertNewline, window: &mut Window, cx: &mut Context<Self>) {
        if self.is_multiline() {
            self.replace_text_in_range(None, "\n", window, cx);
        }
    }

    fn backspace(&mut self, _: &Backspace, window: &mut Window, cx: &mut Context<Self>) {
        if self.selected_range.is_empty() {
            self.select_to(self.previous_boundary(self.cursor_offset()), cx);
        }
        self.replace_text_in_range(None, "", window, cx);
    }

    fn delete(&mut self, _: &Delete, window: &mut Window, cx: &mut Context<Self>) {
        if self.selected_range.is_empty() {
            self.select_to(self.next_boundary(self.cursor_offset()), cx);
        }
        self.replace_text_in_range(None, "", window, cx);
    }

    fn on_mouse_down(
        &mut self,
        event: &MouseDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.is_selecting = true;
        window.focus(&self.focus_handle(cx));

        if event.modifiers.shift {
            self.select_to(self.index_for_mouse_position(event.position), cx);
        } else {
            self.move_to(self.index_for_mouse_position(event.position), cx);
        }
    }

    fn on_mouse_up(&mut self, _: &MouseUpEvent, _: &mut Window, _: &mut Context<Self>) {
        self.is_selecting = false;
    }

    fn on_mouse_move(&mut self, event: &MouseMoveEvent, _: &mut Window, cx: &mut Context<Self>) {
        if self.is_selecting {
            self.select_to(self.index_for_mouse_position(event.position), cx);
        }
    }

    fn show_character_palette(
        &mut self,
        _: &ShowCharacterPalette,
        window: &mut Window,
        _: &mut Context<Self>,
    ) {
        window.show_character_palette();
    }

    fn paste(&mut self, _: &Paste, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(text) = cx.read_from_clipboard().and_then(|item| item.text()) {
            let normalized = if self.is_multiline() {
                text
            } else {
                text.replace('\n', " ")
            };
            self.replace_text_in_range(None, &normalized, window, cx);
        }
    }

    fn copy(&mut self, _: &Copy, _: &mut Window, cx: &mut Context<Self>) {
        if !self.selected_range.is_empty() {
            cx.write_to_clipboard(ClipboardItem::new_string(
                self.content[self.selected_range.clone()].to_string(),
            ));
        }
    }

    fn cut(&mut self, _: &Cut, window: &mut Window, cx: &mut Context<Self>) {
        if !self.selected_range.is_empty() {
            cx.write_to_clipboard(ClipboardItem::new_string(
                self.content[self.selected_range.clone()].to_string(),
            ));
            self.replace_text_in_range(None, "", window, cx);
        }
    }

    fn move_to(&mut self, offset: usize, cx: &mut Context<Self>) {
        self.selected_range = offset..offset;
        self.selection_reversed = false;
        cx.notify();
    }

    fn move_vertical(&mut self, direction: f32, cx: &mut Context<Self>) {
        if !self.is_multiline() {
            return;
        }

        let Some(layout) = self.last_layout.as_ref() else {
            return;
        };
        let cursor = self.cursor_offset();
        let Some(position) = layout.position_for_index(cursor) else {
            return;
        };
        let next = layout.closest_index_for_position(point(
            position.x,
            (position.y + layout.line_height * direction).max(px(0.)),
        ));
        self.move_to(next, cx);
    }

    fn cursor_offset(&self) -> usize {
        if self.selection_reversed {
            self.selected_range.start
        } else {
            self.selected_range.end
        }
    }

    fn index_for_mouse_position(&self, position: Point<Pixels>) -> usize {
        if self.content.is_empty() {
            return 0;
        }

        let (Some(bounds), Some(layout)) = (self.last_bounds.as_ref(), self.last_layout.as_ref())
        else {
            return 0;
        };

        let Some(local_position) = bounds.localize(&position) else {
            return 0;
        };

        layout.closest_index_for_position(point(local_position.x, local_position.y + self.scroll_y))
    }

    fn select_to(&mut self, offset: usize, cx: &mut Context<Self>) {
        if self.selection_reversed {
            self.selected_range.start = offset;
        } else {
            self.selected_range.end = offset;
        }

        if self.selected_range.end < self.selected_range.start {
            self.selection_reversed = !self.selection_reversed;
            self.selected_range = self.selected_range.end..self.selected_range.start;
        }

        cx.notify();
    }

    fn previous_boundary(&self, offset: usize) -> usize {
        self.content
            .grapheme_indices(true)
            .rev()
            .find_map(|(idx, _)| (idx < offset).then_some(idx))
            .unwrap_or(0)
    }

    fn next_boundary(&self, offset: usize) -> usize {
        self.content
            .grapheme_indices(true)
            .find_map(|(idx, _)| (idx > offset).then_some(idx))
            .unwrap_or(self.content.len())
    }

    fn offset_from_utf16(&self, offset: usize) -> usize {
        let mut utf8_offset = 0;
        let mut utf16_count = 0;

        for ch in self.content.chars() {
            if utf16_count >= offset {
                break;
            }
            utf16_count += ch.len_utf16();
            utf8_offset += ch.len_utf8();
        }

        utf8_offset
    }

    fn offset_to_utf16(&self, offset: usize) -> usize {
        let mut utf16_offset = 0;
        let mut utf8_count = 0;

        for ch in self.content.chars() {
            if utf8_count >= offset {
                break;
            }
            utf8_count += ch.len_utf8();
            utf16_offset += ch.len_utf16();
        }

        utf16_offset
    }

    fn range_to_utf16(&self, range: &Range<usize>) -> Range<usize> {
        self.offset_to_utf16(range.start)..self.offset_to_utf16(range.end)
    }

    fn range_from_utf16(&self, range_utf16: &Range<usize>) -> Range<usize> {
        self.offset_from_utf16(range_utf16.start)..self.offset_from_utf16(range_utf16.end)
    }
}

impl EntityInputHandler for TextField {
    fn text_for_range(
        &mut self,
        range_utf16: Range<usize>,
        actual_range: &mut Option<Range<usize>>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> Option<String> {
        let range = self.range_from_utf16(&range_utf16);
        actual_range.replace(self.range_to_utf16(&range));
        Some(self.content[range].to_string())
    }

    fn selected_text_range(
        &mut self,
        _ignore_disabled_input: bool,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> Option<UTF16Selection> {
        Some(UTF16Selection {
            range: self.range_to_utf16(&self.selected_range),
            reversed: self.selection_reversed,
        })
    }

    fn marked_text_range(
        &self,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> Option<Range<usize>> {
        self.marked_range
            .as_ref()
            .map(|range| self.range_to_utf16(range))
    }

    fn unmark_text(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.marked_range = None;
    }

    fn replace_text_in_range(
        &mut self,
        range_utf16: Option<Range<usize>>,
        new_text: &str,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let replacement = if self.is_multiline() {
            new_text.to_string()
        } else {
            new_text.replace('\n', " ")
        };
        let range = range_utf16
            .as_ref()
            .map(|range_utf16| self.range_from_utf16(range_utf16))
            .or(self.marked_range.clone())
            .unwrap_or(self.selected_range.clone());

        self.content =
            self.content[0..range.start].to_owned() + &replacement + &self.content[range.end..];
        let cursor = range.start + replacement.len();
        self.selected_range = cursor..cursor;
        self.selection_reversed = false;
        self.marked_range.take();
        self.clear_layout_cache();
        cx.notify();
    }

    fn replace_and_mark_text_in_range(
        &mut self,
        range_utf16: Option<Range<usize>>,
        new_text: &str,
        new_selected_range_utf16: Option<Range<usize>>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let replacement = if self.is_multiline() {
            new_text.to_string()
        } else {
            new_text.replace('\n', " ")
        };
        let range = range_utf16
            .as_ref()
            .map(|range_utf16| self.range_from_utf16(range_utf16))
            .or(self.marked_range.clone())
            .unwrap_or(self.selected_range.clone());

        self.content =
            self.content[0..range.start].to_owned() + &replacement + &self.content[range.end..];
        if !replacement.is_empty() {
            self.marked_range = Some(range.start..range.start + replacement.len());
        } else {
            self.marked_range = None;
        }
        self.selected_range = new_selected_range_utf16
            .as_ref()
            .map(|range_utf16| self.range_from_utf16(range_utf16))
            .map(|new_range| range.start + new_range.start..range.start + new_range.end)
            .unwrap_or_else(|| range.start + replacement.len()..range.start + replacement.len());
        self.clear_layout_cache();
        cx.notify();
    }

    fn bounds_for_range(
        &mut self,
        range_utf16: Range<usize>,
        bounds: Bounds<Pixels>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> Option<Bounds<Pixels>> {
        let range = self.range_from_utf16(&range_utf16);
        self.last_layout.as_ref().and_then(|layout| {
            layout.bounds_for_range(
                range,
                point(bounds.origin.x, bounds.origin.y - self.scroll_y),
            )
        })
    }

    fn character_index_for_point(
        &mut self,
        position: Point<Pixels>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> Option<usize> {
        let local_point = self.last_bounds?.localize(&position)?;
        let local_point = point(local_point.x, local_point.y + self.scroll_y);
        let index = self
            .last_layout
            .as_ref()?
            .closest_index_for_position(local_point);
        Some(self.offset_to_utf16(index))
    }
}

struct TextLineElement {
    input: Entity<TextField>,
}

struct PrepaintState {
    layout: TextLayoutSnapshot,
    cursor: Option<PaintQuad>,
    selection: Vec<PaintQuad>,
    scroll_y: Pixels,
}

impl IntoElement for TextLineElement {
    type Element = Self;

    fn into_element(self) -> Self::Element {
        self
    }
}

impl Element for TextLineElement {
    type RequestLayoutState = ();
    type PrepaintState = PrepaintState;

    fn id(&self) -> Option<ElementId> {
        None
    }

    fn source_location(&self) -> Option<&'static core::panic::Location<'static>> {
        None
    }

    fn request_layout(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&gpui::InspectorElementId>,
        window: &mut Window,
        cx: &mut App,
    ) -> (LayoutId, Self::RequestLayoutState) {
        let mut style = Style::default();
        style.size.width = relative(1.).into();
        let field_kind = self.input.read(cx).field_kind;
        style.size.height = field_kind.preferred_height(window.line_height()).into();
        (window.request_layout(style, [], cx), ())
    }

    fn prepaint(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&gpui::InspectorElementId>,
        bounds: Bounds<Pixels>,
        _request_layout: &mut Self::RequestLayoutState,
        window: &mut Window,
        cx: &mut App,
    ) -> Self::PrepaintState {
        let input = self.input.read(cx);
        let is_focused = input.focus_handle.is_focused(window);
        let content = input.content.clone();
        let selected_range = input.selected_range.clone();
        let cursor = input.cursor_offset();
        let style = window.text_style();

        let (display_text, text_color, is_placeholder) = if content.is_empty() {
            (input.placeholder.clone(), style.color.opacity(0.42), true)
        } else {
            (content.clone(), style.color, false)
        };

        let run = TextRun {
            len: display_text.len(),
            font: style.font(),
            color: text_color,
            background_color: None,
            underline: None,
            strikethrough: None,
        };
        let runs = if let Some(marked_range) = input.marked_range.as_ref() {
            vec![
                TextRun {
                    len: marked_range.start,
                    ..run.clone()
                },
                TextRun {
                    len: marked_range.end - marked_range.start,
                    underline: Some(UnderlineStyle {
                        color: Some(run.color),
                        thickness: px(1.0),
                        wavy: false,
                    }),
                    ..run.clone()
                },
                TextRun {
                    len: display_text.len() - marked_range.end,
                    ..run
                },
            ]
            .into_iter()
            .filter(|run| run.len > 0)
            .collect()
        } else {
            vec![run]
        };

        let font_size = style.font_size.to_pixels(window.rem_size());
        let wrap_width = input
            .is_multiline()
            .then_some(bounds.size.width.max(px(1.0)));
        let lines = window
            .text_system()
            .shape_text(display_text.into(), font_size, &runs, wrap_width, None)
            .map(|lines| lines.into_vec())
            .unwrap_or_default();

        let layout = TextLayoutSnapshot {
            lines,
            line_height: window.line_height(),
            is_placeholder,
            text_len: content.len(),
        };
        let scroll_y = if input.is_multiline() {
            visible_scroll_offset(&layout, input.scroll_y, bounds.size.height, cursor)
        } else {
            px(0.)
        };
        let paint_origin = point(bounds.origin.x, bounds.origin.y - scroll_y);

        let selection = if !is_focused || selected_range.is_empty() || layout.is_placeholder {
            Vec::new()
        } else {
            layout.selection_quads(selected_range, paint_origin)
        };

        let cursor = if is_focused && selection.is_empty() {
            let cursor_pos = layout.position_for_index(cursor).unwrap_or_default();
            Some(fill(
                Bounds::new(
                    point(
                        bounds.left() + cursor_pos.x,
                        bounds.top() + cursor_pos.y - scroll_y,
                    ),
                    size(px(2.), layout.line_height),
                ),
                rgb(accent()),
            ))
        } else {
            None
        };

        PrepaintState {
            layout,
            cursor,
            selection,
            scroll_y,
        }
    }

    fn paint(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&gpui::InspectorElementId>,
        bounds: Bounds<Pixels>,
        _request_layout: &mut Self::RequestLayoutState,
        prepaint: &mut Self::PrepaintState,
        window: &mut Window,
        cx: &mut App,
    ) {
        let focus_handle = self.input.read(cx).focus_handle.clone();
        window.handle_input(
            &focus_handle,
            ElementInputHandler::new(bounds, self.input.clone()),
            cx,
        );

        for quad in prepaint.selection.drain(..) {
            window.paint_quad(quad);
        }

        let mut line_origin = point(bounds.origin.x, bounds.origin.y - prepaint.scroll_y);
        let align = window.text_style().text_align;
        for line in &prepaint.layout.lines {
            let _ = line.paint(
                line_origin,
                prepaint.layout.line_height,
                align,
                Some(bounds),
                window,
                cx,
            );
            line_origin.y += line.size(prepaint.layout.line_height).height;
        }

        if focus_handle.is_focused(window)
            && let Some(cursor) = prepaint.cursor.take()
        {
            window.paint_quad(cursor);
        }

        let layout = std::mem::take(&mut prepaint.layout);
        let scroll_y = prepaint.scroll_y;
        self.input.update(cx, |input, _cx| {
            input.last_layout = Some(layout);
            input.last_bounds = Some(bounds);
            input.scroll_y = scroll_y;
        });
    }
}

impl Render for TextField {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .w_full()
            .overflow_hidden()
            .key_context(self.field_kind.key_context())
            .track_focus(&self.focus_handle(cx))
            .cursor(CursorStyle::IBeam)
            .on_action(cx.listener(Self::backspace))
            .on_action(cx.listener(Self::delete))
            .on_action(cx.listener(Self::left))
            .on_action(cx.listener(Self::right))
            .on_action(cx.listener(Self::up))
            .on_action(cx.listener(Self::down))
            .on_action(cx.listener(Self::select_left))
            .on_action(cx.listener(Self::select_right))
            .on_action(cx.listener(Self::select_all))
            .on_action(cx.listener(Self::home))
            .on_action(cx.listener(Self::end))
            .on_action(cx.listener(Self::insert_newline))
            .on_action(cx.listener(Self::show_character_palette))
            .on_action(cx.listener(Self::paste))
            .on_action(cx.listener(Self::cut))
            .on_action(cx.listener(Self::copy))
            .on_mouse_down(MouseButton::Left, cx.listener(Self::on_mouse_down))
            .on_mouse_up(MouseButton::Left, cx.listener(Self::on_mouse_up))
            .on_mouse_up_out(MouseButton::Left, cx.listener(Self::on_mouse_up))
            .on_mouse_move(cx.listener(Self::on_mouse_move))
            .child(TextLineElement { input: cx.entity() })
    }
}

impl Focusable for TextField {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

struct TextLayoutSnapshot {
    lines: Vec<WrappedLine>,
    line_height: Pixels,
    is_placeholder: bool,
    text_len: usize,
}

impl Default for TextLayoutSnapshot {
    fn default() -> Self {
        Self {
            lines: Vec::new(),
            line_height: px(0.),
            is_placeholder: false,
            text_len: 0,
        }
    }
}

impl TextLayoutSnapshot {
    fn total_height(&self) -> Pixels {
        self.lines.iter().fold(px(0.), |height, line| {
            height + line.size(self.line_height).height
        })
    }

    fn closest_index_for_position(&self, position: Point<Pixels>) -> usize {
        if self.is_placeholder {
            return 0;
        }

        if position.y < px(0.) {
            return 0;
        }

        let mut line_origin_y = px(0.);
        let mut line_start_ix = 0;

        for line in &self.lines {
            let line_bottom = line_origin_y + line.size(self.line_height).height;
            if position.y > line_bottom {
                line_origin_y = line_bottom;
                line_start_ix += line.len() + 1;
                continue;
            }

            let local_position = point(position.x, position.y - line_origin_y);
            let local_index =
                match line.closest_index_for_position(local_position, self.line_height) {
                    Ok(index) | Err(index) => index,
                };
            return (line_start_ix + local_index).min(self.text_len);
        }

        self.text_len
    }

    fn position_for_index(&self, index: usize) -> Option<Point<Pixels>> {
        let mut line_origin_y = px(0.);
        let mut line_start_ix = 0;

        for line in &self.lines {
            let line_end_ix = line_start_ix + line.len();
            if index > line_end_ix {
                line_origin_y += line.size(self.line_height).height;
                line_start_ix = line_end_ix + 1;
                continue;
            }

            let local_index = index.saturating_sub(line_start_ix);
            let position = line.position_for_index(local_index, self.line_height)?;
            return Some(point(position.x, position.y + line_origin_y));
        }

        Some(Point::default())
    }

    fn bounds_for_range(
        &self,
        range: Range<usize>,
        origin: Point<Pixels>,
    ) -> Option<Bounds<Pixels>> {
        let quads = self.selection_quads(range, origin);
        let first = quads.first()?;
        let mut bounds = first.bounds;

        for quad in quads.into_iter().skip(1) {
            bounds = Bounds::from_corners(
                point(
                    bounds.left().min(quad.bounds.left()),
                    bounds.top().min(quad.bounds.top()),
                ),
                point(
                    bounds.right().max(quad.bounds.right()),
                    bounds.bottom().max(quad.bounds.bottom()),
                ),
            );
        }

        Some(bounds)
    }

    fn selection_quads(&self, range: Range<usize>, origin: Point<Pixels>) -> Vec<PaintQuad> {
        if range.is_empty() || self.is_placeholder {
            return Vec::new();
        }

        let mut quads = Vec::new();
        let mut line_start_ix = 0;

        for line in &self.lines {
            for segment in visual_segments(line, line_start_ix, self.line_height) {
                let start = range.start.max(segment.global_start);
                let end = range.end.min(segment.global_end);

                if start >= end {
                    continue;
                }

                let start_x = if start == segment.global_start {
                    px(0.)
                } else {
                    line.position_for_index(start - line_start_ix, self.line_height)
                        .map(|position| position.x)
                        .unwrap_or(px(0.))
                };
                let end_x = if end == segment.global_end {
                    segment.width
                } else {
                    line.position_for_index(end - line_start_ix, self.line_height)
                        .map(|position| position.x)
                        .unwrap_or(segment.width)
                };

                if end_x <= start_x {
                    continue;
                }

                quads.push(fill(
                    Bounds::new(
                        point(origin.x + start_x, origin.y + segment.top),
                        size(end_x - start_x, self.line_height),
                    ),
                    rgba(selection()),
                ));
            }

            line_start_ix += line.len() + 1;
        }

        quads
    }
}

fn visible_scroll_offset(
    layout: &TextLayoutSnapshot,
    current_scroll_y: Pixels,
    visible_height: Pixels,
    cursor: usize,
) -> Pixels {
    if layout.is_placeholder {
        return px(0.);
    }

    let max_scroll = (layout.total_height() - visible_height).max(px(0.));
    let Some(cursor_position) = layout.position_for_index(cursor) else {
        return current_scroll_y.min(max_scroll).max(px(0.));
    };

    let cursor_top = cursor_position.y;
    let cursor_bottom = cursor_top + layout.line_height;
    let mut scroll_y = current_scroll_y.min(max_scroll).max(px(0.));

    if cursor_top < scroll_y {
        scroll_y = cursor_top;
    } else if cursor_bottom > scroll_y + visible_height {
        scroll_y = cursor_bottom - visible_height;
    }

    scroll_y.min(max_scroll).max(px(0.))
}

struct VisualLineSegment {
    global_start: usize,
    global_end: usize,
    top: Pixels,
    width: Pixels,
}

fn visual_segments(
    line: &WrappedLine,
    global_line_start: usize,
    line_height: Pixels,
) -> Vec<VisualLineSegment> {
    let mut segments = Vec::new();
    let mut segment_start = 0;
    let mut top = px(0.);
    let mut boundaries = line
        .wrap_boundaries()
        .iter()
        .map(|boundary| {
            let run = &line.runs()[boundary.run_ix];
            run.glyphs[boundary.glyph_ix].index
        })
        .collect::<Vec<_>>();
    boundaries.push(line.len());

    for boundary in boundaries {
        let width = line
            .position_for_index(boundary, line_height)
            .map(|position| position.x)
            .unwrap_or(px(0.));
        segments.push(VisualLineSegment {
            global_start: global_line_start + segment_start,
            global_end: global_line_start + boundary,
            top,
            width,
        });
        segment_start = boundary;
        top += line_height;
    }

    if segments.is_empty() {
        segments.push(VisualLineSegment {
            global_start: global_line_start,
            global_end: global_line_start,
            top: px(0.),
            width: px(0.),
        });
    }

    segments
}
