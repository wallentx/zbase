use std::collections::{HashMap, HashSet};

use crate::{
    models::{
        emoji_picker_model::{
            EmojiPickerItem, EmojiPickerModel, SKIN_TONE_OPTIONS, search_emoji_items,
            selected_emoji_for_tone,
        },
        overlay_model::OverlayModel,
    },
    state::state::EmojiRenderState,
    views::{
        accent, accent_soft, app_window::AppWindow, badge, panel_alt_bg, panel_surface,
        search_icon, shell_border, shell_border_strong, text_primary, text_secondary, tint,
    },
};
use gpui::{
    AnyElement, Context, Entity, ImageSource, InteractiveElement, IntoElement, ObjectFit,
    ParentElement, SharedString, StatefulInteractiveElement, Styled, StyledImage, div, img, px,
    rgb,
};

use crate::views::input::TextField;

const PANEL_WIDTH_PX: f32 = 336.0;
const PANEL_HEIGHT_PX: f32 = 480.0;
const CELL_SIZE_PX: f32 = 36.0;
const SEARCH_MAX_RESULTS: usize = 50;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Bookmark {
    Frequent,
    Group(emojis::Group),
    Custom,
}

pub(crate) fn render_emoji_picker(
    overlay: &OverlayModel,
    picker: &EmojiPickerModel,
    input: &Entity<TextField>,
    custom_emoji_index: Option<&HashMap<String, EmojiRenderState>>,
    supports_custom_emoji: bool,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    if !overlay.emoji_picker_open {
        return div().into_any_element();
    }

    let query = picker.query.trim();
    let custom_items = collect_custom_items(custom_emoji_index, supports_custom_emoji);
    let frequent_items = resolve_frequent_items(&picker.recent_aliases, &custom_items);
    let active_group = picker.active_group;
    let hovered = picker
        .hovered
        .clone()
        .or_else(|| frequent_items.first().cloned());

    let mut content = div()
        .rounded_xl()
        .border_1()
        .border_color(shell_border_strong())
        .bg(panel_surface())
        .w(px(PANEL_WIDTH_PX))
        .h(px(PANEL_HEIGHT_PX))
        .flex()
        .flex_col()
        .overflow_hidden();

    content = content.child(render_search_row(input, cx));
    if picker.skin_tone_expanded {
        content = content.child(render_skin_tone_row(picker, cx));
    }

    if !query.is_empty() {
        let results = search_emoji_items(query, &custom_items, SEARCH_MAX_RESULTS);
        content = content.child(render_search_results(&results, picker, cx));
    } else {
        content = content.child(render_bookmarks(
            active_group,
            !frequent_items.is_empty(),
            !custom_items.is_empty(),
            cx,
        ));
        content = content.child(render_sections(
            picker,
            &frequent_items,
            &custom_items,
            active_group,
            cx,
        ));
    }

    content = content.child(render_hover_footer(hovered, picker.selected_skin_tone));
    content.into_any_element()
}

fn render_search_row(input: &Entity<TextField>, cx: &mut Context<AppWindow>) -> AnyElement {
    div()
        .px_3()
        .py_2()
        .flex()
        .items_center()
        .gap_2()
        .border_b_1()
        .border_color(shell_border())
        .child(search_icon(text_secondary()))
        .child(
            div()
                .flex_1()
                .text_sm()
                .text_color(rgb(text_primary()))
                .id("emoji-picker-input")
                .on_click(cx.listener(AppWindow::focus_emoji_picker_input))
                .child(input.clone()),
        )
        .child(
            div()
                .id("emoji-picker-tone-toggle")
                .on_click(cx.listener(AppWindow::toggle_emoji_picker_skin_tone_expanded_click))
                .child(badge("Tone", panel_alt_bg(), text_secondary())),
        )
        .child(
            div()
                .id("emoji-picker-close")
                .on_click(cx.listener(AppWindow::close_emoji_picker_click))
                .child(badge("Close", panel_alt_bg(), text_secondary())),
        )
        .into_any_element()
}

fn render_skin_tone_row(picker: &EmojiPickerModel, cx: &mut Context<AppWindow>) -> AnyElement {
    let selected = picker.selected_skin_tone;
    div()
        .px_3()
        .py_2()
        .border_b_1()
        .border_color(shell_border())
        .flex()
        .items_center()
        .justify_between()
        .children(
            SKIN_TONE_OPTIONS
                .into_iter()
                .enumerate()
                .map(|(idx, tone)| {
                    let is_selected = Some(tone) == selected;
                    div()
                        .id(SharedString::from(format!("emoji-tone-{idx}")))
                        .w(px(22.0))
                        .h(px(22.0))
                        .rounded_full()
                        .bg(rgb(skin_tone_color(tone)))
                        .border_1()
                        .border_color(if is_selected {
                            tint(accent(), 1.0)
                        } else {
                            shell_border()
                        })
                        .hover(|d| d.opacity(0.9))
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.set_emoji_picker_skin_tone(Some(tone), cx);
                        }))
                        .into_any_element()
                }),
        )
        .into_any_element()
}

fn render_bookmarks(
    active_group: Option<emojis::Group>,
    has_frequent: bool,
    has_custom: bool,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let mut bookmarks = Vec::new();
    if has_frequent {
        bookmarks.push(Bookmark::Frequent);
    }
    bookmarks.extend(emojis::Group::iter().map(Bookmark::Group));
    if has_custom {
        bookmarks.push(Bookmark::Custom);
    }

    let default_active = emojis::Group::iter().next();
    let effective_active = active_group.or(default_active);

    div()
        .px_2()
        .py_2()
        .flex()
        .gap_1()
        .border_b_1()
        .border_color(shell_border())
        .children(bookmarks.into_iter().map(|bookmark| {
            let (label, selected) = match bookmark {
                Bookmark::Frequent => ("🕐".to_string(), effective_active.is_none()),
                Bookmark::Group(group) => (
                    group_bookmark_emoji(group).to_string(),
                    Some(group) == effective_active,
                ),
                Bookmark::Custom => ("⭐".to_string(), false),
            };
            div()
                .id(SharedString::from(format!("emoji-bookmark-{label}")))
                .w(px(30.0))
                .h(px(28.0))
                .rounded_md()
                .flex()
                .items_center()
                .justify_center()
                .text_sm()
                .bg(if selected {
                    tint(accent_soft(), 0.92)
                } else {
                    panel_surface()
                })
                .hover(|d| d.bg(rgb(accent_soft())))
                .on_click(cx.listener(move |this, _, _, cx| match bookmark {
                    Bookmark::Group(group) => this.set_emoji_picker_active_group(Some(group), cx),
                    Bookmark::Frequent | Bookmark::Custom => {
                        this.set_emoji_picker_active_group(None, cx)
                    }
                }))
                .child(label)
                .into_any_element()
        }))
        .into_any_element()
}

fn render_sections(
    picker: &EmojiPickerModel,
    frequent_items: &[EmojiPickerItem],
    custom_items: &[EmojiPickerItem],
    active_group: Option<emojis::Group>,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let mut groups = emojis::Group::iter().collect::<Vec<_>>();
    if let Some(group) = active_group
        && let Some(idx) = groups.iter().position(|candidate| *candidate == group)
    {
        let selected = groups.remove(idx);
        groups.insert(0, selected);
    }

    let mut content = div().px_3().py_2().flex().flex_col().gap_2();

    if !frequent_items.is_empty() {
        content = content
            .child(section_title("Frequently used"))
            .child(render_grid(frequent_items, picker.selected_skin_tone, cx));
    }

    for group in groups {
        let stock_items = group
            .emojis()
            .map(EmojiPickerItem::Stock)
            .collect::<Vec<EmojiPickerItem>>();
        content = content
            .child(section_title(group_label(group)))
            .child(render_grid(&stock_items, picker.selected_skin_tone, cx));
    }

    if !custom_items.is_empty() {
        content = content
            .child(section_title("Custom emoji"))
            .child(render_grid(custom_items, picker.selected_skin_tone, cx));
    }

    div()
        .flex_1()
        .min_h(px(0.0))
        .id("emoji-picker-scroll")
        .overflow_y_scroll()
        .scrollbar_width(px(8.0))
        .pr_1()
        .child(content)
        .into_any_element()
}

fn render_search_results(
    results: &[EmojiPickerItem],
    picker: &EmojiPickerModel,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let mut content = div()
        .px_3()
        .py_2()
        .flex()
        .flex_col()
        .gap_2()
        .child(section_title("Search results"));

    if results.is_empty() {
        content = content.child(
            div()
                .rounded_md()
                .bg(panel_surface())
                .px_3()
                .py_2()
                .text_sm()
                .text_color(rgb(text_secondary()))
                .child("No emoji matches"),
        );
    } else {
        content = content.child(render_grid(results, picker.selected_skin_tone, cx));
    }

    div()
        .flex_1()
        .min_h(px(0.0))
        .id("emoji-picker-search-scroll")
        .overflow_y_scroll()
        .scrollbar_width(px(8.0))
        .pr_1()
        .child(content)
        .into_any_element()
}

fn render_grid(
    items: &[EmojiPickerItem],
    tone: Option<emojis::SkinTone>,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let mut seen = HashSet::new();
    div()
        .flex()
        .flex_wrap()
        .gap_1()
        .children(items.iter().filter_map(|item| {
            let key = item.key();
            if !seen.insert(key.clone()) {
                return None;
            }
            let item = item.clone();
            let hover_item = item.clone();
            let click_item = item.clone();
            Some(
                div()
                    .id(SharedString::from(format!("emoji-cell-{key}")))
                    .w(px(CELL_SIZE_PX))
                    .h(px(CELL_SIZE_PX))
                    .rounded_sm()
                    .flex()
                    .items_center()
                    .justify_center()
                    .hover(|d| d.bg(rgb(accent_soft())))
                    .on_mouse_move(cx.listener(move |this, _, _, cx| {
                        this.set_emoji_picker_hovered(Some(hover_item.clone()), cx);
                    }))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.emoji_picker_pick_item(click_item.clone(), window, cx);
                    }))
                    .child(render_emoji_item(&item, tone))
                    .into_any_element(),
            )
        }))
        .into_any_element()
}

fn render_emoji_item(item: &EmojiPickerItem, tone: Option<emojis::SkinTone>) -> AnyElement {
    match item {
        EmojiPickerItem::Stock(emoji) => {
            let selected = selected_emoji_for_tone(emoji, tone);
            div()
                .text_size(px(24.0))
                .line_height(px(26.0))
                .child(selected.as_str().to_string())
                .into_any_element()
        }
        EmojiPickerItem::Custom {
            alias,
            unicode,
            asset_path,
        } => {
            if let Some(path) = asset_path.as_ref() {
                return div()
                    .w(px(24.0))
                    .h(px(24.0))
                    .overflow_hidden()
                    .child(
                        img(ImageSource::from(std::path::PathBuf::from(
                            crate::views::normalize_local_source_path(path),
                        )))
                        .w(px(24.0))
                        .h(px(24.0))
                        .object_fit(ObjectFit::Contain)
                        .with_fallback({
                            let alias = alias.clone();
                            move || {
                                div()
                                    .text_xs()
                                    .text_color(rgb(text_secondary()))
                                    .child(format!(":{alias}:"))
                                    .into_any_element()
                            }
                        }),
                    )
                    .into_any_element();
            }
            if let Some(unicode) = unicode.as_ref() {
                return div()
                    .text_size(px(24.0))
                    .line_height(px(26.0))
                    .child(unicode.clone())
                    .into_any_element();
            }
            div()
                .text_xs()
                .text_color(rgb(text_secondary()))
                .child(format!(":{alias}:"))
                .into_any_element()
        }
    }
}

fn render_hover_footer(
    item: Option<EmojiPickerItem>,
    tone: Option<emojis::SkinTone>,
) -> AnyElement {
    let mut footer = div()
        .px_3()
        .py_2()
        .border_t_1()
        .border_color(shell_border())
        .bg(rgb(panel_alt_bg()))
        .flex()
        .items_center()
        .gap_2();

    if let Some(item) = item {
        let (title, subtitle) = hover_text(&item);
        footer = footer
            .child(
                div()
                    .w(px(30.0))
                    .h(px(30.0))
                    .rounded_sm()
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(render_emoji_item(&item, tone)),
            )
            .child(
                div()
                    .flex_1()
                    .min_w(px(0.0))
                    .flex()
                    .flex_col()
                    .child(div().text_sm().text_color(rgb(text_primary())).child(title))
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(text_secondary()))
                            .child(subtitle),
                    ),
            );
    } else {
        footer = footer.child(
            div()
                .text_xs()
                .text_color(rgb(text_secondary()))
                .child("Pick an emoji"),
        );
    }

    footer.into_any_element()
}

fn section_title(title: impl Into<String>) -> AnyElement {
    div()
        .text_xs()
        .text_color(rgb(text_secondary()))
        .font_weight(gpui::FontWeight::MEDIUM)
        .child(title.into())
        .into_any_element()
}

fn hover_text(item: &EmojiPickerItem) -> (String, String) {
    match item {
        EmojiPickerItem::Stock(emoji) => {
            let title = emoji.name().to_string();
            let shortcodes = emoji
                .shortcodes()
                .map(|shortcode| format!(":{shortcode}:"))
                .collect::<Vec<_>>()
                .join("  ");
            (title, shortcodes)
        }
        EmojiPickerItem::Custom { alias, .. } => (format!(":{alias}:"), "Custom emoji".to_string()),
    }
}

fn resolve_frequent_items(
    recent_aliases: &[String],
    custom_items: &[EmojiPickerItem],
) -> Vec<EmojiPickerItem> {
    let custom_lookup = custom_items
        .iter()
        .filter_map(|item| match item {
            EmojiPickerItem::Custom { alias, .. } => {
                Some((alias.to_ascii_lowercase(), item.clone()))
            }
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    let mut seen = HashSet::new();
    let mut items = Vec::new();
    for alias in recent_aliases {
        let key = alias.trim().to_ascii_lowercase();
        if key.is_empty() || !seen.insert(key.clone()) {
            continue;
        }
        if let Some(custom) = custom_lookup.get(&key) {
            items.push(custom.clone());
            continue;
        }
        if let Some(stock) = emojis::get_by_shortcode(&key) {
            items.push(EmojiPickerItem::Stock(stock));
        }
    }
    items
}

fn collect_custom_items(
    custom_emoji_index: Option<&HashMap<String, EmojiRenderState>>,
    supports_custom_emoji: bool,
) -> Vec<EmojiPickerItem> {
    if !supports_custom_emoji {
        return Vec::new();
    }
    let mut items = custom_emoji_index
        .into_iter()
        .flat_map(|index| index.values())
        .map(|emoji| EmojiPickerItem::Custom {
            alias: emoji.alias.clone(),
            unicode: emoji.unicode.clone(),
            asset_path: emoji.asset_path.clone(),
        })
        .collect::<Vec<_>>();
    items.sort_by_key(|left| left.key());
    items
}

fn group_label(group: emojis::Group) -> &'static str {
    match group {
        emojis::Group::SmileysAndEmotion => "Smileys & Emotion",
        emojis::Group::PeopleAndBody => "People & Body",
        emojis::Group::AnimalsAndNature => "Animals & Nature",
        emojis::Group::FoodAndDrink => "Food & Drink",
        emojis::Group::TravelAndPlaces => "Travel & Places",
        emojis::Group::Activities => "Activities",
        emojis::Group::Objects => "Objects",
        emojis::Group::Symbols => "Symbols",
        emojis::Group::Flags => "Flags",
    }
}

fn group_bookmark_emoji(group: emojis::Group) -> &'static str {
    match group {
        emojis::Group::SmileysAndEmotion => "😀",
        emojis::Group::PeopleAndBody => "🖐",
        emojis::Group::AnimalsAndNature => "🐻",
        emojis::Group::FoodAndDrink => "🍎",
        emojis::Group::TravelAndPlaces => "✈️",
        emojis::Group::Activities => "⚽",
        emojis::Group::Objects => "💡",
        emojis::Group::Symbols => "🔣",
        emojis::Group::Flags => "🏁",
    }
}

fn skin_tone_color(tone: emojis::SkinTone) -> u32 {
    match tone {
        emojis::SkinTone::Default => 0xf4cc87,
        emojis::SkinTone::Light => 0xf6d6b7,
        emojis::SkinTone::MediumLight => 0xe6bc98,
        emojis::SkinTone::Medium => 0xd19a73,
        emojis::SkinTone::MediumDark => 0xad7a52,
        emojis::SkinTone::Dark => 0x7f5539,
        _ => 0xf4cc87,
    }
}
