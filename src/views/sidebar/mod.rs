use crate::{
    domain::{ids::SidebarSectionId, route::Route},
    models::{
        app_model::Connectivity,
        sidebar_model::{SidebarModel, SidebarRow, SidebarSection},
    },
    views::{
        accent,
        avatar::{Avatar, default_avatar_background, demo_avatar_asset},
        chevron_down_icon, chevron_right_icon, glass_surface_dark, hash_icon, is_dark_theme,
        mention, plus_icon, search_icon, sidebar_bg, subtle_surface, success, success_soft,
        text_primary, text_secondary, tint,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, AppContext, ClickEvent, Context, DragMoveEvent, FontWeight, InteractiveElement,
    IntoElement, MouseMoveEvent, ParentElement, Render, ScrollHandle, SharedString,
    StatefulInteractiveElement, Styled, Window, div, px, rgb,
};
use std::collections::HashMap;

#[derive(Clone)]
struct DraggedSection(SidebarSectionId);

struct DragPreview {
    title: String,
}

impl Render for DragPreview {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .px_3()
            .py_1p5()
            .rounded_md()
            .bg(subtle_surface())
            .text_xs()
            .font_weight(FontWeight::SEMIBOLD)
            .text_color(rgb(text_secondary()))
            .child(self.title.clone())
    }
}

#[derive(Default)]
pub struct Sidebar;

pub trait SidebarHost: Sized + 'static {
    fn sidebar_toggle_quick_switcher(&mut self, cx: &mut Context<Self>);
    fn sidebar_open_new_chat(&mut self, window: &mut Window, cx: &mut Context<Self>);
    fn sidebar_open_preferences(&mut self, window: &mut Window, cx: &mut Context<Self>);
    fn sidebar_show_hover_tooltip(
        &mut self,
        text: String,
        anchor_x: f32,
        anchor_y: f32,
        cx: &mut Context<Self>,
    );
    fn sidebar_hide_hover_tooltip(&mut self, cx: &mut Context<Self>);
    fn sidebar_toggle_section(&mut self, section_id: SidebarSectionId, cx: &mut Context<Self>);
    fn sidebar_reorder_section(
        &mut self,
        dragged_id: SidebarSectionId,
        target_id: SidebarSectionId,
        cx: &mut Context<Self>,
    );
    fn sidebar_navigate_to(&mut self, route: Route, window: &mut Window, cx: &mut Context<Self>);
    fn sidebar_set_hovered_row(&mut self, label: Option<String>, cx: &mut Context<Self>);
    fn sidebar_hovered_row(&self) -> Option<&str>;
}

impl Sidebar {
    pub fn render<H: SidebarHost>(
        &self,
        sidebar: &SidebarModel,
        connectivity: &Connectivity,
        current_user_display_name: &str,
        current_user_avatar_asset: Option<&str>,
        dm_avatar_assets: &HashMap<String, String>,
        current_route: &Route,
        hovered_row: Option<&str>,
        scroll_handle: &ScrollHandle,
        cx: &mut Context<H>,
    ) -> AnyElement {
        let connectivity_tint = match connectivity {
            Connectivity::Online => success(),
            Connectivity::Reconnecting => 0xf59e0b,
            Connectivity::Offline => 0xef4444,
        };

        let drag_scroll_handle = scroll_handle.clone();

        div()
            .w(px(sidebar.width_px))
            .flex_shrink_0()
            .h_full()
            .flex()
            .flex_col()
            .px_2()
            .bg(if is_dark_theme() {
                tint(sidebar_bg(), 0.98)
            } else {
                glass_surface_dark()
            })
            .overflow_hidden()
            .text_color(rgb(text_primary()))
            .child(Self::render_title_bar_row(cx))
            .child(
                div()
                    .flex_1()
                    .id("sidebar-sections-scroll")
                    .overflow_y_scroll()
                    .scrollbar_width(px(8.))
                    .track_scroll(scroll_handle)
                    .on_drag_move::<DraggedSection>(move |event, _, _| {
                        Self::auto_scroll_during_drag(event, &drag_scroll_handle);
                    })
                    .pr_2()
                    .pt_2()
                    .children(sidebar.sections.iter().map(|section| {
                        Self::render_section(
                            section,
                            &section.rows,
                            dm_avatar_assets,
                            current_route,
                            hovered_row,
                            cx,
                        )
                    })),
            )
            .child(Self::render_profile_footer(
                connectivity_tint,
                current_user_display_name,
                current_user_avatar_asset,
                cx,
            ))
            .into_any_element()
    }

    fn render_title_bar_row<H: SidebarHost>(cx: &mut Context<H>) -> AnyElement {
        let search_tooltip = format!("Search ({})", command_shortcut_label("K"));
        let new_chat_tooltip = format!("New chat ({})", command_shortcut_label("N"));
        div()
            .when(cfg!(target_os = "macos"), |d| d.h(px(44.)))
            .flex()
            .items_end()
            .justify_end()
            .gap_1()
            .child(
                div()
                    .id(SharedString::from("sidebar-search-trigger"))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.sidebar_hide_hover_tooltip(cx);
                        this.sidebar_toggle_quick_switcher(cx);
                    }))
                    .on_mouse_move(cx.listener({
                        let search_tooltip = search_tooltip.clone();
                        move |this, event: &MouseMoveEvent, _, cx| {
                            this.sidebar_show_hover_tooltip(
                                search_tooltip.clone(),
                                f32::from(event.position.x),
                                f32::from(event.position.y),
                                cx,
                            );
                            cx.stop_propagation();
                        }
                    }))
                    .w(px(24.))
                    .h(px(24.))
                    .rounded_full()
                    .flex()
                    .items_center()
                    .justify_center()
                    .hover(|s| s.bg(subtle_surface()))
                    .child(search_icon(text_secondary())),
            )
            .child(
                div()
                    .id(SharedString::from("sidebar-new-chat-trigger"))
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.sidebar_hide_hover_tooltip(cx);
                        this.sidebar_open_new_chat(window, cx);
                    }))
                    .on_mouse_move(cx.listener({
                        let new_chat_tooltip = new_chat_tooltip.clone();
                        move |this, event: &MouseMoveEvent, _, cx| {
                            this.sidebar_show_hover_tooltip(
                                new_chat_tooltip.clone(),
                                f32::from(event.position.x),
                                f32::from(event.position.y),
                                cx,
                            );
                            cx.stop_propagation();
                        }
                    }))
                    .w(px(24.))
                    .h(px(24.))
                    .rounded_full()
                    .flex()
                    .items_center()
                    .justify_center()
                    .hover(|s| s.bg(subtle_surface()))
                    .child(plus_icon(text_secondary())),
            )
            .into_any_element()
    }

    fn render_profile_footer<H: SidebarHost>(
        connectivity_tint: u32,
        current_user_display_name: &str,
        current_user_avatar_asset: Option<&str>,
        cx: &mut Context<H>,
    ) -> AnyElement {
        div()
            .py_2()
            .flex()
            .items_center()
            .justify_between()
            .child(
                div()
                    .id(SharedString::from("sidebar-profile"))
                    .on_click(cx.listener(|this, _: &ClickEvent, window, cx| {
                        this.sidebar_open_preferences(window, cx);
                    }))
                    .flex()
                    .items_center()
                    .gap_2()
                    .rounded_md()
                    .px_1()
                    .py_1()
                    .hover(|s| s.bg(subtle_surface()))
                    .child(Avatar::render(
                        current_user_display_name,
                        current_user_avatar_asset,
                        24.,
                        success_soft(),
                        0xffffff,
                    ))
                    .child(
                        div()
                            .text_xs()
                            .font_weight(FontWeight::MEDIUM)
                            .child(current_user_display_name.to_string()),
                    ),
            )
            .child(
                div()
                    .w(px(6.))
                    .h(px(6.))
                    .rounded_full()
                    .bg(rgb(connectivity_tint)),
            )
            .into_any_element()
    }

    fn auto_scroll_during_drag(
        event: &DragMoveEvent<DraggedSection>,
        scroll_handle: &ScrollHandle,
    ) {
        let edge_zone = px(40.);
        let max_speed = px(8.);

        let mouse_y = event.event.position.y;
        let top = event.bounds.top();
        let bottom = event.bounds.bottom();

        let delta = if mouse_y < top + edge_zone {
            let proximity = (top + edge_zone - mouse_y) / edge_zone;
            max_speed * proximity.clamp(0.0, 1.0)
        } else if mouse_y > bottom - edge_zone {
            let proximity = (mouse_y - (bottom - edge_zone)) / edge_zone;
            -(max_speed * proximity.clamp(0.0, 1.0))
        } else {
            return;
        };

        let mut offset = scroll_handle.offset();
        let max = scroll_handle.max_offset();
        offset.y = (offset.y + delta).clamp(-max.height, px(0.));
        scroll_handle.set_offset(offset);
    }

    fn render_section<H: SidebarHost>(
        section: &SidebarSection,
        visible_rows: &[SidebarRow],
        dm_avatar_assets: &HashMap<String, String>,
        current_route: &Route,
        hovered_row: Option<&str>,
        cx: &mut Context<H>,
    ) -> AnyElement {
        let section_id = section.id.clone();
        let section_id_for_drop = section.id.clone();
        let drag_title = section.title.clone();
        let is_unread_section = section.id.0 == "unread";

        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(
                div()
                    .id(SharedString::from(format!(
                        "sidebar-section-{}",
                        section.id.0
                    )))
                    .flex()
                    .items_center()
                    .gap_1()
                    .text_xs()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(text_secondary()))
                    .px_2()
                    .py_0p5()
                    .rounded_md()
                    .cursor_pointer()
                    .when(!is_unread_section, |div| {
                        div.on_click(cx.listener(move |this, _, _, cx| {
                            this.sidebar_toggle_section(section_id.clone(), cx);
                        }))
                        .on_drag(DraggedSection(section.id.clone()), {
                            let title = drag_title.clone();
                            move |_, _, _, cx| {
                                cx.new(|_| DragPreview {
                                    title: title.clone(),
                                })
                            }
                        })
                        .drag_over::<DraggedSection>(|style, _, _, _| {
                            style.border_t_2().border_color(rgb(accent()))
                        })
                        .on_drop(cx.listener(
                            move |this, dragged: &DraggedSection, _, cx| {
                                this.sidebar_reorder_section(
                                    dragged.0.clone(),
                                    section_id_for_drop.clone(),
                                    cx,
                                );
                            },
                        ))
                    })
                    .child(section.title.clone())
                    .when(!is_unread_section, |el| {
                        el.child(if section.collapsed {
                            chevron_right_icon(text_secondary())
                        } else {
                            chevron_down_icon(text_secondary())
                        })
                    }),
            )
            .when(is_unread_section || !section.collapsed, |div| {
                div.children(visible_rows.iter().map(|row| {
                    Self::render_row(
                        &section.id,
                        row,
                        dm_avatar_assets,
                        current_route,
                        hovered_row,
                        cx,
                    )
                }))
            })
            .into_any_element()
    }

    fn render_row<H: SidebarHost>(
        section_id: &SidebarSectionId,
        row: &SidebarRow,
        dm_avatar_assets: &HashMap<String, String>,
        current_route: &Route,
        hovered_row: Option<&str>,
        cx: &mut Context<H>,
    ) -> AnyElement {
        let selected = &row.route == current_route;
        let has_unread = row.unread_count > 0;
        let has_mention = row.mention_count > 0;
        let route_label = row.route.label();
        let route = row.route.clone();
        let is_hovered = hovered_row == Some(route_label.as_str());
        let hover_label = route_label.clone();

        div()
            // Rows can appear in multiple sections (e.g. Unread + Channels/DMs). Keep ids unique so
            // hit-testing and click dispatch remain reliable.
            .id(SharedString::from(format!(
                "sidebar-{}-{route_label}",
                section_id.0
            )))
            .w_full()
            .rounded_md()
            .px_2()
            .py_1()
            .flex()
            .items_center()
            .justify_between()
            .cursor_pointer()
            .on_hover(cx.listener(move |this, hovered: &bool, _, cx| {
                this.sidebar_set_hovered_row(
                    if *hovered {
                        Some(hover_label.clone())
                    } else {
                        None
                    },
                    cx,
                );
            }))
            .on_click(cx.listener(move |this, _, window, cx| {
                this.sidebar_navigate_to(route.clone(), window, cx);
            }))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1p5()
                    .text_xs()
                    .when(selected || has_unread || is_hovered, |d| {
                        d.font_weight(FontWeight::BOLD)
                    })
                    .text_color(rgb(if selected { accent() } else { text_primary() }))
                    .child(Self::leading_element(
                        row,
                        &route_label,
                        dm_avatar_assets,
                        selected,
                    ))
                    .child(row.label.clone()),
            )
            .when(has_mention, |d| {
                d.child(div().w(px(6.)).h(px(6.)).rounded_full().bg(rgb(mention())))
            })
            .when(has_unread && !has_mention, |d| {
                d.child(div().w(px(6.)).h(px(6.)).rounded_full().bg(rgb(accent())))
            })
            .into_any_element()
    }

    fn leading_element(
        row: &SidebarRow,
        route_label: &str,
        dm_avatar_assets: &HashMap<String, String>,
        selected: bool,
    ) -> AnyElement {
        match &row.route {
            Route::Channel { .. } => div()
                .w(px(16.))
                .flex()
                .items_center()
                .justify_center()
                .child(hash_icon(if selected {
                    accent()
                } else {
                    text_secondary()
                }))
                .into_any_element(),
            Route::DirectMessage { .. } => {
                let live_asset = dm_avatar_assets.get(route_label).map(String::as_str);
                let asset = live_asset.or_else(|| demo_avatar_asset(&row.label));
                Avatar::render(
                    &row.label,
                    asset,
                    20.,
                    default_avatar_background(&row.label),
                    text_primary(),
                )
            }
            _ => div()
                .w(px(16.))
                .flex()
                .items_center()
                .justify_center()
                .child(chevron_right_icon(text_secondary()))
                .into_any_element(),
        }
    }
}

fn command_shortcut_label(key: &str) -> String {
    if cfg!(target_os = "macos") {
        format!("⌘{key}")
    } else {
        format!("Cmd+{key}")
    }
}
