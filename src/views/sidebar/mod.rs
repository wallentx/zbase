use crate::{
    domain::{ids::SidebarSectionId, route::Route},
    models::{
        app_model::Connectivity,
        sidebar_model::{SidebarModel, SidebarRow, SidebarSection},
    },
    views::app_window::AppWindow,
    views::{
        SIDEBAR_WIDTH_PX, accent,
        avatar::{Avatar, default_avatar_background, demo_avatar_asset},
        chevron_down_icon, chevron_right_icon, glass_surface_dark, hash_icon, mention, search_icon,
        subtle_surface, success, success_soft, text_primary, text_secondary,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, AppContext, Context, FontWeight, InteractiveElement, IntoElement, ParentElement,
    Render, SharedString, StatefulInteractiveElement, Styled, Window, div, px, rgb,
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

impl Sidebar {
    pub fn render(
        &self,
        sidebar: &SidebarModel,
        connectivity: &Connectivity,
        current_user_display_name: &str,
        current_user_avatar_asset: Option<&str>,
        dm_avatar_assets: &HashMap<String, String>,
        current_route: &Route,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let connectivity_tint = match connectivity {
            Connectivity::Online => success(),
            Connectivity::Reconnecting => 0xf59e0b,
            Connectivity::Offline => 0xef4444,
        };

        div()
            .w(px(SIDEBAR_WIDTH_PX))
            .flex_shrink_0()
            .h_full()
            .flex()
            .flex_col()
            .px_2()
            .bg(glass_surface_dark())
            .overflow_hidden()
            .text_color(rgb(text_primary()))
            .child(Self::render_title_bar_row(cx))
            .child(
                div()
                    .flex_1()
                    .id("sidebar-sections-scroll")
                    .overflow_y_scroll()
                    .scrollbar_width(px(8.))
                    .pr_2()
                    .pt_2()
                    .children(sidebar.sections.iter().map(|section| {
                        Self::render_section(
                            section,
                            &section.rows,
                            dm_avatar_assets,
                            current_route,
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

    fn render_title_bar_row(cx: &mut Context<AppWindow>) -> AnyElement {
        div()
            .when(cfg!(target_os = "macos"), |d| d.h(px(36.)))
            .flex()
            .items_end()
            .justify_end()
            .child(
                div()
                    .id(SharedString::from("sidebar-search-trigger"))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.toggle_quick_switcher(cx);
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
            .into_any_element()
    }

    fn render_profile_footer(
        connectivity_tint: u32,
        current_user_display_name: &str,
        current_user_avatar_asset: Option<&str>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        div()
            .py_2()
            .flex()
            .items_center()
            .justify_between()
            .child(
                div()
                    .id(SharedString::from("sidebar-profile"))
                    .on_click(cx.listener(AppWindow::preferences_click))
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

    fn render_section(
        section: &SidebarSection,
        visible_rows: &[SidebarRow],
        dm_avatar_assets: &HashMap<String, String>,
        current_route: &Route,
        cx: &mut Context<AppWindow>,
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
                    .justify_between()
                    .text_xs()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(text_secondary()))
                    .px_2()
                    .py_0p5()
                    .rounded_md()
                    .when(!is_unread_section, |div| {
                        div.on_click(cx.listener(move |this, _, _, cx| {
                            this.toggle_sidebar_section_click(section_id.clone(), cx);
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
                                this.reorder_sidebar_section(
                                    dragged.0.clone(),
                                    section_id_for_drop.clone(),
                                    cx,
                                );
                            },
                        ))
                    })
                    .child(section.title.clone())
                    .when(!is_unread_section, |div| {
                        div.child(if section.collapsed {
                            chevron_right_icon(text_secondary())
                        } else {
                            chevron_down_icon(text_secondary())
                        })
                    }),
            )
            .when(is_unread_section || !section.collapsed, |div| {
                div.children(
                    visible_rows
                        .iter()
                        .map(|row| Self::render_row(row, dm_avatar_assets, current_route, cx)),
                )
            })
            .into_any_element()
    }

    fn render_row(
        row: &SidebarRow,
        dm_avatar_assets: &HashMap<String, String>,
        current_route: &Route,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let selected = &row.route == current_route;
        let has_unread = row.unread_count > 0;
        let has_mention = row.mention_count > 0;
        let route_label = row.route.label();
        let route = row.route.clone();

        div()
            .id(SharedString::from(format!("sidebar-{route_label}")))
            .w_full()
            .rounded_md()
            .hover(|s| s.bg(subtle_surface()))
            .px_2()
            .py_1()
            .flex()
            .items_center()
            .justify_between()
            .on_click(cx.listener(move |this, _, window, cx| {
                this.navigate_to(route.clone(), window, cx);
            }))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1p5()
                    .text_xs()
                    .when(selected || has_unread, |d| d.font_weight(FontWeight::BOLD))
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
