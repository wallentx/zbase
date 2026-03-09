use crate::domain::{ids::DmId, route::Route};
use crate::{
    models::app_model::AppModel,
    views::app_window::AppWindow,
    views::{
        accent, accent_soft, text_primary, text_secondary, RAIL_WIDTH_PX, activity_icon, dm_icon,
        floating_shadow, glass_surface, home_icon, search_icon, shell_border, sliders_icon,
        subtle_surface, tint,
    },
};
use gpui::{
    AnyElement, Context, InteractiveElement, IntoElement, ParentElement, SharedString,
    StatefulInteractiveElement, Styled, div, px, rgb,
};

#[derive(Default)]
pub struct WorkspaceRail;

fn workspace_button(label: impl Into<SharedString>) -> AnyElement {
    div()
        .w(px(52.))
        .h(px(52.))
        .rounded_lg()
        .flex()
        .items_center()
        .justify_center()
        .bg(tint(accent_soft(), 0.90))
        .text_color(rgb(accent()))
        .font_weight(gpui::FontWeight::SEMIBOLD)
        .child(label.into())
        .into_any_element()
}

fn nav_glyph(active: bool, icon: AnyElement) -> AnyElement {
    div()
        .w(px(44.))
        .h(px(44.))
        .rounded_lg()
        .border_1()
        .border_color(shell_border())
        .flex()
        .items_center()
        .justify_center()
        .bg(if active {
            tint(accent_soft(), 0.96)
        } else {
            subtle_surface()
        })
        .child(icon)
        .into_any_element()
}

fn nav_item(
    icon: AnyElement,
    label: impl Into<SharedString>,
    active: bool,
    count: Option<u32>,
) -> AnyElement {
    let label = label.into();
    let item = div()
        .flex()
        .flex_col()
        .items_center()
        .gap_2()
        .child(nav_glyph(active, icon))
        .child(
            div()
                .text_xs()
                .text_color(rgb(if active { text_primary() } else { text_secondary() }))
                .child(label),
        )
        .into_any_element();

    match count.filter(|count| *count > 0) {
        Some(count) => div()
            .flex()
            .flex_col()
            .items_center()
            .gap_1()
            .child(item)
            .child(
                div()
                    .min_w(px(20.))
                    .h(px(20.))
                    .px_1p5()
                    .rounded_full()
                    .bg(subtle_surface())
                    .flex()
                    .items_center()
                    .justify_center()
                    .text_xs()
                    .text_color(rgb(text_secondary()))
                    .child(format!("{count}")),
            )
            .into_any_element(),
        None => item,
    }
}

impl WorkspaceRail {
    pub fn render(
        &self,
        app: &AppModel,
        current_route: &Route,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let workspace_route = Route::WorkspaceHome {
            workspace_id: app.active_workspace_id.clone(),
        };
        let dm_route = Route::DirectMessage {
            workspace_id: app.active_workspace_id.clone(),
            dm_id: DmId::new("alice"),
        };
        let home_active = matches!(current_route, Route::WorkspaceHome { .. });
        let dm_active = matches!(current_route, Route::DirectMessage { .. });
        let activity_active = matches!(current_route, Route::Activity { .. });
        let search_active = matches!(current_route, Route::Search { .. });
        let prefs_active = matches!(current_route, Route::Preferences);
        div()
            .w(px(RAIL_WIDTH_PX))
            .flex_shrink_0()
            .h_full()
            .px_3()
            .py_4()
            .flex()
            .flex_col()
            .justify_between()
            .rounded_lg()
            .border_1()
            .border_color(shell_border())
            .bg(glass_surface())
            .shadow(floating_shadow())
            .child(
                div()
                    .flex()
                    .flex_col()
                    .items_center()
                    .gap_5()
                    .child(workspace_button("A"))
                    .child(
                        div()
                            .id("rail-home")
                            .on_click(cx.listener(AppWindow::home_click))
                            .child(nav_item(
                                home_icon(if home_active { accent() } else { text_secondary() }),
                                "Home",
                                home_active,
                                None,
                            )),
                    )
                    .child(
                        div()
                            .id("rail-dms")
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.navigate_to(dm_route.clone(), window, cx);
                            }))
                            .child(nav_item(
                                dm_icon(if dm_active { accent() } else { text_secondary() }),
                                "DMs",
                                dm_active,
                                Some(1),
                            )),
                    )
                    .child(
                        div()
                            .id("rail-activity")
                            .on_click(cx.listener(AppWindow::activity_click))
                            .child(nav_item(
                                activity_icon(if activity_active {
                                    accent()
                                } else {
                                    text_secondary()
                                }),
                                "Activity",
                                activity_active,
                                Some(app.global_unread_count),
                            )),
                    )
                    .child(
                        div()
                            .id("rail-workspace-home")
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.navigate_to(workspace_route.clone(), window, cx);
                            }))
                            .child(nav_item(home_icon(text_secondary()), "Work", false, None)),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .items_center()
                    .gap_5()
                    .child(
                        div()
                            .id("rail-search")
                            .on_click(cx.listener(AppWindow::search_click))
                            .child(nav_item(
                                search_icon(if search_active {
                                    accent()
                                } else {
                                    text_secondary()
                                }),
                                "Search",
                                search_active,
                                None,
                            )),
                    )
                    .child(
                        div()
                            .id("rail-settings")
                            .on_click(cx.listener(AppWindow::preferences_click))
                            .child(nav_item(
                                sliders_icon(if prefs_active { accent() } else { text_secondary() }),
                                "Prefs",
                                prefs_active,
                                None,
                            )),
                    )
                    .child(
                        div()
                            .id("rail-profile")
                            .on_click(cx.listener(AppWindow::preferences_click))
                            .child(workspace_button("ME")),
                    ),
            )
            .into_any_element()
    }
}
