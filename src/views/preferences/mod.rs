use crate::{
    models::settings_model::{Density, SettingsModel, ThemeMode},
    views::{
        accent, app_window::AppWindow, badge, border, panel_alt_bg, panel_alt_surface, panel_bg,
        panel_surface, text_primary, text_secondary,
    },
};
use gpui::{
    AnyElement, Context, FontWeight, InteractiveElement, IntoElement, ParentElement,
    StatefulInteractiveElement, Styled, div, px, rgb,
};

#[derive(Default)]
pub struct PreferencesView;

impl PreferencesView {
    pub fn render(&self, settings: &SettingsModel, cx: &mut Context<AppWindow>) -> AnyElement {
        let theme = match settings.theme_mode {
            ThemeMode::Light => "Light",
            ThemeMode::Dark => "Dark",
            ThemeMode::System => "System",
        };
        let density = match settings.density {
            Density::Comfortable => "Comfortable",
            Density::Compact => "Compact",
        };

        div()
            .flex_1()
            .m_4()
            .rounded_lg()
            .border_1()
            .border_color(rgb(border()))
            .bg(panel_surface())
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .flex_1()
                    .id("preferences-scroll")
                    .overflow_y_scroll()
                    .scrollbar_width(px(8.))
                    .p_6()
                    .pr_4()
                    .flex()
                    .flex_col()
                    .gap_4()
                    .child(div().font_weight(FontWeight::SEMIBOLD).child("Preferences"))
                    .child(
                        div()
                            .flex()
                            .gap_2()
                            .child(badge(theme, panel_alt_bg(), text_primary()))
                            .child(badge(density, panel_alt_bg(), text_primary())),
                    )
                    .child(
                        div()
                            .rounded_lg()
                            .bg(panel_alt_surface())
                            .p_4()
                            .text_sm()
                            .text_color(rgb(text_secondary()))
                            .child(
                                "Appearance, notifications, shortcuts, accessibility, and devices.",
                            ),
                    )
                    .child(
                        div()
                            .rounded_lg()
                            .border_1()
                            .border_color(rgb(border()))
                            .bg(panel_alt_surface())
                            .p_4()
                            .flex()
                            .justify_between()
                            .items_center()
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_1()
                                    .child(div().font_weight(FontWeight::MEDIUM).child("Theme"))
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(rgb(text_secondary()))
                                            .child(format!("Current: {theme}")),
                                    ),
                            )
                            .child(
                                div()
                                    .id("preferences-theme")
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.cycle_theme(window, cx);
                                    }))
                                    .child(badge("Cycle", accent(), panel_bg())),
                            ),
                    )
                    .child(
                        div()
                            .rounded_lg()
                            .border_1()
                            .border_color(rgb(border()))
                            .bg(panel_alt_surface())
                            .p_4()
                            .flex()
                            .justify_between()
                            .items_center()
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_1()
                                    .child(div().font_weight(FontWeight::MEDIUM).child("Density"))
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(rgb(text_secondary()))
                                            .child(format!("Current: {density}")),
                                    ),
                            )
                            .child(
                                div()
                                    .id("preferences-density")
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.cycle_density(window, cx);
                                    }))
                                    .child(badge("Toggle", accent(), panel_bg())),
                            ),
                    )
                    .child(
                        div()
                            .rounded_lg()
                            .border_1()
                            .border_color(rgb(border()))
                            .bg(panel_alt_surface())
                            .p_4()
                            .flex()
                            .justify_between()
                            .items_center()
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_1()
                                    .child(
                                        div()
                                            .font_weight(FontWeight::MEDIUM)
                                            .child("Reduced motion"),
                                    )
                                    .child(
                                        div().text_sm().text_color(rgb(text_secondary())).child(
                                            if settings.reduced_motion {
                                                "On".to_string()
                                            } else {
                                                "Off".to_string()
                                            },
                                        ),
                                    ),
                            )
                            .child(
                                div()
                                    .id("preferences-motion")
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.toggle_reduced_motion(window, cx);
                                    }))
                                    .child(badge("Toggle", accent(), panel_bg())),
                            ),
                    )
                    .child(
                        div()
                            .rounded_lg()
                            .border_1()
                            .border_color(rgb(border()))
                            .bg(panel_alt_surface())
                            .p_4()
                            .flex()
                            .justify_between()
                            .items_center()
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_1()
                                    .child(
                                        div().font_weight(FontWeight::MEDIUM).child("Right pane"),
                                    )
                                    .child(
                                        div().text_sm().text_color(rgb(text_secondary())).child(
                                            if settings.show_right_pane {
                                                "Enabled for threads and details".to_string()
                                            } else {
                                                "Disabled globally".to_string()
                                            },
                                        ),
                                    ),
                            )
                            .child(
                                div()
                                    .id("preferences-right-pane")
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.toggle_right_pane_setting(window, cx);
                                    }))
                                    .child(badge(
                                        if settings.show_right_pane {
                                            "Disable"
                                        } else {
                                            "Enable"
                                        },
                                        accent(),
                                        panel_bg(),
                                    )),
                            ),
                    ),
            )
            .into_any_element()
    }
}
