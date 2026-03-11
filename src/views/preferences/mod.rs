use crate::{
    models::settings_model::{SettingsModel, ThemeMode},
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
                            .child(badge(theme, panel_alt_bg(), text_primary())),
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
                    ),
            )
            .into_any_element()
    }
}
