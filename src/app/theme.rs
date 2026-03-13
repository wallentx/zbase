use crate::models::settings_model::ThemeMode;
use gpui::WindowAppearance;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ThemeVariant {
    Light,
    Dark,
}

pub fn resolve_theme(mode: &ThemeMode, appearance: WindowAppearance) -> ThemeVariant {
    match mode {
        ThemeMode::Light => ThemeVariant::Light,
        ThemeMode::Dark => ThemeVariant::Dark,
        ThemeMode::System => match appearance {
            WindowAppearance::Dark | WindowAppearance::VibrantDark => ThemeVariant::Dark,
            WindowAppearance::Light | WindowAppearance::VibrantLight => ThemeVariant::Light,
        },
    }
}
