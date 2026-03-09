#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThemeMode {
    Light,
    Dark,
    System,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Density {
    Comfortable,
    Compact,
}

use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct SettingsModel {
    pub theme_mode: ThemeMode,
    pub density: Density,
    pub reduced_motion: bool,
    pub show_right_pane: bool,
    pub sidebar_section_order: HashMap<String, Vec<String>>,
    pub sidebar_collapsed_sections: HashMap<String, HashSet<String>>,
    pub dismissed_pinned_items: HashMap<String, String>,
}

impl Default for SettingsModel {
    fn default() -> Self {
        Self {
            theme_mode: ThemeMode::System,
            density: Density::Comfortable,
            reduced_motion: false,
            show_right_pane: true,
            sidebar_section_order: HashMap::new(),
            sidebar_collapsed_sections: HashMap::new(),
            dismissed_pinned_items: HashMap::new(),
        }
    }
}
