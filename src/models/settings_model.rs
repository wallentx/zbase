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
    pub emoji_skin_tone: Option<String>,
    pub emoji_recents: Vec<String>,
    pub sidebar_section_order: HashMap<String, Vec<String>>,
    pub sidebar_collapsed_sections: HashMap<String, HashSet<String>>,
    pub dismissed_pinned_items: HashMap<String, String>,
    pub quick_switcher_affinity: HashMap<String, (f32, i64)>,
}

impl Default for SettingsModel {
    fn default() -> Self {
        Self {
            theme_mode: ThemeMode::System,
            density: Density::Comfortable,
            reduced_motion: false,
            show_right_pane: true,
            emoji_skin_tone: None,
            emoji_recents: Vec::new(),
            sidebar_section_order: HashMap::new(),
            sidebar_collapsed_sections: HashMap::new(),
            dismissed_pinned_items: HashMap::new(),
            quick_switcher_affinity: HashMap::new(),
        }
    }
}
