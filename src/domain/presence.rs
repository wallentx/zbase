#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Availability {
    Active,
    Away,
    DoNotDisturb,
    Offline,
}

#[derive(Clone, Debug)]
pub struct Presence {
    pub availability: Availability,
    pub status_text: Option<String>,
}
