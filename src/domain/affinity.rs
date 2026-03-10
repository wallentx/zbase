#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Affinity {
    #[default]
    None,
    Positive,
    Broken,
}
