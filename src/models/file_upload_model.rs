use std::path::PathBuf;

use crate::domain::attachment::AttachmentKind;

#[derive(Clone, Debug)]
pub struct FileUploadCandidate {
    pub path: PathBuf,
    pub filename: String,
    pub kind: AttachmentKind,
    pub size_bytes: u64,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub caption: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadTarget {
    Composer,
    Thread,
}

#[derive(Clone, Debug)]
pub struct FileUploadLightboxModel {
    pub candidates: Vec<FileUploadCandidate>,
    pub current_index: usize,
    pub target: UploadTarget,
}

impl FileUploadLightboxModel {
    pub fn current_candidate(&self) -> Option<&FileUploadCandidate> {
        self.candidates.get(self.current_index)
    }

    pub fn current_candidate_mut(&mut self) -> Option<&mut FileUploadCandidate> {
        self.candidates.get_mut(self.current_index)
    }
}
