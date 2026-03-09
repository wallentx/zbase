pub mod backends;
pub mod draft_store;
pub mod local_store;
pub mod media_service;
pub mod notifications_service;
pub mod search_index;
pub mod search_service;
pub mod settings_store;
pub mod sync_service;
pub mod uploads_service;

use self::{
    draft_store::DraftStore, media_service::MediaService,
    notifications_service::NotificationsService, search_service::SearchService,
    settings_store::SettingsStore, sync_service::SyncService, uploads_service::UploadsService,
};

#[derive(Default)]
pub struct AppServices {
    pub sync: SyncService,
    pub media: MediaService,
    pub search: SearchService,
    pub uploads: UploadsService,
    pub notifications: NotificationsService,
    pub draft_store: DraftStore,
    pub settings_store: SettingsStore,
}
