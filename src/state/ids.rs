macro_rules! runtime_id_type {
    ($name:ident, $default:literal) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new($default)
            }
        }
    };
}

runtime_id_type!(OpId, "op");
runtime_id_type!(QueryId, "query");
runtime_id_type!(ClientMessageId, "client_message");
runtime_id_type!(LocalMessageId, "local_message");
runtime_id_type!(LocalAttachmentId, "local_attachment");
runtime_id_type!(DebounceKey, "debounce");
