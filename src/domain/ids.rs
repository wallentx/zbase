macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new(stringify!($name))
            }
        }
    };
}

id_type!(WorkspaceId);
id_type!(ChannelId);
id_type!(DmId);
id_type!(ConversationId);
id_type!(MessageId);
id_type!(UserId);
id_type!(CallId);
id_type!(SidebarSectionId);
