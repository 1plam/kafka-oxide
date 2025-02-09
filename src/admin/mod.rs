mod builder;
mod client;
mod types;

pub use builder::AdminClientBuilder;
pub use types::AdminClient;

impl AdminClient {
    pub fn builder() -> AdminClientBuilder {
        AdminClientBuilder::new()
    }
}
