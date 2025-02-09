use super::types::{AdminClient, DefaultAdminClient};
use crate::error::Error;
use rdkafka::ClientConfig;

/// Builder for creating an AdminClient with custom configuration
pub struct AdminClientBuilder {
    config: ClientConfig,
}

impl AdminClientBuilder {
    pub fn new() -> Self {
        let config = ClientConfig::new();
        Self { config }
    }

    pub fn bootstrap_servers(mut self, servers: &[&str]) -> Self {
        self.config.set("bootstrap.servers", servers.join(","));
        self
    }

    pub fn client_id(mut self, client_id: &str) -> Self {
        self.config.set("client.id", client_id);
        self
    }

    pub fn set(mut self, key: &str, value: &str) -> Self {
        self.config.set(key, value);
        self
    }

    pub async fn build(self) -> Result<AdminClient, Error> {
        let admin_client: DefaultAdminClient = self.config.create()?;
        AdminClient::new(admin_client).await
    }
}

impl Default for AdminClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}
