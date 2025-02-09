use super::types::AdminClient;
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

    pub async fn build(self) -> Result<AdminClient, Error> {
        let admin_client = self.config.create()?;
        Ok(AdminClient::new(admin_client).await?)
    }
}
