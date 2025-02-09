use rdkafka::admin::{AdminOptions, NewTopic};
use std::sync::Arc;

use super::types::{AdminClient, AdminResult, DefaultAdminClient};

impl AdminClient {
    pub async fn new(admin_client: DefaultAdminClient) -> AdminResult<Self> {
        Ok(Self {
            admin_client: Arc::new(admin_client),
        })
    }

    pub async fn create_topic(
        &self,
        name: &str,
        num_partitions: u16,
        replication_factor: u16,
    ) -> AdminResult<()> {
        let opts = AdminOptions::new();
        let topic = NewTopic {
            name,
            num_partitions: num_partitions.into(),
            replication: rdkafka::admin::TopicReplication::Fixed(replication_factor.into()),
            config: vec![
                ("compression.type", "zstd"),
                ("auto.offset.reset", "beginning"),
            ],
        };

        self.admin_client.create_topics([&topic], &opts).await?;
        Ok(())
    }
}