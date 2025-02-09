use super::types::{AdminClient, AdminResult, DefaultAdminClient};
use rdkafka::admin::{AdminOptions, NewTopic, ResourceSpecifier};
use std::sync::Arc;
use tracing::{event, instrument, Level};

impl AdminClient {
    #[instrument(skip(admin_client))]
    pub async fn new(admin_client: DefaultAdminClient) -> AdminResult<Self> {
        let opts = AdminOptions::new();
        let configs = ResourceSpecifier::Topic("_schemas");

        if let Err(e) = admin_client.describe_configs([&configs], &opts).await {
            event!(
                Level::ERROR,
                "Failed to connect admin client to cluster: {}",
                e
            );
            return Err(e.into());
        }

        event!(Level::INFO, "Connected admin client to Kafka cluster");
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
