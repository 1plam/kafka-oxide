use super::types::{AdminClient, AdminResult, DefaultAdminClient};
use rdkafka::admin::{AdminOptions, NewTopic, ResourceSpecifier};
use rdkafka::error::KafkaError;
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

    #[instrument(skip(self))]
    pub async fn create_topic(
        &self,
        name: &str,
        num_partitions: u16,
        replication_factor: u16,
    ) -> AdminResult<()> {
        let opts = AdminOptions::new();
        let replication = rdkafka::admin::TopicReplication::Fixed(replication_factor.into());
        let config = vec![
            ("compression.type", "zstd"),
            ("auto.offset.reset", "beginning"),
        ];

        let topic = NewTopic {
            name,
            num_partitions: num_partitions.into(),
            replication,
            config,
        };

        match self.admin_client.create_topics([&topic], &opts).await {
            Ok(results_vec) => match &results_vec[0] {
                Ok(_) => {
                    event!(
                        Level::INFO,
                        "Created topic {} with {} partitions, replication factor {}",
                        name,
                        num_partitions,
                        replication_factor
                    );
                    Ok(())
                }
                Err(e) => {
                    event!(Level::ERROR, "Failed to create topic {}, {:?}", e.0, e.1);
                    Err(KafkaError::AdminOp(e.1).into())
                }
            },
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    pub async fn delete_topic(&self, name: &str) -> AdminResult<()> {
        let opts = AdminOptions::new();

        match self.admin_client.delete_topics(&[name], &opts).await {
            Ok(results_vec) => match &results_vec[0] {
                Ok(deleted_name) => {
                    event!(Level::INFO, "Deleted topic {}", deleted_name);
                    Ok(())
                }
                Err(e) => {
                    event!(Level::ERROR, "Failed to delete topic {}, {:?}", e.0, e.1);
                    Err(KafkaError::AdminOp(e.1).into())
                }
            },
            Err(e) => Err(e.into()),
        }
    }
}
