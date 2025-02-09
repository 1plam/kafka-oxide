use rdkafka::client::DefaultClientContext;
use rdkafka::admin::AdminClient as RDKafkaAdminClient;
use std::sync::Arc;
use crate::error::Error;

pub type DefaultAdminClient = RDKafkaAdminClient<DefaultClientContext>;
pub type AdminResult<T> = Result<T, Error>;

/// Admin client for managing Kafka topics and configurations
#[derive(Clone)]
pub struct AdminClient {
    pub(crate) admin_client: Arc<DefaultAdminClient>,
}