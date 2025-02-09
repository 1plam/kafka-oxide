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

impl std::fmt::Debug for AdminClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminClient")
            .field("admin_client", &"<admin_client>")
            .finish()
    }
}