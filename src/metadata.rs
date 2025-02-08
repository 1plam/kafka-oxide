use rdkafka::types::RDKafkaRespErr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaError(i32);

impl From<RDKafkaRespErr> for KafkaError {
    fn from(err: RDKafkaRespErr) -> Self {
        KafkaError(err as i32)
    }
}

fn convert_kafka_error(err: Option<RDKafkaRespErr>) -> Option<KafkaError> {
    err.map(KafkaError::from)
}
