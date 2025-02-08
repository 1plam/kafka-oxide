use rdkafka::metadata::{Metadata, MetadataBroker, MetadataTopic};
use rdkafka::statistics::Partition;
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

pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
    pub error: Option<KafkaError>,
}

impl From<&MetadataTopic> for Topic {
    fn from(t: &MetadataTopic) -> Self {
        Self {
            name: t.name().to_owned(),
            partitions: t.partitions().iter().map(Into::into).collect(),
            error: convert_kafka_error(t.error()),
        }
    }
}


pub struct ClusterMetadata {
    pub orig_broker_id: i32,
    pub orig_broker_name: String,
    pub brokers: Vec<Broker>,
    pub topics: Vec<Topic>,
}

pub struct Broker {
    pub id: i32,
    pub hostname: String,
    pub port: u16,
}

impl From<Metadata> for ClusterMetadata {
    fn from(m: Metadata) -> Self {
        Self {
            orig_broker_id: m.orig_broker_id(),
            orig_broker_name: m.orig_broker_name().to_owned(),
            brokers: m.brokers().iter().map(Into::into).collect(),
            topics: m.topics().iter().map(Into::into).collect(),
        }
    }
}

impl From<&MetadataBroker> for Broker {
    fn from(b: &MetadataBroker) -> Self {
        Self {
            id: b.id(),
            hostname: b.host().to_owned(),
            port: b.port() as u16,
        }
    }
}