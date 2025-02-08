use rdkafka::{
    metadata::{Metadata, MetadataBroker, MetadataPartition, MetadataTopic},
    types::RDKafkaRespErr,
};
use serde::{Deserialize, Serialize};
use std::convert::From;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMetadata {
    pub orig_broker_id: i32,
    pub orig_broker_name: String,
    pub brokers: Vec<Broker>,
    pub topics: Vec<Topic>,
}

impl ClusterMetadata {
    pub fn topic_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.topics.iter().map(|t| t.name.clone()).collect();
        names.sort();
        names
    }

    pub fn find_topic(&self, name: &str) -> Option<&Topic> {
        self.topics.iter().find(|t| t.name == name)
    }

    pub fn broker_count(&self) -> usize {
        self.brokers.len()
    }

    pub fn total_partitions(&self) -> usize {
        self.topics.iter().map(|t| t.partitions.len()).sum()
    }

    pub fn has_topic(&self, name: &str) -> bool {
        self.topics.iter().any(|t| t.name == name)
    }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Broker {
    pub id: i32,
    pub hostname: String,
    pub port: u16,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
    pub error: Option<KafkaError>,
}

impl Topic {
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn find_partition(&self, id: i32) -> Option<&Partition> {
        self.partitions.iter().find(|p| p.id == id)
    }

    pub fn has_error(&self) -> bool {
        self.error.is_some() || self.partitions.iter().any(|p| p.error.is_some())
    }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    pub id: i32,
    pub leader: i32,
    pub error: Option<KafkaError>,
    pub replicas: Vec<i32>,
    pub in_sync_replicas: Vec<i32>,
}

impl Partition {
    pub fn has_replica(&self, broker_id: i32) -> bool {
        self.replicas.contains(&broker_id)
    }

    pub fn is_in_sync(&self, broker_id: i32) -> bool {
        self.in_sync_replicas.contains(&broker_id)
    }

    pub fn replication_factor(&self) -> usize {
        self.replicas.len()
    }
}

impl From<&MetadataPartition> for Partition {
    fn from(p: &MetadataPartition) -> Self {
        Self {
            id: p.id(),
            leader: p.leader(),
            error: convert_kafka_error(p.error()),
            replicas: p.replicas().to_vec(),
            in_sync_replicas: p.isr().to_vec(),
        }
    }
}