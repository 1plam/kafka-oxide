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

// Helper conversion for Option<RDKafkaRespErr>
fn convert_kafka_error(err: Option<RDKafkaRespErr>) -> Option<KafkaError> {
    err.map(KafkaError::from)
}

/// Metadata information about a Kafka cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMetadata {
    /// ID of the broker originating this metadata
    pub orig_broker_id: i32,
    /// Hostname of the broker originating this metadata
    pub orig_broker_name: String,
    /// Metadata information for all the brokers in the cluster
    pub brokers: Vec<Broker>,
    /// Metadata information for all the topics in the cluster
    pub topics: Vec<Topic>,
}

impl ClusterMetadata {
    /// Get a sorted list of the names of all topics in the cluster
    pub fn topic_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.topics.iter().map(|t| t.name.clone()).collect();
        names.sort();
        names
    }

    /// Find a topic by name
    pub fn find_topic(&self, name: &str) -> Option<&Topic> {
        self.topics.iter().find(|t| t.name == name)
    }

    /// Get the number of brokers in the cluster
    pub fn broker_count(&self) -> usize {
        self.brokers.len()
    }

    /// Get total number of partitions across all topics
    pub fn total_partitions(&self) -> usize {
        self.topics.iter().map(|t| t.partitions.len()).sum()
    }

    /// Check if a topic exists
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

/// Metadata information about a Kafka broker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Broker {
    /// Broker ID
    pub id: i32,
    /// Broker hostname
    pub hostname: String,
    /// Broker port
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

/// Metadata information about a Kafka topic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topic {
    /// Topic name
    pub name: String,
    /// Partition metadata information
    pub partitions: Vec<Partition>,
    /// Topic error, if any
    pub error: Option<KafkaError>,
}

impl Topic {
    /// Get the number of partitions
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Find a partition by ID
    pub fn find_partition(&self, id: i32) -> Option<&Partition> {
        self.partitions.iter().find(|p| p.id == id)
    }

    /// Check if the topic has any errors
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

/// Metadata information about a Kafka partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    /// Partition ID
    pub id: i32,
    /// Leader broker ID
    pub leader: i32,
    /// Partition error, if any
    pub error: Option<KafkaError>,
    /// Replica broker IDs
    pub replicas: Vec<i32>,
    /// In-sync replica broker IDs
    pub in_sync_replicas: Vec<i32>,
}

impl Partition {
    /// Check if a broker is a replica for this partition
    pub fn has_replica(&self, broker_id: i32) -> bool {
        self.replicas.contains(&broker_id)
    }

    /// Check if a broker is an in-sync replica
    pub fn is_in_sync(&self, broker_id: i32) -> bool {
        self.in_sync_replicas.contains(&broker_id)
    }

    /// Get the replication factor
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
