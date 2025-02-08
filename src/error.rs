use rdkafka::error::KafkaError;
use std::array::TryFromSliceError;
use thiserror::Error;

/// Main error type for kafka-oxide operations
#[derive(Error, Debug)]
pub enum Error {
    /// Underlying Kafka error
    #[error("Kafka error: {0}")]
    Kafka(#[from] KafkaError),

    /// Record serialization/deserialization error
    #[error("Record error: {0}")]
    Record(#[from] RecordError),

    /// Invalid configuration
    #[error("Configuration error: {0}")]
    Config(String),

    /// Topic validation error
    #[error("Topic validation error: {0}")]
    TopicValidation(String),

    /// Admin operation error
    #[error("Admin operation error: {0}")]
    Admin(String),

    /// Connection error
    #[error("Connection error: {0}")]
    Connection(String),

    /// Timeout error
    #[error("Operation timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// Generic error with message
    #[error("{0}")]
    Other(String),
}

/// Errors specific to record operations
#[derive(Error, Debug)]
pub enum RecordError {
    /// Error deserializing record key
    #[error("Failed to deserialize key: {0}")]
    KeyDeserialize(#[from] TryFromSliceError),

    /// Error deserializing record value
    #[error("Failed to deserialize value: {0}")]
    ValueDeserialize(String),

    /// Error serializing record
    #[error("Failed to serialize record: {0}")]
    Serialize(String),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Missing required field
    #[error("Missing required field: {0}")]
    MissingField(String),
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Other(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Other(s.to_owned())
    }
}