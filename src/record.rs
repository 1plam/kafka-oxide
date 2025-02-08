use crate::error::{Error, RecordError};
use rdkafka::{message::OwnedHeaders, producer::FutureRecord, Timestamp};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// A record that can be sent to Kafka
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Topic name
    topic: String,
    /// Optional message key
    key: Option<Vec<u8>>,
    /// Message payload
    payload: Vec<u8>,
    /// Optional message headers
    #[serde(skip)]
    headers: Option<OwnedHeaders>,
    /// Creation timestamp
    #[serde(skip)]
    created_timestamp: Option<Timestamp>,
}

impl Default for Record {
    fn default() -> Self {
        Self {
            topic: String::new(),
            key: None,
            payload: Vec::new(),
            headers: None,
            created_timestamp: Some(Timestamp::now()),
        }
    }
}

impl Record {
    pub fn new(topic: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            topic: topic.into(),
            key: None,
            payload,
            headers: None,
            created_timestamp: Some(Timestamp::now()),
        }
    }

    pub fn with_string(topic: impl Into<String>, payload: impl AsRef<str>) -> Self {
        Self::s(topic, payload.as_ref().as_bytes())
    }

    pub fn with_json<T: Serialize>(topic: impl Into<String>, payload: &T) -> Result<Self, Error> {
        Ok(Self::new(
            topic,
            serde_json::to_vec(payload).map_err(RecordError::from)?,
        ))
    }

    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn with_string_key(self, key: impl AsRef<str>) -> Self {
        self.with_key(key.as_ref().as_bytes())
    }

    pub fn with_json_key<T: Serialize>(self, key: &T) -> Result<Self, Error> {
        Ok(self.with_key(serde_json::to_vec(key).map_err(RecordError::from)?))
    }

    pub fn with_headers(mut self, headers: OwnedHeaders) -> Self {
        self.headers = Some(headers);
        self
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn headers(&self) -> Option<&OwnedHeaders> {
        self.headers.as_ref()
    }

    pub fn created_at(&self) -> SystemTime {
        match self.created_timestamp.unwrap_or_else(Timestamp::now) {
            Timestamp::CreateTime(ms) => {
                SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(ms as u64)
            }
            Timestamp::LogAppendTime(ms) => {
                SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(ms as u64)
            }
            Timestamp::NotAvailable => SystemTime::now(),
        }
    }

    /// Parse the payload as UTF-8 string
    pub fn payload_string(&self) -> Result<String, Error> {
        String::from_utf8(self.payload.clone())
            .map_err(|e| RecordError::ValueDeserialize(e.to_string()).into())
    }

    /// Parse the payload as JSON
    pub fn payload_json<T>(&self) -> Result<T, Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        serde_json::from_slice(&self.payload)
            .map_err(RecordError::from)
            .map_err(Error::from)
    }
}

impl<'a> From<&'a Record> for FutureRecord<'a, Vec<u8>, Vec<u8>> {
    fn from(r: &'a Record) -> Self {
        FutureRecord {
            topic: &r.topic,
            partition: None,
            payload: Some(&r.payload),
            key: r.key.as_ref(),
            timestamp: r
                .created_timestamp
                .unwrap_or_else(Timestamp::now)
                .to_millis(),
            headers: r.headers.clone(),
        }
    }
}
