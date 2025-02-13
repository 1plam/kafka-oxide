use crate::error::RecordError;
use rdkafka::message::BorrowedHeaders;
use rdkafka::{
    consumer::ConsumerContext as RDKafkaConsumerContext,
    message::{BorrowedMessage, Message},
    ClientContext,
};

#[derive(Clone)]
pub struct ConsumerContext;

impl ClientContext for ConsumerContext {}
impl RDKafkaConsumerContext for ConsumerContext {}

#[derive(Debug)]
pub struct ConsumerMessage<'a> {
    inner: BorrowedMessage<'a>,
}

impl<'a> ConsumerMessage<'a> {
    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    pub fn partition(&self) -> i32 {
        self.inner.partition()
    }

    pub fn offset(&self) -> i64 {
        self.inner.offset()
    }

    pub fn timestamp(&self) -> rdkafka::Timestamp {
        self.inner.timestamp()
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.inner.key()
    }

    pub fn payload(&self) -> Option<&[u8]> {
        self.inner.payload()
    }

    pub fn headers(&self) -> Option<&BorrowedHeaders> {
        self.inner.headers()
    }

    pub fn payload_string(&self) -> Result<String, RecordError> {
        let payload = self
            .payload()
            .ok_or_else(|| RecordError::MissingField("payload".to_string()))?;

        String::from_utf8(payload.to_vec())
            .map_err(|e| RecordError::ValueDeserialize(e.to_string()).into())
    }

    pub fn payload_json(&self) -> Result<serde_json::Value, RecordError> {
        let payload = self
            .payload()
            .ok_or_else(|| RecordError::MissingField("payload".to_string()))?;

        serde_json::from_slice(payload).map_err(|e| RecordError::Json(e).into())
    }

    pub fn inner(&self) -> &BorrowedMessage<'a> {
        &self.inner
    }
}

impl<'a> From<BorrowedMessage<'a>> for ConsumerMessage<'a> {
    fn from(msg: BorrowedMessage<'a>) -> Self {
        Self { inner: msg }
    }
}
