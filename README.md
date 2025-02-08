## kafka-oxide

An ergonomic and type-safe Rust wrapper for Apache Kafka and Redpanda, designed for a seamless developer experience with
safety in mind.

> [!IMPORTANT]
> This project is currently under development and may not work as intended. Stay tuned for updates and improvements.

## Planned Key Features

- 🦀 **Type-safe Interface**: Leverage Rust's type system for compile-time message validation
- 🔗 **Robust Connection Management**: Automatic reconnection handling with configurable retry policies
- 📝 **Observability**: Structured logging with `tracing` and detailed metrics
- ⏳ **Modern Async Runtime**: Built on `tokio` with full async/await support
- ⚠️ **Error Handling**: Rich error types with detailed context and recovery suggestions
- 🌐 **Unified Protocol**: Seamless support for both Apache Kafka and Redpanda
- 🔒 **Security**: Built-in support for SASL, SSL/TLS, and custom authentication mechanisms
- 🎯 **Zero-Copy**: Optimized message handling with minimal memory overhead

## Installation

Add `kafka-oxide` to your `Cargo.toml`:

```toml
[dependencies]
kafka-oxide = "0.1"
```


## Quick Start

### Producer Example

Here's how you can publish a message to Kafka using `kafka-oxide`:

```rust
use kafka_oxide::{Producer, Record};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer = Producer::builder()
        .bootstrap_servers(&["localhost:9092"])
        .client_id("my-app")
        .build()?;

    let record = Record::new("my-topic", b"Hello, Kafka!".to_vec());
    producer.send(record).await?;
    
    Ok(())
}
```

### Consumer Example

To consume messages from Kafka, use the Consumer struct:

```rust
use kafka_oxide::{Consumer, Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let consumer = Consumer::builder()
        .bootstrap_servers(&["localhost:9092"])
        .group_id("my-consumer-group")
        .build()?;

    consumer.subscribe(&["my-topic"])?;

    while let Some(message) = consumer.next().await {
        let message = message?;
        println!("Received message: {:?}", message);
        
        consumer
            .commit_message(&message, Default::default())
            .await?;
    }

    Ok(())
}
```

_This project is licensed under [Apache License, Version 2.0](.github/LICENSE)_
