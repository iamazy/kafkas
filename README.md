# kafkas

async kafka client in pure Rust.

## Features

- multiple async runtime (`tokio`, `async-std`, etc.)
- all kafka versions
- compression (`gzip`, `snappy`, `lz4`)

## APIs

- [x] producer
- [ ] consumer
- [ ] streams
- [ ] connect
- [ ] admin client

## Usage

```toml
[dependencies]
kafkas = { git = "https://github.com/iamazy/kafkas", branch = "main" }
```

To get started using kafkas:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<Error>> {
    let mut options = KafkaOptions::new();
    options.client_id("app");
    let kafka_client = Kafka::new("127.0.0.1:50088", options, None, None, TokioExecutor).await?;

    let producer = Producer::new(client, ProducerOptions::default()).await?;

    let (mut tx, mut rx) = futures::channel::mpsc::unbounded();
    tokio::task::spawn(Box::pin(async move {
        while let Some(fut) = rx.next().await {
            if let Err(e) = fut.await {
                error!("{e}");
            }
        }
    }));

    let topic = topic_name("kafka");
    for _ in 0..10000_0000 {
        let record = TestData::new("hello kafka");
        let ret = producer.send(&topic, record).await?;
        let _ = tx.send(ret).await;
    }
    // hold your main thread to prevent exit immediately.
    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

## Examples

Examples can be found in [`examples`](https://github.com/iamazy/kafkas/blob/main/examples).

## Flame graph

<img style="width:800px" src="./benchmark/flamegraph.svg"  alt="flamegraph"/>

## Rust version requirements

The rust version used for `kafkas` development is `1.65`.

## Acknowledgments

- [kafka-protocol-rs](https://github.com/tychedelia/kafka-protocol-rs) : Rust implementation of the [Kafka wire protocol](https://kafka.apache.org/protocol.html).
- [pulsar-rs](https://github.com/streamnative/pulsar-rs) : Rust Client library for Apache Pulsar
- [rskafka](https://github.com/influxdata/rskafka) : A minimal Rust client for Apache Kafka