use std::time::Duration;

use rdkafka::ClientConfig;
use serial_test::serial;
use tokio::time::sleep;
use uuid::Uuid;

use crate::{
    events::Event,
    publisher::topic::kafka::KafkaHandler,
    tests::publisher::{
        test_notify, test_override_subscribe, test_subscribe_only_chosen_events, test_unsubscribe,
    },
};

const TEST_TIMEOUT: u64 = 400;

async fn test<T: AsyncFnOnce(KafkaHandler<Event>) -> anyhow::Result<()>>(
    test: T,
) -> anyhow::Result<()> {
    sleep(Duration::from_millis(TEST_TIMEOUT)).await;
    let bootstrap_servers =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", bootstrap_servers)
        .set(
            "group.id",
            format!("devcord-events-test-{}", Uuid::new_v4()),
        )
        .set("auto.offset.reset", "latest")
        .set("message.timeout.ms", "5000");
    let handler: KafkaHandler<Event> = KafkaHandler::with_config(client_config).await?;

    test(handler).await
}

#[tokio::test]
#[serial]
pub async fn kafka_test_notify() -> anyhow::Result<()> {
    test(test_notify).await
}

#[tokio::test]
#[serial]
pub async fn kafka_test_subscribe_only_chosen_events() -> anyhow::Result<()> {
    test(test_subscribe_only_chosen_events).await
}

#[tokio::test]
#[serial]
pub async fn kafka_test_unsubscribe() -> anyhow::Result<()> {
    test(test_unsubscribe).await
}

#[tokio::test]
#[serial]
pub async fn kafka_test_override_subscribe() -> anyhow::Result<()> {
    test(test_override_subscribe).await
}
