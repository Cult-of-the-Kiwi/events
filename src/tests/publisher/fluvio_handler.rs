use std::time::Duration;

use serial_test::serial;
use tokio::time::sleep;

use crate::{
    events::Event,
    publisher::topic::fluvio::FluvioHandler,
    tests::publisher::{
        test_notify, test_override_subscribe, test_subscribe_only_chosen_events, test_unsubscribe,
    },
};

const TEST_TIMEOUT: u64 = 400;

async fn test<T: AsyncFnOnce(FluvioHandler<Event>) -> anyhow::Result<()>>(
    test: T,
) -> anyhow::Result<()> {
    sleep(Duration::from_millis(TEST_TIMEOUT)).await;
    let handler: FluvioHandler<Event> = FluvioHandler::new().unwrap();
    handler.reset_fluvio().await.unwrap();

    test(handler).await
}

#[tokio::test]
#[serial]
pub async fn fluvio_test_notify() -> anyhow::Result<()> {
    test(test_notify).await
}

#[tokio::test]
#[serial]
pub async fn fluvio_test_subscribe_only_chosen_events() -> anyhow::Result<()> {
    test(test_subscribe_only_chosen_events).await
}

#[tokio::test]
#[serial]
pub async fn fluvio_test_unsubscribe() -> anyhow::Result<()> {
    test(test_unsubscribe).await
}

#[tokio::test]
#[serial]
pub async fn fluvio_test_override_subscribe() -> anyhow::Result<()> {
    test(test_override_subscribe).await
}
