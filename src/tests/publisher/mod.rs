use std::time::Duration;

use tokio::{
    sync::mpsc::{self, Receiver},
    time::timeout,
};

use crate::{
    events::{
        Event,
        auth::{AuthEvent, UserCreated},
        message::{MessageEvent, MessageSent},
        user::{UserEvent, UserUpdated},
    },
    publisher::EventManager,
};

#[cfg(feature = "fluvio")]
mod fluvio_handler;
#[cfg(feature = "kafka")]
mod kafka_handler;

async fn subscribe_receiver<T: EventManager<Event = Event>>(
    publisher: &T,
    event: &Event,
) -> anyhow::Result<Receiver<Event>> {
    let (tx, rx) = mpsc::channel::<Event>(1);

    publisher
        .subscribe(
            event.clone(),
            Box::new(move |event| {
                let tx = tx.clone();
                Box::pin(async move {
                    tx.send(event).await.unwrap();
                })
            }),
        )
        .await?;

    Ok(rx)
}

pub async fn test_notify<T: EventManager<Event = Event>>(publisher: T) -> anyhow::Result<()> {
    let event = Event::MessageEvent(MessageEvent::MessageSentEvent(MessageSent {
        channel_id: String::from("Test"),
        sender: String::from("Test"),
        message: String::from("Test"),
    }));

    let mut rx = subscribe_receiver(&publisher, &event).await?;

    publisher.notify(event.clone()).await?;

    let Some(received_event) = rx.recv().await else {
        return Err(anyhow::anyhow!("Event not received when it should"));
    };
    assert_eq!(event, received_event);

    Ok(())
}

pub async fn test_subscribe_only_chosen_events<T: EventManager<Event = Event>>(
    publisher: T,
) -> anyhow::Result<()> {
    let event_1 = Event::UserEvent(UserEvent::UserUpdatedEvent(UserUpdated {
        id: String::from("Test"),
    }));

    let event_2 = Event::AuthEvent(AuthEvent::UserSignedUpEvent(UserCreated {
        id: String::from("Test"),
        username: String::from("Test"),
    }));

    let mut rx = subscribe_receiver(&publisher, &event_1).await?;

    publisher.notify(event_1.clone()).await?;

    let received_event = rx.recv().await;
    assert_eq!(Some(event_1), received_event);
    publisher.notify(event_2.clone()).await?;
    let received_event = timeout(Duration::from_secs(1), rx.recv()).await.ok();
    assert_eq!(None, received_event);

    Ok(())
}

pub async fn test_unsubscribe<T: EventManager<Event = Event>>(publisher: T) -> anyhow::Result<()> {
    let event_1 = Event::UserEvent(UserEvent::UserUpdatedEvent(UserUpdated {
        id: String::from("Test"),
    }));

    let event_2 = Event::UserEvent(UserEvent::UserUpdatedEvent(UserUpdated {
        id: String::from("Test"),
    }));

    let mut rx = subscribe_receiver(&publisher, &event_1).await?;

    publisher.notify(event_1.clone()).await?;

    let received_event = rx.recv().await;
    assert_eq!(Some(event_1.clone()), received_event);
    publisher.unsubscribe(event_1).await?;
    publisher.notify(event_2.clone()).await?;
    let received_event = timeout(Duration::from_secs(1), rx.recv()).await.ok();
    assert_eq!(Some(None), received_event);

    Ok(())
}

pub async fn test_override_subscribe<T: EventManager<Event = Event>>(
    publisher: T,
) -> anyhow::Result<()> {
    let event_1 = Event::UserEvent(UserEvent::UserUpdatedEvent(UserUpdated {
        id: String::from("Test"),
    }));

    let event_2 = Event::UserEvent(UserEvent::UserUpdatedEvent(UserUpdated {
        id: String::from("Test"),
    }));

    let mut rx_1 = subscribe_receiver(&publisher, &event_1).await?;

    publisher.notify(event_1.clone()).await?;

    let received_event = rx_1.recv().await;
    assert_eq!(Some(event_1.clone()), received_event);

    let (tx_2, mut rx_2) = mpsc::channel::<Event>(1);

    publisher
        .subscribe(
            event_1.clone(),
            Box::new(move |event| {
                let tx = tx_2.clone();
                Box::pin(async move {
                    tx.send(event).await.unwrap();
                })
            }),
        )
        .await?;

    publisher.notify(event_2.clone()).await?;
    let received_event = timeout(Duration::from_secs(1), rx_1.recv()).await.ok();
    assert_eq!(Some(None), received_event);
    let received_event = rx_2.recv().await;
    assert_eq!(Some(event_2), received_event);

    Ok(())
}
