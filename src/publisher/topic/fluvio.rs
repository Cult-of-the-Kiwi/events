use async_trait::async_trait;
use fluvio::{
    Fluvio, Offset, RecordKey, TopicProducer, consumer::ConsumerConfigExtBuilder,
    metadata::topic::TopicSpec, spu::SpuSocketPool,
};
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec};
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tokio::{sync::RwLock, task::JoinHandle};
use tokio_stream::StreamExt;
use tracing::error;

use crate::publisher::{
    EventManager, EventSubscriberHdlrFn, TypedEvent,
    topic::{Topic, TopicEvent},
};

pub use super::KeyEvent;

type SubscriberMap<T> =
    Arc<RwLock<HashMap<<T as TypedEvent>::EventType, EventSubscriberHdlrFn<T>>>>;
type ProducerMap = HashMap<Topic, TopicProducer<SpuSocketPool>>;
type ReceiversMap = Arc<RwLock<HashMap<Topic, (usize, JoinHandle<()>)>>>;

const CONSUMER_OFFSET: &str = "consumer-auto";

#[derive(Debug, Error)]
enum Error {
    #[error("Error creating fluvio topic: {0}")]
    ErrorCreatingTopic(anyhow::Error),
    #[error("Error acquiring fluvio topics: {0}")]
    ErrorAcquiringTopics(anyhow::Error),
    #[error("Error connecting to fluvio: {0}")]
    ErrorConnectingToFluvio(anyhow::Error),
    #[error("Error creating fluvio producer: {0}")]
    ErrorCreatingProducer(anyhow::Error),
    #[error("Error creating fluvio consumer: {0}")]
    ErrorCreatingConsumer(anyhow::Error),
    #[error("Internal handler error: {0}")]
    InternalError(anyhow::Error),
}

pub struct FluvioHandler<T: TypedEvent> {
    fluvio: Fluvio,
    subscribers: SubscriberMap<T>,
    receivers: ReceiversMap,
    producers: RwLock<ProducerMap>,
}

impl<T: TypedEvent> FluvioHandler<T> {
    pub async fn new() -> anyhow::Result<Self> {
        Ok(Self {
            fluvio: Fluvio::connect()
                .await
                .map_err(|e| Error::ErrorConnectingToFluvio(e))?,
            subscribers: Default::default(),
            receivers: Default::default(),
            producers: Default::default(),
        })
    }

    #[cfg(test)]
    pub(crate) async fn reset_fluvio(&self) -> anyhow::Result<()> {
        let admin = self.fluvio.admin().await;

        let topics = admin
            .all::<TopicSpec>()
            .await
            .map_err(|e| Error::ErrorAcquiringTopics(e))?;

        for topic in topics {
            let name = topic.name;
            tracing::debug!("Deleting topic: {}", name);
            admin.delete::<TopicSpec>(&name).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl<T> EventManager for FluvioHandler<T>
where
    T: TypedEvent
        + TopicEvent
        + KeyEvent
        + for<'a> Deserialize<'a>
        + Serialize
        + Send
        + Sync
        + 'static,
{
    type Event = T;

    async fn subscribe(
        &self,
        event: Self::Event,
        listener: EventSubscriberHdlrFn<Self::Event>,
    ) -> anyhow::Result<()> {
        let mut lock = self.subscribers.write().await;
        lock.insert(event.event_type(), listener);

        let mut lock = self.receivers.write().await;
        lock.entry(event.event_topic())
            .or_insert(
                new_topic_reader::<T>(&event, &self.subscribers, &self.fluvio)
                    .await
                    .map_err(|e| Error::InternalError(e))?,
            )
            .0 += 1;
        Ok(())
    }

    async fn unsubscribe(&self, event: Self::Event) -> anyhow::Result<()> {
        let mut lock = self.subscribers.write().await;
        lock.remove(&event.event_type());

        let mut lock = self.receivers.write().await;
        if let Some(counter) = lock.get_mut(&event.event_topic()) {
            counter.0 -= 1;
            if counter.0 == 0 {
                counter.1.abort();
                lock.remove(&event.event_topic());
            }
        }
        Ok(())
    }

    async fn notify(&self, event: Self::Event) -> anyhow::Result<()> {
        let mut binding = self.producers.write().await;
        let producer = binding.entry(event.event_topic()).or_insert({
            try_create_topic(&self.fluvio, event.event_topic()).await?;
            self.fluvio
                .topic_producer(event.event_topic())
                .await
                .map_err(|e| Error::ErrorCreatingProducer(e))?
        });
        let key = event.event_key().map_or(RecordKey::NULL, RecordKey::from);
        producer
            .send(key, to_vec(&event)?)
            .await
            .map_err(|e| Error::InternalError(e))?;
        Ok(())
    }
}

async fn new_topic_reader<T>(
    event: &T,
    subscribers: &SubscriberMap<T>,
    fluvio: &Fluvio,
) -> anyhow::Result<(usize, JoinHandle<()>)>
where
    T: TypedEvent + TopicEvent + for<'a> Deserialize<'a> + 'static + Send,
{
    let subscribers = subscribers.clone();
    let topic = event.event_topic();

    try_create_topic(fluvio, &topic).await?;

    //FIXME This should be modificable from the outside
    let consumer_config = ConsumerConfigExtBuilder::default()
        .topic(topic)
        .offset_start(Offset::end())
        .offset_consumer(CONSUMER_OFFSET)
        .build()
        .map_err(|e| Error::ErrorCreatingConsumer(e))?;

    let mut consumer_stream = fluvio
        .consumer_with_config(consumer_config)
        .await
        .map_err(|e| Error::ErrorCreatingConsumer(e))?;

    let event_type = event.event_type();
    let handle = tokio::spawn(async move {
        while let Some(Ok(record)) = consumer_stream.next().await {
            let Ok(event) =
                from_slice::<T>(record.value()).map_err(|e| error!("Error parsing event: {}", e))
            else {
                continue;
            };

            let mut map = subscribers.write().await;
            let Some(subscriber) = map.get_mut(&event_type) else {
                break;
            };

            subscriber(event).await;
        }
    });
    Ok((0, handle))
}

async fn try_create_topic(fluvio: &Fluvio, topic: &str) -> anyhow::Result<()> {
    let admin = fluvio.admin().await;

    let topics = admin
        .all::<TopicSpec>()
        .await
        .map_err(|e| Error::ErrorAcquiringTopics(e))?;
    let topic_names = topics
        .iter()
        .map(|topic| topic.name.clone())
        .collect::<Vec<String>>();

    if !topic_names.contains(&topic.to_string()) {
        let topic_spec = TopicSpec::new_computed(1, 1, None);
        admin
            .create(topic.to_string(), false, topic_spec)
            .await
            .map_err(|e| Error::ErrorCreatingTopic(e))?
    }

    Ok(())
}
