use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use rdkafka::{
    ClientConfig, Message,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    error::RDKafkaErrorCode,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::Offset,
    util::Timeout,
};
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec};
use tokio::{
    sync::RwLock,
    task::JoinHandle,
    time::{Instant, sleep},
};
use tracing::error;
use uuid::Uuid;

use crate::publisher::{
    EventManager, EventSubscriberHdlrFn, TypedEvent,
    topic::{KeyEvent, Topic, TopicEvent},
};

type SubscriberMap<T> =
    Arc<RwLock<HashMap<<T as TypedEvent>::EventType, EventSubscriberHdlrFn<T>>>>;
type SubscriptionMap<T> = RwLock<HashMap<<T as TypedEvent>::EventType, Topic>>;
type ProducerMap = HashMap<Topic, FutureProducer>;

const DEFAULT_BOOTSTRAP_SERVERS: &str = "localhost:9092";
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);
const SUBSCRIPTION_TIMEOUT: Duration = Duration::from_secs(10);
const TOPIC_PARTITIONS: i32 = 1;
const TOPIC_REPLICATION_FACTOR: i32 = 1;

pub struct KafkaHandler<T: TypedEvent> {
    client_config: ClientConfig,
    admin: AdminClient<DefaultClientContext>,
    consumer: Arc<StreamConsumer>,
    consumer_task: JoinHandle<()>,
    subscribers: SubscriberMap<T>,
    subscriptions: SubscriptionMap<T>,
    producers: RwLock<ProducerMap>,
    start_at_latest: bool,
}

impl<T> KafkaHandler<T>
where
    T: TypedEvent + for<'de> Deserialize<'de> + Send + 'static,
{
    pub async fn new() -> anyhow::Result<Self> {
        let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
            .unwrap_or_else(|_| DEFAULT_BOOTSTRAP_SERVERS.to_owned());
        let group_id = std::env::var("KAFKA_GROUP_ID")
            .unwrap_or_else(|_| format!("devcord-events-{}", Uuid::new_v4()));

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "latest")
            .set(
                "message.timeout.ms",
                DELIVERY_TIMEOUT.as_millis().to_string(),
            );

        Self::with_config(client_config).await
    }

    pub async fn with_config(client_config: ClientConfig) -> anyhow::Result<Self> {
        let start_at_latest = client_config.get("auto.offset.reset") == Some("latest");
        let admin = client_config.create()?;
        let consumer: Arc<StreamConsumer> = Arc::new(client_config.create()?);
        let subscribers: SubscriberMap<T> = Default::default();
        let subscriber_clone = Arc::clone(&subscribers);
        let consumer_clone = Arc::clone(&consumer);

        let consumer_task = tokio::spawn(async move {
            loop {
                let message = match consumer_clone.recv().await {
                    Ok(message) => message,
                    Err(error) => {
                        error!(%error, "Error receiving Kafka event");
                        continue;
                    }
                };
                let Some(payload) = message.payload() else {
                    continue;
                };
                let event = match from_slice::<T>(payload) {
                    Ok(event) => event,
                    Err(error) => {
                        error!(%error, "Error parsing Kafka event");
                        continue;
                    }
                };

                let mut subscribers = subscriber_clone.write().await;
                if let Some(subscriber) = subscribers.get_mut(&event.event_type()) {
                    subscriber(event).await;
                }
            }
        });

        Ok(Self {
            client_config,
            admin,
            consumer,
            consumer_task,
            subscribers,
            subscriptions: Default::default(),
            producers: Default::default(),
            start_at_latest,
        })
    }

    async fn try_create_topic(&self, topic: Topic) -> anyhow::Result<()> {
        let topic_spec = NewTopic::new(
            topic,
            TOPIC_PARTITIONS,
            TopicReplication::Fixed(TOPIC_REPLICATION_FACTOR),
        );
        let results = self
            .admin
            .create_topics(&[topic_spec], &AdminOptions::new())
            .await?;

        for result in results {
            match result {
                Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                Err((topic, error)) => {
                    anyhow::bail!("Error creating Kafka topic {topic}: {error}")
                }
            }
        }

        Ok(())
    }

    async fn refresh_consumer_topics(&self, new_topic: Option<Topic>) -> anyhow::Result<()> {
        let subscriptions = self.subscriptions.read().await;
        let topics: HashSet<Topic> = subscriptions.values().copied().collect();
        drop(subscriptions);

        if topics.is_empty() {
            self.consumer.unsubscribe();
        } else {
            let topics: Vec<Topic> = topics.into_iter().collect();
            self.consumer.subscribe(&topics)?;

            let deadline = Instant::now() + SUBSCRIPTION_TIMEOUT;
            loop {
                let all_topics_assigned = {
                    let assignment = self.consumer.assignment()?;
                    let elements = assignment.elements();
                    let assigned_topics: HashSet<&str> =
                        elements.iter().map(|element| element.topic()).collect();
                    topics.iter().all(|topic| assigned_topics.contains(topic))
                };
                if all_topics_assigned {
                    break;
                }
                if Instant::now() >= deadline {
                    anyhow::bail!(
                        "Kafka consumer was not assigned to topics within {:?}: {}",
                        SUBSCRIPTION_TIMEOUT,
                        topics.join(", ")
                    );
                }
                sleep(Duration::from_millis(25)).await;
            }

            if self.start_at_latest
                && let Some(new_topic) = new_topic
            {
                let assignment = self.consumer.assignment()?;
                let committed = self
                    .consumer
                    .committed_offsets(assignment, Timeout::After(SUBSCRIPTION_TIMEOUT))?;

                for partition in committed.elements() {
                    if partition.topic() == new_topic && partition.offset() == Offset::Invalid {
                        self.consumer.seek(
                            partition.topic(),
                            partition.partition(),
                            Offset::End,
                            Timeout::After(SUBSCRIPTION_TIMEOUT),
                        )?;
                    }
                }
            }
        }

        Ok(())
    }
}

impl<T: TypedEvent> Drop for KafkaHandler<T> {
    fn drop(&mut self) {
        self.consumer_task.abort();
    }
}

#[async_trait]
impl<T> EventManager for KafkaHandler<T>
where
    T: TypedEvent
        + TopicEvent
        + KeyEvent
        + for<'de> Deserialize<'de>
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
        let event_type = event.event_type();
        let topic = event.event_topic();

        let previous_listener = self
            .subscribers
            .write()
            .await
            .insert(event_type.clone(), listener);

        let (previous_topic, topic_was_subscribed) = {
            let mut subscriptions = self.subscriptions.write().await;
            let topic_was_subscribed = subscriptions.values().any(|value| *value == topic);
            let previous_topic = subscriptions.insert(event_type.clone(), topic);
            (previous_topic, topic_was_subscribed)
        };
        if previous_topic != Some(topic) {
            if let Err(error) = self.try_create_topic(topic).await {
                let mut subscribers = self.subscribers.write().await;
                if let Some(previous_listener) = previous_listener {
                    subscribers.insert(event_type.clone(), previous_listener);
                } else {
                    subscribers.remove(&event_type);
                }

                let mut subscriptions = self.subscriptions.write().await;
                if let Some(previous_topic) = previous_topic {
                    subscriptions.insert(event_type, previous_topic);
                } else {
                    subscriptions.remove(&event_type);
                }
                return Err(error);
            }

            let new_topic = (!topic_was_subscribed).then_some(topic);
            if let Err(error) = self.refresh_consumer_topics(new_topic).await {
                let mut subscribers = self.subscribers.write().await;
                if let Some(previous_listener) = previous_listener {
                    subscribers.insert(event_type.clone(), previous_listener);
                } else {
                    subscribers.remove(&event_type);
                }

                let mut subscriptions = self.subscriptions.write().await;
                if let Some(previous_topic) = previous_topic {
                    subscriptions.insert(event_type, previous_topic);
                } else {
                    subscriptions.remove(&event_type);
                }
                return Err(error);
            }
        }

        Ok(())
    }

    async fn unsubscribe(&self, event: Self::Event) -> anyhow::Result<()> {
        let event_type = event.event_type();
        self.subscribers.write().await.remove(&event_type);

        if self
            .subscriptions
            .write()
            .await
            .remove(&event_type)
            .is_some()
        {
            self.refresh_consumer_topics(None).await?;
        }

        Ok(())
    }

    async fn notify(&self, event: Self::Event) -> anyhow::Result<()> {
        let topic = event.event_topic();
        let payload = to_vec(&event)?;
        let key = event.event_key();

        let producer = {
            if let Some(producer) = self.producers.read().await.get(topic).cloned() {
                producer
            } else {
                let mut producers = self.producers.write().await;
                match producers.get(topic).cloned() {
                    Some(producer) => producer,
                    None => {
                        self.try_create_topic(topic).await?;
                        let producer: FutureProducer = self.client_config.create()?;
                        producers.insert(topic, producer.clone());
                        producer
                    }
                }
            }
        };

        let mut record: FutureRecord<'_, [u8], [u8]> =
            FutureRecord::to(topic).payload(payload.as_slice());
        if let Some(key) = key.as_deref() {
            record = record.key(key);
        }

        producer
            .send(record, Timeout::After(DELIVERY_TIMEOUT))
            .await
            .map_err(|(error, _)| error)?;

        Ok(())
    }
}
