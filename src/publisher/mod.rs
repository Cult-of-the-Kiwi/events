use std::{hash::Hash, pin::Pin};

use async_trait::async_trait;

pub mod topic;

type EventSubscriberHdlrFn<T> =
    Box<dyn (FnMut(T) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>) + Send + Sync>;

pub trait TypedEvent {
    type EventType: Eq + Hash + Clone + Send + Sync;
    fn event_type(&self) -> Self::EventType;
}

#[async_trait]
pub trait EventManager: Send + Sync {
    type Event: TypedEvent;
    async fn subscribe(
        &self,
        event: Self::Event,
        listener: EventSubscriberHdlrFn<Self::Event>,
    ) -> anyhow::Result<()>;
    async fn unsubscribe(&self, event: Self::Event) -> anyhow::Result<()>;
    async fn notify(&self, event: Self::Event) -> anyhow::Result<()>;
}
