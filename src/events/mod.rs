use serde::{Deserialize, Serialize};

use crate::{
    events::{
        auth::{AuthEvent, AuthEventType},
        group::{GroupEvent, GroupEventType},
        message::{MessageEvent, MessageEventType},
        user::{UserEvent, UserEventType},
    },
    publisher::{
        TypedEvent,
        topic::{KeyEvent, TopicEvent},
    },
};

pub mod auth;
pub mod group;
pub mod message;
pub mod user;

trait FullEvent: KeyEvent + TopicEvent + TypedEvent {}

impl<T> FullEvent for T where T: TopicEvent + KeyEvent + TypedEvent {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "service")]
pub enum Event {
    #[serde(rename = "user")]
    UserEvent(UserEvent),
    #[serde(rename = "auth")]
    AuthEvent(AuthEvent),
    #[serde(rename = "message")]
    MessageEvent(MessageEvent),
    #[serde(rename = "group")]
    GroupEvent(GroupEvent),
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum EventType {
    User(UserEventType),
    Auth(AuthEventType),
    Message(MessageEventType),
    Group(GroupEventType),
}

impl TopicEvent for EventType {
    fn event_topic(&self) -> crate::publisher::topic::Topic {
        self.inner().event_topic()
    }
}

impl TopicEvent for Event {
    fn event_topic(&self) -> crate::publisher::topic::Topic {
        self.inner().event_topic()
    }
}

impl KeyEvent for Event {
    fn event_key(&self) -> Option<Vec<u8>> {
        self.inner().event_key()
    }
}

impl TypedEvent for Event {
    type EventType = EventType;

    fn event_type(&self) -> Self::EventType {
        self.inner().event_type()
    }
}

impl Event {
    fn inner(&self) -> &dyn FullEvent<EventType = EventType> {
        match self {
            Event::UserEvent(event) => event,
            Event::AuthEvent(event) => event,
            Event::MessageEvent(event) => event,
            Event::GroupEvent(event) => event,
        }
    }
}

impl EventType {
    fn inner(&self) -> &dyn TopicEvent {
        match self {
            EventType::User(event_type) => event_type,
            EventType::Auth(event_type) => event_type,
            EventType::Message(event_type) => event_type,
            EventType::Group(event_type) => event_type,
        }
    }
}
