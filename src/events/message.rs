use serde::{Deserialize, Serialize};

use crate::{
    events::EventType,
    publisher::{
        TypedEvent,
        topic::{KeyEvent, TopicEvent},
    },
};

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageSent {
    pub channel_id: String,
    pub sender: String,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum MessageEvent {
    #[serde(rename = "message_sent")]
    MessageSentEvent(MessageSent),
}

#[derive(Clone, Copy, Hash, PartialEq, PartialOrd, Ord, Eq)]
pub enum MessageEventType {
    Sent,
}

impl TopicEvent for MessageEvent {
    fn event_topic(&self) -> crate::publisher::topic::Topic {
        self.event_type().event_topic()
    }
}

impl TopicEvent for MessageEventType {
    fn event_topic(&self) -> crate::publisher::topic::Topic {
        match self {
            MessageEventType::Sent => "message-sent",
        }
    }
}

impl KeyEvent for MessageEvent {
    fn event_key(&self) -> Option<Vec<u8>> {
        match self {
            MessageEvent::MessageSentEvent(_) => None,
        }
    }
}

impl TypedEvent for MessageEvent {
    type EventType = EventType;

    fn event_type(&self) -> Self::EventType {
        EventType::Message(match self {
            MessageEvent::MessageSentEvent(_) => MessageEventType::Sent,
        })
    }
}
