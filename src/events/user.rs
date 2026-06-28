use serde::{Deserialize, Serialize};

use crate::{
    events::EventType,
    publisher::{
        TypedEvent,
        topic::{KeyEvent, TopicEvent},
    },
};

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct UserUpdated {
    pub id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct FriendRequestCreated {
    pub from_username: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct FriendRequestAnswered {
    pub from_username: String,
    pub accepted: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum UserEvent {
    #[serde(rename = "user_updated")]
    UserUpdatedEvent(UserUpdated),
    #[serde(rename = "friend_request_created")]
    FriendRequestCreatedEvent(FriendRequestCreated),
    #[serde(rename = "friend_request_answered")]
    FriendRequestAnsweredEvent(FriendRequestAnswered),
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum UserEventType {
    Updated,
    Friendship,
}

impl TopicEvent for UserEvent {
    fn event_topic(&self) -> crate::publisher::topic::Topic {
        self.event_type().event_topic()
    }
}

impl TopicEvent for UserEventType {
    fn event_topic(&self) -> crate::publisher::topic::Topic {
        match self {
            UserEventType::Updated => "user-updated",
            UserEventType::Friendship => "user-friendship",
        }
    }
}

impl KeyEvent for UserEvent {
    fn event_key(&self) -> Option<Vec<u8>> {
        None
    }
}

impl TypedEvent for UserEvent {
    type EventType = EventType;

    fn event_type(&self) -> Self::EventType {
        EventType::User(match self {
            UserEvent::UserUpdatedEvent(_) => UserEventType::Updated,
            UserEvent::FriendRequestCreatedEvent(_) => UserEventType::Friendship,
            UserEvent::FriendRequestAnsweredEvent(_) => UserEventType::Friendship,
        })
    }
}
