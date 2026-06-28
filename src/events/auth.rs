use serde::{Deserialize, Serialize};

use crate::{
    events::EventType,
    publisher::{
        TypedEvent,
        topic::{KeyEvent, TopicEvent},
    },
};

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct UserCreated {
    pub id: String,
    pub username: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct UserLoggedIn {
    pub id: String,
    pub username: String,
    pub login_time: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct UserLoggedOut {
    pub id: String,
    pub logout_time: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum AuthEvent {
    #[serde(rename = "user_signed_up")]
    UserSignedUpEvent(UserCreated),
    #[serde(rename = "user_logged_in")]
    UserLoggedInEvent(UserLoggedIn),
    #[serde(rename = "user_logged_out")]
    UserLoggedOutEvent(UserLoggedOut),
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum AuthEventType {
    SignedUp,
    Logged,
}

impl TopicEvent for AuthEventType {
    fn event_topic(&self) -> crate::publisher::topic::Topic {
        match self {
            AuthEventType::SignedUp => "auth-signed",
            AuthEventType::Logged => "auth-logged",
        }
    }
}

impl TopicEvent for AuthEvent {
    fn event_topic(&self) -> crate::publisher::topic::Topic {
        self.event_type().event_topic()
    }
}

impl KeyEvent for AuthEvent {
    fn event_key(&self) -> Option<Vec<u8>> {
        match self {
            AuthEvent::UserSignedUpEvent(_) => None,
            AuthEvent::UserLoggedInEvent(_) => None,
            AuthEvent::UserLoggedOutEvent(_) => None,
        }
    }
}

impl TypedEvent for AuthEvent {
    type EventType = EventType;

    fn event_type(&self) -> Self::EventType {
        EventType::Auth(match self {
            AuthEvent::UserSignedUpEvent(_) => AuthEventType::SignedUp,
            AuthEvent::UserLoggedInEvent(_) => AuthEventType::Logged,
            AuthEvent::UserLoggedOutEvent(_) => AuthEventType::Logged,
        })
    }
}
