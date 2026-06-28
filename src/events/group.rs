use serde::{Deserialize, Serialize};

use crate::{
    events::EventType,
    publisher::{
        TypedEvent,
        topic::{KeyEvent, TopicEvent},
    },
};

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupCreatedEvent {
    pub group_id: String,
    pub owner_id: String,
    pub channel_id: String,
    pub member_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupDeletedEvent {
    pub group_id: String,
    pub owner_id: String,
    pub member_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupUserAddedEvent {
    pub group_id: String,
    pub user_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupUserRemovedEvent {
    pub group_id: String,
    pub user_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum GroupEvent {
    #[serde(rename = "group_created")]
    GroupCreatedEvent(GroupCreatedEvent),
    #[serde(rename = "group_deleted")]
    GroupDeletedEvent(GroupDeletedEvent),
    #[serde(rename = "group_user_added")]
    GroupUserAddedEvent(GroupUserAddedEvent),
    #[serde(rename = "group_user_removed")]
    GroupUserRemovedEvent(GroupUserRemovedEvent),
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum GroupEventType {
    Groups,
    Members,
}

impl TopicEvent for GroupEvent {
    fn event_topic(&self) -> crate::publisher::topic::Topic {
        self.event_type().event_topic()
    }
}

impl TopicEvent for GroupEventType {
    fn event_topic(&self) -> crate::publisher::topic::Topic {
        match self {
            GroupEventType::Groups => "group-groups",
            GroupEventType::Members => "group-members",
        }
    }
}

impl KeyEvent for GroupEvent {
    fn event_key(&self) -> Option<Vec<u8>> {
        match self {
            GroupEvent::GroupCreatedEvent(_) => None,
            GroupEvent::GroupDeletedEvent(_) => None,
            GroupEvent::GroupUserAddedEvent(_) => None,
            GroupEvent::GroupUserRemovedEvent(_) => None,
        }
    }
}

impl TypedEvent for GroupEvent {
    type EventType = EventType;

    fn event_type(&self) -> Self::EventType {
        EventType::Group(match self {
            GroupEvent::GroupCreatedEvent(_) => GroupEventType::Groups,
            GroupEvent::GroupDeletedEvent(_) => GroupEventType::Groups,
            GroupEvent::GroupUserAddedEvent(_) => GroupEventType::Members,
            GroupEvent::GroupUserRemovedEvent(_) => GroupEventType::Members,
        })
    }
}
