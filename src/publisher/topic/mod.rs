#[cfg(feature = "fluvio")]
pub mod fluvio;
#[cfg(feature = "kafka")]
pub mod kafka;

pub type Topic = &'static str;

pub trait TopicEvent {
    fn event_topic(&self) -> Topic;
}

pub trait KeyEvent {
    fn event_key(&self) -> Option<Vec<u8>>;
}
