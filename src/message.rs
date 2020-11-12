use btleplug::api::BDAddr;
use chrono::{DateTime, Local};

#[derive(Debug)]
pub(crate) enum Payload {
    Temperature(f32),
    Battery(i8),
    Rssi(i16),
}

#[derive(Debug)]
pub(crate) struct UpdateMessage {
    pub timestamp: DateTime<Local>,
    pub device: BDAddr,
    pub value: Payload,
}
