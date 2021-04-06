use btleplug::api::BDAddr;
use chrono::{DateTime, Local};

#[derive(Debug)]
pub struct UpdateMessage {
    pub timestamp: DateTime<Local>,
    pub device: BDAddr,
    pub temperature: f32,
    pub battery: i8,
    pub rssi: i16,
}
