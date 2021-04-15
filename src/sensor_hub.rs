use std::{fmt::Display, time::Duration};

use anyhow::{anyhow, Context as AnyhowContext};
use btleplug::api::{BDAddr, Central, Peripheral, ValueNotification, WriteType};
#[cfg(target_os = "linux")]
use btleplug::bluez::{adapter::Adapter, manager::Manager};
#[cfg(target_os = "macos")]
use btleplug::corebluetooth::{adapter::Adapter, manager::Manager};
#[cfg(target_os = "windows")]
use btleplug::winrtble::{adapter::Adapter, manager::Manager};
use chrono::Local;
use log::{debug, info};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::message::UpdateMessage;

#[cfg(any(target_os = "windows", target_os = "macos"))]
fn get_central(manager: &Manager) -> anyhow::Result<Adapter> {
    let adapters = manager.adapters()?;
    adapters
        .into_iter()
        .nth(0)
        .ok_or(anyhow!("cannot find bluetooth adapter"))
}

#[cfg(target_os = "linux")]
fn get_central(manager: &Manager) -> anyhow::Result<Adapter> {
    let adapters = manager.adapters()?;
    let adapter = adapters
        .into_iter()
        .nth(0)
        .ok_or(anyhow!("cannot find bluetooth adapter"))?;
    adapter.set_powered(true)?;
    Ok(adapter)
}

const UUID_NOTIFY: Uuid = Uuid::from_u128(0x0000ffe1_0000_1000_8000_00805f9b34fb);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("bluetooth backend is dead")]
    BackendDead,
    #[error("bluetooth device not found")]
    DeviceNotFound,
    #[error("device not supported")]
    NotSupported,
    #[error("fail to connect to device `{1}`: {0}")]
    FailedToConnect(btleplug::Error, String),
    #[error("data request to device `{0}` timed-out")]
    RequestTimeout(String),
    #[error("invalid response for {0} from device `{1}`")]
    InvalidResponse(&'static str, String),
    #[error("error during bluetooth operation on `{2}` when {1}: {0}")]
    Other(btleplug::Error, &'static str, String),
}

#[derive(Debug)]
struct PollRequest {
    address: BDAddr,
    callback_channel: oneshot::Sender<Result<UpdateMessage, Error>>,
}

#[derive(Debug)]
enum BusMessage {
    Poll(PollRequest),
    Terminate,
}

trait Sensor {
    fn poll(&mut self) -> Result<UpdateMessage, Error>;
}

struct DeviceWrapper<P: Peripheral + Display> {
    device: P,
}

fn sensor_connect(central: &Adapter, address: BDAddr) -> Result<impl Sensor, Error> {
    let device = central.peripheral(address).ok_or(Error::DeviceNotFound)?;

    device
        .connect()
        .map_err(|e| Error::FailedToConnect(e, device.to_string()))?;

    debug!("connected to {}", device);

    Ok(DeviceWrapper { device })
}

impl<P: Peripheral + Display> Sensor for DeviceWrapper<P> {
    fn poll(&mut self) -> Result<UpdateMessage, Error> {
        let characteristics = self
            .device
            .discover_characteristics()
            .map_err(|e| Error::Other(e, "discovering characteristics", self.device.to_string()))?;
        let service = characteristics
            .into_iter()
            .find(|c| c.uuid == UUID_NOTIFY)
            .ok_or(Error::NotSupported)?;

        self.device
            .subscribe(&service)
            .map_err(|e| Error::Other(e, "subscribing", self.device.to_string()))?;

        let (send, recv) = std::sync::mpsc::channel();
        let address = self.device.address();

        self.device
            .on_notification(Box::new(move |notification: ValueNotification| {
                debug!(
                    "received message from {}: {:x?}",
                    address, notification.value
                );
                if let Some(s) = std::str::from_utf8(&notification.value)
                    .ok()
                    .map(str::trim_start)
                    .and_then(|s| s.strip_prefix("OK+Get:"))
                    .map(str::trim)
                {
                    send.send(s.to_string()).ok();
                }
            }));

        // Probe temperature
        let temperature = loop {
            self.device
                .write(&service, "AT+TEMP?".as_bytes(), WriteType::WithoutResponse)
                .map_err(|e| Error::Other(e, "sending AT commands", self.device.to_string()))?;

            let response = recv
                .recv_timeout(Duration::from_secs(1))
                .map_err(|_| Error::RequestTimeout(self.device.to_string()))?;

            if &response != "085.00" {
                break response
                    .parse()
                    .map_err(|_| Error::InvalidResponse("AT+TEMP?", self.device.to_string()))?;
            }

            std::thread::sleep(Duration::from_millis(50));
        };

        // Probe battery
        self.device
            .write(&service, "AT+BATT?".as_bytes(), WriteType::WithoutResponse)
            .map_err(|e| Error::Other(e, "sending AT commands", self.device.to_string()))?;
        let battery: i8 = recv
            .recv_timeout(Duration::from_secs(1))
            .map_err(|_| Error::RequestTimeout(self.device.to_string()))?
            .parse()
            .map_err(|_| Error::InvalidResponse("AT+BATT?", self.device.to_string()))?;

        // Probe rssi
        self.device
            .write(&service, "AT+RSSI?".as_bytes(), WriteType::WithoutResponse)
            .map_err(|e| Error::Other(e, "sending AT commands", self.device.to_string()))?;
        let rssi: i16 = recv
            .recv_timeout(Duration::from_secs(1))
            .map_err(|_| Error::RequestTimeout(self.device.to_string()))?
            .parse()
            .map_err(|_| Error::InvalidResponse("AT+RSSI?", self.device.to_string()))?;

        info!("polled device {}", self.device);

        Ok(UpdateMessage {
            timestamp: Local::now(),
            device: self.device.address(),
            temperature,
            battery,
            rssi,
        })
    }
}

impl<P: Peripheral + Display> Drop for DeviceWrapper<P> {
    fn drop(&mut self) {
        if self.device.is_connected() {
            debug!("disconnecting from {}", self.device);
            self.device.disconnect().ok();
        }
    }
}

pub struct Backend {
    control_recv: mpsc::UnboundedReceiver<BusMessage>,

    _manager: Manager,
    central: Adapter,
}

impl Backend {
    fn new(recv: mpsc::UnboundedReceiver<BusMessage>) -> anyhow::Result<Self> {
        let manager = Manager::new().context("fail to initialize Bluetooth manager")?;
        let central = get_central(&manager).context("fail to acquire Bluetooth bus")?;

        central
            .start_scan()
            .context("fail to start Bluetooth devices scanning")?;

        Ok(Self {
            control_recv: recv,
            _manager: manager,
            central,
        })
    }

    pub fn serve(mut self) {
        while let Some(command) = self.control_recv.blocking_recv() {
            match command {
                BusMessage::Poll(PollRequest {
                    address,
                    callback_channel,
                }) => match sensor_connect(&self.central, address) {
                    Ok(mut sensor) => {
                        callback_channel.send(sensor.poll()).ok();
                    }
                    Err(e) => {
                        callback_channel.send(Err(e)).ok();
                    }
                },
                BusMessage::Terminate => {
                    break;
                }
            }
        }
    }
}

pub struct Hub {
    control_send: mpsc::UnboundedSender<BusMessage>,
}

impl Hub {
    fn new(send: mpsc::UnboundedSender<BusMessage>) -> Self {
        Self { control_send: send }
    }

    pub async fn poll(&self, address: &BDAddr) -> Result<UpdateMessage, Error> {
        let (send, recv) = oneshot::channel();
        self.control_send
            .send(BusMessage::Poll(PollRequest {
                address: address.clone(),
                callback_channel: send,
            }))
            .map_err(|_| Error::BackendDead)?;
        recv.await.map_err(|_| Error::BackendDead)?
    }
}

impl Drop for Hub {
    fn drop(&mut self) {
        self.control_send.send(BusMessage::Terminate).ok();
    }
}

pub fn init() -> anyhow::Result<(Hub, Backend)> {
    let (send, recv) = mpsc::unbounded_channel();
    let hub = Hub::new(send);
    let backend = Backend::new(recv)?;
    Ok((hub, backend))
}
