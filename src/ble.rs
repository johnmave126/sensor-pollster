use crate::{
    config::Config,
    message::{Payload, UpdateMessage},
    Error,
};

use std::{
    collections::{hash_map, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc,
    },
};

use btleplug::api::{
    BDAddr, Central, CentralEvent, Characteristic, Peripheral, ValueNotification, UUID,
};
#[cfg(target_os = "linux")]
use btleplug::bluez::{adapter::ConnectedAdapter, manager::Manager};
#[cfg(target_os = "macos")]
use btleplug::corebluetooth::{adapter::Adapter, manager::Manager};
#[cfg(target_os = "windows")]
use btleplug::winrtble::{adapter::Adapter, manager::Manager};
use chrono::{DateTime, Local};
use futures::prelude::Future;
use log::{debug, error, info, warn};
use tokio::{
    stream::StreamExt,
    sync::mpsc,
    task::{spawn, spawn_blocking},
    time::{sleep, Duration},
};

#[derive(Debug)]
enum DeviceState<P: Peripheral> {
    Unknown,
    Connecting(P, usize),
    Connected(P, Characteristic, usize, DateTime<Local>),
}

#[derive(Debug)]
enum BusMessage {
    Discovered(BDAddr),
    Lost(BDAddr),
    Connected(BDAddr),
    Disconnected(BDAddr),
    HealthVerification(BDAddr, DateTime<Local>),
    ConnectionFailed(BDAddr),
    Poll(BDAddr, DateTime<Local>),
    TemperatureUpdate(BDAddr, f32),
    BatteryUpdate(BDAddr, i8),
    RssiUpdate(BDAddr, i16),
    ScanStart,
    ScanEnd,
    PruneTasks,
}

// 0000ffe1-0000-1000-8000-00805f9b34fb
const UUID_NOTIFY: UUID = UUID::B128([
    0xfb, 0x34, 0x9b, 0x5f, 0x80, 0x00, 0x00, 0x80, 0x00, 0x10, 0x00, 0x00, 0xe1, 0xff, 0x00, 0x00,
]);
const QUICK_DISCONNECT_THRESHOLD: u64 = 10;

#[cfg(any(target_os = "windows", target_os = "macos"))]
fn get_central(manager: &Manager) -> Result<Adapter, Error> {
    let adapters = manager.adapters()?;
    adapters
        .into_iter()
        .nth(0)
        .map_or(Err(Error::NoAdapter), |adapter| Ok(adapter))
}

#[cfg(target_os = "linux")]
fn get_central(manager: &Manager) -> Result<ConnectedAdapter> {
    let adapters = manager.adapters()?;
    let adapter = adapters
        .into_iter()
        .nth(0)
        .map_or(Err(Error::NoAdapter), |adapter| Ok(adapter));
    adapter.connect()
}

fn calculate_backoff_intervals(min: u16, max: u16, max_step: u16) -> Vec<u64> {
    if max == 0 {
        vec![0; 1]
    } else {
        let ln_backoff_ratio =
            (f32::from(max).ln() - f32::from(min + 1).ln()) / f32::from(max_step);
        (0..=max_step + 1)
            .map(|i| (f32::from(min + 1) * (f32::from(i) * ln_backoff_ratio).exp()) as u64)
            .collect()
    }
}

fn try_connect<P: Peripheral>(
    bus_sender: mpsc::Sender<BusMessage>,
    device: P,
) -> Result<(), Error> {
    debug!("trying to connect to {}", device.address());
    if let Err(_) = device.connect() {
        let _ = bus_sender.blocking_send(BusMessage::ConnectionFailed(device.address()));
    }
    Ok(())
}

fn reconnect_with_reason<P: 'static + Peripheral, C: Central<P>>(
    addr: &BDAddr,
    central: &C,
    backoff_intervals: &Vec<u64>,
    bus_sender: mpsc::Sender<BusMessage>,
    devices: &mut HashMap<BDAddr, DeviceState<P>>,
    reason: &str,
) {
    if let hash_map::Entry::Occupied(mut entry) = devices.entry(*addr) {
        // Try to reconnect
        let state = entry.get_mut();
        match state {
            DeviceState::Connected(_, _, backoff, _) => {
                if let Some(device) = central.peripheral(*addr) {
                    let retry_delay = *backoff_intervals
                        .get(*backoff)
                        .or(backoff_intervals.last())
                        .unwrap_or(&10);
                    info!(
                        "device {} {}, reconnect in {} seconds",
                        device.address(),
                        reason,
                        retry_delay
                    );
                    *state = DeviceState::Connecting(
                        device.clone(),
                        std::cmp::min(*backoff + 1, backoff_intervals.len()),
                    );
                    spawn_blocking(move || {
                        std::thread::sleep(Duration::from_secs(retry_delay));
                        debug!("trying to connect to {}", device.address());
                        if let Err(err) = device.connect() {
                            warn!("failed to connect to {}: {}", device.address(), err);
                            let _ = bus_sender
                                .blocking_send(BusMessage::ConnectionFailed(device.address()));
                        }
                    });
                } else {
                    *state = DeviceState::Unknown;
                }
            }
            _ => (),
        }
    }
}

fn init_device<P: Peripheral>(
    bus_sender: mpsc::Sender<BusMessage>,
    device: P,
) -> Result<Characteristic, Error> {
    let properties = device.properties();
    debug!(
        "setting up {} {}",
        properties.address,
        properties.local_name.unwrap_or("<Unnamed>".to_string())
    );
    let addr = properties.address;
    let characteristics = device.discover_characteristics()?;
    let notify_service = characteristics
        .into_iter()
        .find(|c| c.uuid == UUID_NOTIFY)
        .ok_or(Error::NotHMDevice)?;
    device.subscribe(&notify_service)?;
    device.on_notification(Box::new(move |notification: ValueNotification| {
        debug!("received message from {}: {:x?}", addr, notification.value);
        match std::str::from_utf8(&notification.value) {
            Ok(s) => {
                let message = s.trim();
                if message.starts_with("OK+Get:") {
                    let payload = &s[7..];
                    if payload.starts_with('-') {
                        // RSSI
                        if let Ok(rssi) = payload.parse() {
                            let _ = bus_sender.blocking_send(BusMessage::RssiUpdate(addr, rssi));
                        }
                    } else if payload.contains('.') {
                        // Temperature
                        if payload == "085.00" {
                            // Temperature sensor not initialized, redo polling
                            let _ = bus_sender.blocking_send(BusMessage::Poll(addr, Local::now()));
                        } else if let Ok(temperature) = payload.parse() {
                            let _ = bus_sender
                                .blocking_send(BusMessage::TemperatureUpdate(addr, temperature));
                        }
                    } else if let Ok(battery) = payload.parse() {
                        // Battery
                        let _ = bus_sender.blocking_send(BusMessage::BatteryUpdate(addr, battery));
                    }
                }
            }
            Err(_) => {
                warn!(
                    "failed to decode message from {}: {:x?}",
                    addr, notification.value
                );
            }
        }
    }));
    Ok(notify_service)
}

fn poll_device<P: 'static + Peripheral>(device: P, endpoint: &Characteristic) {
    let endpoint = endpoint.clone();
    spawn_blocking(move || -> Result<(), Error> {
        device.command(&endpoint, "AT+TEMP?".as_bytes())?;
        std::thread::sleep(Duration::from_millis(5));
        device.command(&endpoint, "AT+BATT?".as_bytes())?;
        std::thread::sleep(Duration::from_millis(5));
        device.command(&endpoint, "AT+RSSI?".as_bytes())?;
        Ok(())
    });
}

async fn poll_ble_devices(
    config: &Config,
    sender: mpsc::Sender<UpdateMessage>,
    is_running: Arc<AtomicBool>,
) -> Result<(), Error> {
    let (bus_sender, mut bus_receiver) = mpsc::channel(64);

    let manager = Manager::new()?;
    let central = get_central(&manager)?;
    let ble_event_receiver = central.event_receiver().unwrap();

    let bus_sender_2 = bus_sender.clone();
    spawn_blocking(move || {
        while let Ok(event) = ble_event_receiver.recv() {
            match event {
                CentralEvent::DeviceDiscovered(addr) => {
                    debug!("Device discovered: {}", addr);
                    let _ = bus_sender_2.blocking_send(BusMessage::Discovered(addr));
                }
                CentralEvent::DeviceLost(addr) => {
                    debug!("Device lost: {}", addr);
                    let _ = bus_sender_2.blocking_send(BusMessage::Lost(addr));
                }
                CentralEvent::DeviceConnected(addr) => {
                    debug!("Device connected: {}", addr);
                    let _ = bus_sender_2.blocking_send(BusMessage::Connected(addr));
                }
                CentralEvent::DeviceDisconnected(addr) => {
                    debug!("Device disconnected: {}", addr);
                    let _ = bus_sender_2.blocking_send(BusMessage::Disconnected(addr));
                }
                _ => (),
            }
        }
    });

    let mut devices: HashMap<_, _> = config
        .devices
        .keys()
        .map(|addr| (addr.clone(), DeviceState::Unknown))
        .collect();
    let backoff_intervals =
        calculate_backoff_intervals(config.retry_min, config.retry_max, config.retry_backoff);

    bus_sender
        .send(BusMessage::ScanStart)
        .await
        .expect("failed to send message");

    let bus_sender_2 = bus_sender.clone();
    spawn(async move {
        sleep(Duration::from_secs(5)).await;
        let _ = bus_sender_2.send(BusMessage::PruneTasks).await;
    });

    while is_running.load(AtomicOrdering::SeqCst) {
        if let Some(message) = bus_receiver.next().await {
            match message {
                BusMessage::PruneTasks => {
                    let bus_sender_2 = bus_sender.clone();
                    spawn(async move {
                        sleep(Duration::from_secs(5)).await;
                        let _ = bus_sender_2.send(BusMessage::PruneTasks).await;
                    });
                }
                BusMessage::Discovered(addr) => {
                    if let hash_map::Entry::Occupied(mut entry) = devices.entry(addr) {
                        if let Some(device) = central.peripheral(addr) {
                            *entry.get_mut() = DeviceState::Connecting(device.clone(), 0);
                            let bus_sender_2 = bus_sender.clone();
                            spawn_blocking(move || try_connect(bus_sender_2, device));
                        }
                    }
                }
                BusMessage::Lost(addr) => {
                    devices
                        .entry(addr)
                        .and_modify(|state| *state = DeviceState::Unknown);
                }
                BusMessage::Connected(addr) => {
                    if let hash_map::Entry::Occupied(mut entry) = devices.entry(addr) {
                        let state = entry.get_mut();
                        match state {
                            DeviceState::Connecting(device, backoff) => {
                                info!("device {} connected", device.address());
                                // subscribe to the device
                                match init_device(bus_sender.clone(), device.clone()) {
                                    Ok(chara) => {
                                        let now = Local::now();
                                        let addr = device.address();
                                        // Keep backoff count until health check passes
                                        *state = DeviceState::Connected(
                                            device.clone(),
                                            chara,
                                            *backoff,
                                            now.clone(),
                                        );
                                        let bus_sender_2 = bus_sender.clone();
                                        let now_2 = now.clone();
                                        spawn(async move {
                                            // Health check after QUICK_DISCONNECT_THRESHOLD seconds
                                            sleep(Duration::from_secs(QUICK_DISCONNECT_THRESHOLD))
                                                .await;
                                            let _ = bus_sender_2
                                                .send(BusMessage::HealthVerification(addr, now_2))
                                                .await;
                                        });
                                        let _ = bus_sender.send(BusMessage::Poll(addr, now)).await;
                                    }
                                    Err(Error::NotHMDevice) => {
                                        error!("device {} is not a HM-11 device, removing from monitored devices", addr);
                                        entry.remove();
                                    }
                                    _ => (),
                                }
                            }
                            _ => (),
                        }
                    }
                }
                BusMessage::Disconnected(addr) => {
                    reconnect_with_reason(
                        &addr,
                        &central,
                        &backoff_intervals,
                        bus_sender.clone(),
                        &mut devices,
                        "disconnected",
                    );
                }
                BusMessage::ConnectionFailed(addr) => {
                    reconnect_with_reason(
                        &addr,
                        &central,
                        &backoff_intervals,
                        bus_sender.clone(),
                        &mut devices,
                        "failed to connect",
                    );
                }
                BusMessage::HealthVerification(addr, connected_time) => {
                    devices.entry(addr).and_modify(|state| {
                        // If the device is still connected and doesn't suffer reconnection in between
                        // Clear the backoff count
                        if let DeviceState::Connected(_, _, ref mut backoff, ref last_time) = state
                        {
                            if last_time <= &connected_time {
                                *backoff = 0;
                            }
                        }
                    });
                }
                BusMessage::Poll(addr, last_connected) => {
                    if let hash_map::Entry::Occupied(entry) = devices.entry(addr) {
                        match entry.get() {
                            DeviceState::Connected(device, chara, _, connected) => {
                                let connected = connected.clone();
                                if connected <= last_connected {
                                    info!("poll device {}", device.address());
                                    poll_device(device.clone(), &chara);
                                    let period = Duration::from_secs(config.poll_period);
                                    let bus_sender_2 = bus_sender.clone();
                                    spawn(async move {
                                        sleep(period).await;
                                        let _ = bus_sender_2
                                            .send(BusMessage::Poll(addr, connected))
                                            .await;
                                    });
                                }
                            }
                            _ => (),
                        }
                    }
                }
                BusMessage::TemperatureUpdate(device, temperature) => {
                    let _ = sender
                        .send(UpdateMessage {
                            timestamp: Local::now(),
                            device,
                            value: Payload::Temperature(temperature),
                        })
                        .await;
                }
                BusMessage::BatteryUpdate(device, battery) => {
                    let _ = sender
                        .send(UpdateMessage {
                            timestamp: Local::now(),
                            device,
                            value: Payload::Battery(battery),
                        })
                        .await;
                }
                BusMessage::RssiUpdate(device, rssi) => {
                    let _ = sender
                        .send(UpdateMessage {
                            timestamp: Local::now(),
                            device,
                            value: Payload::Rssi(rssi),
                        })
                        .await;
                }
                BusMessage::ScanStart => {
                    central.start_scan()?;
                    let duration = Duration::from_secs(config.scan_duration);
                    let bus_sender_2 = bus_sender.clone();
                    spawn(async move {
                        sleep(duration).await;
                        let _ = bus_sender_2.send(BusMessage::ScanEnd).await;
                    });
                }
                BusMessage::ScanEnd => {
                    central.stop_scan()?;
                    let duration = Duration::from_secs(config.scan_period);
                    let bus_sender_2 = bus_sender.clone();
                    spawn(async move {
                        sleep(duration).await;
                        let _ = bus_sender_2.send(BusMessage::ScanStart).await;
                    });
                }
            }
        } else {
            break;
        }
    }

    Ok(())
}

pub(crate) fn create_ble_source<'a>(
    config: &'a Config,
    is_running: Arc<AtomicBool>,
) -> Result<
    (
        mpsc::Receiver<UpdateMessage>,
        impl Future<Output = Result<(), Error>> + 'a,
    ),
    Error,
> {
    let (sender, receiver) = mpsc::channel(64);
    Ok((receiver, poll_ble_devices(config, sender, is_running)))
}
