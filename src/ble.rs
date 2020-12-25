use crate::{
    config::Config,
    message::{Payload, UpdateMessage},
    Error,
};

use std::{collections::HashSet, fmt::Display, thread::spawn as spawn_thread};

use btleplug::api::{
    BDAddr, Central, CentralEvent, Characteristic, Peripheral, ValueNotification, UUID,
};
#[cfg(target_os = "linux")]
use btleplug::bluez::{adapter::ConnectedAdapter, manager::Manager};
#[cfg(target_os = "macos")]
use btleplug::corebluetooth::{adapter::Adapter, manager::Manager};
#[cfg(target_os = "windows")]
use btleplug::winrtble::{adapter::Adapter, manager::Manager};
use chrono::Local;
use futures::{executor::block_on, prelude::Future};
use log::{debug, error, info, warn};
use rand::Rng;
use tokio::{
    stream::StreamExt,
    sync::{broadcast, mpsc},
    task::{spawn, spawn_blocking},
    time::{interval_at, timeout, Duration, Instant},
};

#[derive(Debug)]
enum BusMessage {
    Connected(BDAddr),
    Poll,
    TemperatureUpdate(BDAddr, f32),
    BatteryUpdate(BDAddr, i8),
    RssiUpdate(BDAddr, i16),
    Terminate,
    ErrorTerminate(String),
}

#[derive(Debug, Clone)]
enum DeviceResponse {
    TemperatureSkipped,
    Temperature(f32),
    Battery(i8),
    Rssi(i16),
}

trait LogError {
    fn pipe_log<S: AsRef<str>, Formatter: FnOnce() -> S>(self, f: Formatter) -> Self;
}

impl<T, E: Display> LogError for Result<T, E> {
    fn pipe_log<S: AsRef<str>, Formatter: FnOnce() -> S>(self, f: Formatter) -> Self {
        if let Err(ref e) = self {
            error!("{}: {}", f().as_ref(), e);
        }
        self
    }
}

// 0000ffe1-0000-1000-8000-00805f9b34fb
#[cfg(any(target_os = "windows", target_os = "macos"))]
const UUID_NOTIFY: UUID = UUID::B128([
    0xFB, 0x34, 0x9B, 0x5F, 0x80, 0x00, 0x00, 0x80, 0x00, 0x10, 0x00, 0x00, 0xE1, 0xFF, 0x00, 0x00,
]);
#[cfg(target_os = "linux")]
const UUID_NOTIFY: UUID = UUID::B16(0xFFE1);

#[cfg(any(target_os = "windows", target_os = "macos"))]
fn get_central(manager: &Manager) -> Result<Adapter, Error> {
    let adapters = manager.adapters()?;
    adapters
        .into_iter()
        .nth(0)
        .map_or(Err(Error::NoAdapter), Result::Ok)
}

#[cfg(target_os = "linux")]
fn get_central(manager: &Manager) -> Result<ConnectedAdapter, Error> {
    let adapters = manager.adapters()?;
    let adapter = adapters
        .into_iter()
        .nth(0)
        .map_or(Err(Error::NoAdapter), Result::Ok)?;
    Ok(adapter.connect()?)
}

fn setup_device<P: Peripheral + Display>(
    sender: broadcast::Sender<DeviceResponse>,
    device: &P,
) -> Result<Characteristic, Error> {
    info!("setting up {}", device);
    let addr = device.address();
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
                            let _ = sender.send(DeviceResponse::Rssi(rssi));
                        }
                    } else if payload.contains('.') {
                        // Temperature
                        if payload == "085.00" {
                            // Temperature sensor not initialized, skip polling
                            debug!(
                                "temperature sensor of device {} is not yet initialized",
                                addr
                            );
                            let _ = sender.send(DeviceResponse::TemperatureSkipped);
                        } else if let Ok(temperature) = payload.parse() {
                            let _ = sender.send(DeviceResponse::Temperature(temperature));
                        }
                    } else if let Ok(battery) = payload.parse() {
                        // Battery
                        let _ = sender.send(DeviceResponse::Battery(battery));
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
    debug!("finish setting up {}", device);
    Ok(notify_service)
}

fn poll_device<P: 'static + Peripheral + Display>(
    device: P,
    bus_sender: mpsc::Sender<BusMessage>,
) -> Result<(), Error> {
    async fn wait_command<P: 'static + Peripheral + Display>(
        device: P,
        notify_service: Characteristic,
        bus_sender: mpsc::Sender<BusMessage>,
        receiver: broadcast::Receiver<DeviceResponse>,
    ) -> Result<(), Error> {
        if !device.is_connected() {
            return Err(Error::PreemptDisconnect);
        }

        let address = device.address();

        let mut has_temp = false;
        let mut has_batt = false;
        let mut has_rssi = false;

        tokio::pin! {
            let stream = receiver.into_stream().take_while(Result::is_ok).map(Result::unwrap);
        }

        while let Some(message) = stream.next().await {
            match message {
                DeviceResponse::TemperatureSkipped => {
                    // Ask for temperature again
                    device.command(&notify_service, "AT+TEMP?".as_bytes())?;
                }
                DeviceResponse::Temperature(temperature) => {
                    info!("device {} temperature polled: {}", device, temperature);
                    has_temp = true;
                    let _ = bus_sender
                        .send(BusMessage::TemperatureUpdate(address, temperature))
                        .await;
                }
                DeviceResponse::Battery(battery) => {
                    info!("device {} battery polled: {}", device, battery);
                    has_batt = true;
                    let _ = bus_sender
                        .send(BusMessage::BatteryUpdate(address, battery))
                        .await;
                }
                DeviceResponse::Rssi(rssi) => {
                    info!("device {} rssi polled: {}", device, rssi);
                    has_rssi = true;
                    let _ = bus_sender.send(BusMessage::RssiUpdate(address, rssi)).await;
                }
            }
            if has_temp && has_batt && has_rssi {
                break;
            }
        }
        Ok(())
    }

    debug!("polling {}", device);

    let (value_sender, value_receiver) = broadcast::channel(16);
    let notify_service = setup_device(value_sender, &device)?;

    device.command(&notify_service, "AT+TEMP?".as_bytes())?;
    std::thread::sleep(Duration::from_millis(5));
    device.command(&notify_service, "AT+BATT?".as_bytes())?;
    std::thread::sleep(Duration::from_millis(5));
    device.command(&notify_service, "AT+RSSI?".as_bytes())?;
    info!("device {} polling commands sent", device);
    // wait for all response, or timeout after 5s
    let timeout_task = timeout(
        Duration::from_secs(5),
        wait_command(device.clone(), notify_service, bus_sender, value_receiver),
    );
    if let Err(_) = block_on(timeout_task) {
        warn!("device {} doesn't finish polling in time", device);
    }
    info!("device {} polled", device);

    Ok(())
}

async fn poll_ble_devices(
    config: &Config,
    sender: mpsc::Sender<UpdateMessage>,
    termination_receiver: broadcast::Receiver<()>,
) -> Result<(), Error> {
    let (bus_sender, bus_receiver) = mpsc::channel(64);
    tokio::pin! {
        let termination_receiver = termination_receiver.into_stream().take_while(Result::is_ok).map(|_| BusMessage::Terminate).take(1);
    }
    let mut bus_receiver = bus_receiver.merge(termination_receiver);

    let manager = Manager::new()?;
    let central = get_central(&manager)?;
    central.active(false);
    let ble_event_receiver = central.event_receiver().unwrap();

    let bus_sender_2 = bus_sender.clone();
    spawn_thread(move || {
        while let Ok(event) = ble_event_receiver.recv() {
            match event {
                CentralEvent::DeviceDiscovered(addr) => {
                    debug!("device discovered: {}", addr);
                }
                CentralEvent::DeviceConnected(addr) => {
                    info!("device connected: {}", addr);
                    let _ = bus_sender_2.blocking_send(BusMessage::Connected(addr));
                }
                CentralEvent::DeviceLost(addr) => {
                    debug!("device lost: {}", addr);
                }
                CentralEvent::DeviceDisconnected(addr) => {
                    info!("device disconnected: {}", addr);
                }
                _ => (),
            }
        }
    });
    central.start_scan()?;

    let devices: HashSet<_> = config.devices.keys().cloned().collect();

    let bus_sender_2 = bus_sender.clone();
    let poll_period = config.poll_period;
    spawn(async move {
        // Give it 10 seconds to start up scanning
        let mut interval = interval_at(
            Instant::now() + Duration::from_secs(10),
            Duration::from_secs(poll_period),
        );
        loop {
            interval.tick().await;
            let _ = bus_sender_2.send(BusMessage::Poll).await;
        }
    });

    let bus_sender_2 = bus_sender.clone();
    let central_2 = central.clone();
    let (connect_sender, mut connect_receiver) = mpsc::channel(32);
    let retry = config.retry;

    let connector = spawn_thread(move || {
        while let Some(addr) = connect_receiver.blocking_recv() {
            debug!("connect to device {}", addr);
            if let Some(device) = central_2.peripheral(addr) {
                let _ = device.disconnect();
                for i in 0..retry + 1 {
                    match device.connect() {
                        Ok(_) => {
                            break;
                        }
                        Err(btleplug::Error::Other(reason)) => {
                            error!("syscall error encountered: {}", reason);
                            bus_sender_2
                                .blocking_send(BusMessage::ErrorTerminate(reason))
                                .unwrap();
                            return;
                        }
                        Err(err) => {
                            warn!("failed to connect to {}: {}", device, err);
                        }
                    }
                    debug!("{}-th try to connect to {} failed", i, device);
                    std::thread::sleep(Duration::from_millis(rand::thread_rng().gen_range(10, 50)));
                    if i == retry {
                        error!("failed to connect to device {}", device);
                    }
                }
            } else {
                warn!("device {} lost before connecting", addr);
            }
        }
    });

    while let Some(message) = bus_receiver.next().await {
        match message {
            BusMessage::Connected(addr) => {
                if devices.contains(&addr) {
                    if let Some(device) = central.peripheral(addr) {
                        info!("prepare to poll {}", device);
                        let address = device.address();
                        let bus_sender_2 = bus_sender.clone();
                        spawn_blocking(move || {
                            std::thread::sleep(Duration::from_millis(10));
                            let _ = poll_device(device.clone(), bus_sender_2)
                                .pipe_log(|| format!("failed to poll device {}", address));
                            // Make sure the device is always disconnected after polling
                            let _ = device.disconnect();
                        });
                    } else {
                        warn!("device {} lost before performing operation", addr);
                    }
                }
            }
            BusMessage::Poll => {
                // Prune polls that exits
                info!("issuing poll request to all devices");
                for addr in devices.iter() {
                    connect_sender
                        .send(addr.clone())
                        .await
                        .expect("device connector unresponsable");
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
            BusMessage::Terminate => {
                debug!("terminate message received");
                break;
            }
            BusMessage::ErrorTerminate(reason) => {
                return Err(Error::SysError(reason));
            }
        }
    }

    info!("polling stopping...");
    central.stop_scan()?;
    info!("scanning stopped");
    drop(connect_sender);
    info!("connect sender dropped");
    drop(central);
    info!("central dropped");
    connector
        .join()
        .expect("unable to terminate connecting thread");
    Ok(())
}

pub(crate) fn create_ble_source<'a>(
    config: &'a Config,
    termination_receiver: broadcast::Receiver<()>,
) -> Result<
    (
        mpsc::Receiver<UpdateMessage>,
        impl Future<Output = Result<(), Error>> + 'a,
    ),
    Error,
> {
    let (sender, receiver) = mpsc::channel(64);
    Ok((
        receiver,
        poll_ble_devices(config, sender, termination_receiver),
    ))
}
