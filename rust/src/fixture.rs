//! Test-only peripheral beneath the production adapter/connection workers.
//! No alternate request coordinator, FFI transport, or lifecycle state machine.
use crate::{
    codec::{Error, Reader, event},
    connection::{self, Notifications},
    event::Sink,
    scan::{self, Events},
};
use btleplug::api::{CharPropFlags, Characteristic, Descriptor, Service, ValueNotification};
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

const SERVICE: Uuid = Uuid::from_u128(0x0000180f00001000800000805f9b34fb);
const CHARACTERISTIC: Uuid = Uuid::from_u128(0x00002a1900001000800000805f9b34fb);
const DESCRIPTOR: Uuid = Uuid::from_u128(0x0000290200001000800000805f9b34fb);

pub(crate) struct Adapter {
    sink: Sink,
    events: mpsc::Sender<Result<Vec<u8>, Error>>,
    receiver: Option<mpsc::Receiver<Result<Vec<u8>, Error>>>,
    connect: watch::Sender<bool>,
    discovery: watch::Sender<bool>,
    notification_transport: watch::Sender<u8>,
}
impl Adapter {
    pub fn new(sink: Sink) -> Self {
        let (events, receiver) = mpsc::channel(1024);
        let (connect, _) = watch::channel(true);
        let (discovery, _) = watch::channel(true);
        let (notification_transport, _) = watch::channel(1u8);
        Self {
            sink,
            events,
            receiver: Some(receiver),
            connect,
            discovery,
            notification_transport,
        }
    }
}
impl scan::Driver for Adapter {
    type Device = Device;
    fn control(&mut self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        match payload {
            [4, states @ ..]
                if !states.is_empty()
                    && states.len() <= 64
                    && states.iter().all(|state| (2..=4).contains(state)) =>
            {
                for state in states {
                    self.events
                        .try_send(Ok(event(3, 0, Ok(vec![*state]))))
                        .map_err(|_| Error::new(16, "Fixture adapter event queue unavailable"))?;
                }
            }
            [1, state @ 2..=4] => self
                .events
                .try_send(Ok(event(3, 0, Ok(vec![*state]))))
                .map_err(|_| Error::new(16, "Fixture adapter event queue unavailable"))?,
            [2, value @ (0 | 1)] => {
                self.connect.send_replace(*value == 1);
            }
            [3, value @ (0 | 1)] => {
                self.discovery.send_replace(*value == 1);
            }
            [5, value @ (0 | 1)] => {
                self.notification_transport.send_replace(*value);
            }
            [10] => {
                return Err(Error::new(3, "Controlled native access failure")
                    .with_native_code("0x80070005"));
            }
            [7] => {
                return Err(
                    Error::new(19, "Controlled native platform failure").with_native_code("133")
                );
            }
            [8] => self
                .events
                .try_send(Err(
                    Error::new(19, "Controlled native scan failure").with_native_code("3")
                ))
                .map_err(|_| Error::new(16, "Fixture adapter event queue unavailable"))?,
            [6] => {
                self.notification_transport.send_replace(2);
            }
            _ => return Err(Error::new(16, "Malformed fixture control")),
        }
        Ok(Vec::new())
    }
    async fn initialize(&mut self) -> Result<(u8, Events), Error> {
        let receiver = self
            .receiver
            .take()
            .ok_or_else(|| Error::new(16, "Fixture adapter already initialized"))?;
        Ok((
            4,
            Box::pin(futures_util::stream::unfold(
                receiver,
                |mut receiver| async move { receiver.recv().await.map(|value| (value, receiver)) },
            )),
        ))
    }
    async fn start(&mut self) -> Result<(), Error> {
        Ok(())
    }
    async fn stop(&mut self) -> Result<(), Error> {
        Ok(())
    }
    async fn peripheral(&mut self, id: &str) -> Result<Device, Error> {
        if id != "bletide-fixture" {
            return Err(Error::new(4, "Unknown fixture peripheral"));
        }
        Ok(Device {
            connected: Arc::new(AtomicBool::new(false)),
            sink: self.sink.clone(),
            connect: self.connect.clone(),
            discovery: self.discovery.clone(),
            notification_transport: self.notification_transport.clone(),
        })
    }
}
#[derive(Clone)]
pub(crate) struct Device {
    connected: Arc<AtomicBool>,
    sink: Sink,
    connect: watch::Sender<bool>,
    discovery: watch::Sender<bool>,
    notification_transport: watch::Sender<u8>,
}
async fn gate(gate: &watch::Sender<bool>, sink: &Sink, operation: u8) -> Result<(), Error> {
    let mut released = gate.subscribe();
    if !*released.borrow() && !sink.send(&event(8, 0, Ok(vec![operation]))) {
        return Err(Error::new(17, "Fixture event sink closed"));
    }
    while !*released.borrow_and_update() {
        released
            .changed()
            .await
            .map_err(|_| Error::new(17, "Fixture gate closed"))?;
    }
    Ok(())
}
impl connection::Device for Device {
    type Driver = Driver;
    fn driver(self) -> Driver {
        let (notifications, receiver) = mpsc::channel(1024);
        Driver {
            device: self,
            notifications,
            receiver: Some(receiver),
            value: vec![0, 255, 128],
            descriptor_value: vec![0, 0],
            discovered: false,
        }
    }
    async fn probe_connected(&self) -> Result<bool, Error> {
        Ok(self.connected.load(Ordering::Acquire))
    }
}
pub(crate) struct Driver {
    device: Device,
    notifications: mpsc::Sender<ValueNotification>,
    receiver: Option<mpsc::Receiver<ValueNotification>>,
    value: Vec<u8>,
    descriptor_value: Vec<u8>,
    discovered: bool,
}
impl connection::Driver for Driver {
    async fn connect(&mut self) -> Result<(), Error> {
        gate(&self.device.connect, &self.device.sink, 30).await?;
        self.device.connected.store(true, Ordering::Release);
        Ok(())
    }
    async fn disconnect(&mut self) -> Result<(), Error> {
        self.device.connected.store(false, Ordering::Release);
        Ok(())
    }
    async fn notifications(&mut self) -> Result<Notifications, Error> {
        let receiver = self
            .receiver
            .take()
            .ok_or_else(|| Error::new(16, "Fixture stream already taken"))?;
        let transport = self.device.notification_transport.subscribe();
        Ok(Box::pin(futures_util::stream::unfold(
            (receiver, transport, false),
            |(mut receiver, mut transport, ended)| async move {
                if ended {
                    return None;
                }
                loop {
                    let transport_state = *transport.borrow();
                    match transport_state {
                        0 => return None,
                        2 => {
                            return Some((
                                Err(Error::new(15, "Controlled notification transport failure")),
                                (receiver, transport, true),
                            ));
                        }
                        _ => {}
                    }
                    tokio::select! { biased;
                        changed = transport.changed() => { if changed.is_err() { return None; } },
                        value = receiver.recv() => return value.map(|value| (Ok(value), (receiver, transport, false))),
                    }
                }
            },
        )))
    }
    async fn operate(&mut self, operation: u32, payload: &[u8]) -> Result<Vec<u8>, Error> {
        let mut reader = Reader::new(payload);
        match operation {
            40 => {
                reader.finish()?;
                gate(&self.device.discovery, &self.device.sink, 40).await?;
                self.discovered = true;
                connection::encode_services(&BTreeSet::from([Service {
                    uuid: SERVICE,
                    primary: true,
                    characteristics: BTreeSet::from([Characteristic {
                        uuid: CHARACTERISTIC,
                        service_uuid: SERVICE,
                        properties: CharPropFlags::READ
                            | CharPropFlags::WRITE
                            | CharPropFlags::WRITE_WITHOUT_RESPONSE
                            | CharPropFlags::NOTIFY,
                        descriptors: BTreeSet::from([Descriptor {
                            uuid: DESCRIPTOR,
                            service_uuid: SERVICE,
                            characteristic_uuid: CHARACTERISTIC,
                        }]),
                    }]),
                }]))
            }
            41..=47 => {
                if !self.discovered {
                    return Err(Error::new(13, "Discover fixture attributes first"));
                }
                if reader.uuid()? != SERVICE {
                    return Err(Error::new(12, "Unknown fixture service"));
                }
                if reader.uuid()? != CHARACTERISTIC {
                    return Err(Error::new(13, "Unknown fixture characteristic"));
                }
                match operation {
                    41 => {
                        reader.finish()?;
                        Ok(self.value.clone())
                    }
                    42 | 43 => {
                        let value = reader.remaining();
                        // Explicit fault inputs for deterministic queue/cancellation contracts.
                        if value == [0xee] {
                            return Err(Error::new(15, "Controlled fixture write failure"));
                        }
                        if value.len() == 2 && value[0] == 0xed && matches!(value[1], 1 | 2 | 3 | 8)
                        {
                            return Err(Error::new(
                                value[1].into(),
                                "Controlled fixture OS access loss",
                            ));
                        }
                        if value == [0xff] {
                            // A test barrier proves this operation is running before cancellation.
                            if !self.device.sink.send(&event(8, 0, Ok(vec![0xff]))) {
                                return Err(Error::new(17, "Fixture event sink closed"));
                            }
                            return std::future::pending().await;
                        }
                        self.value = value.to_vec();
                        self.notifications
                            .try_send(ValueNotification {
                                service_uuid: SERVICE,
                                uuid: CHARACTERISTIC,
                                value: self.value.clone(),
                            })
                            .map_err(|_| Error::new(15, "Fixture notification backlog exceeded"))?;
                        Ok(Vec::new())
                    }
                    44 | 45 => {
                        reader.finish()?;
                        Ok(Vec::new())
                    }
                    46 | 47 => {
                        if reader.uuid()? != DESCRIPTOR {
                            return Err(Error::new(14, "Unknown fixture descriptor"));
                        }
                        if operation == 46 {
                            reader.finish()?;
                            Ok(self.descriptor_value.clone())
                        } else {
                            self.descriptor_value = reader.remaining().to_vec();
                            Ok(Vec::new())
                        }
                    }
                    _ => Err(Error::new(11, "Unknown fixture operation")),
                }
            }
            48 => {
                reader.finish()?;
                Ok((-42_i16).to_le_bytes().to_vec())
            }
            49 => {
                reader.finish()?;
                Ok(23_u16.to_le_bytes().to_vec())
            }
            _ => Err(Error::new(11, "Unsupported fixture operation")),
        }
    }
}
