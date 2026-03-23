use std::collections::VecDeque;
use std::fmt;
use std::io::{self, IoSliceMut};
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use iroh::endpoint::transports::{Addr, CustomEndpoint, CustomSender, CustomTransport, Transmit};
use iroh_base::CustomAddr;
use n0_watcher::Watchable;
use noq_udp::{EcnCodepoint, RecvMeta};
use tokio::sync::Notify;

pub const DEFAULT_SEND_QUEUE_CAPACITY: usize = 64;
pub const SANDSTORM_RAW_UDP_TRANSPORT_ID: u64 = 0x5a5d_5354_5550_4450;

#[derive(Clone, Debug)]
pub struct OwnedUdpPacket {
    pub payload: Vec<u8>,
    pub src: SocketAddr,
    pub dst: SocketAddr,
    pub ecn: Option<EcnCodepoint>,
    pub dst_ip: Option<IpAddr>,
}

pub fn new_sandstorm_custom_transport(
    local_addr: SocketAddr,
    transport_id: u64,
    send_queue_capacity: usize,
) -> (
    Arc<SandstormCustomTransport>,
    SandstormCustomTransportDriver,
) {
    let local_custom_addr = socket_addr_to_custom_addr(transport_id, local_addr);
    let shared = Arc::new(Shared {
        transport_id,
        local_addr,
        local_addrs: Watchable::new(vec![local_custom_addr]),
        recv: Mutex::new(RecvState::default()),
        send: Mutex::new(SendState {
            queue: VecDeque::new(),
            capacity: send_queue_capacity.max(1),
            writable_waker: None,
            closed: false,
        }),
        send_notify: Notify::new(),
    });

    (
        Arc::new(SandstormCustomTransport {
            shared: shared.clone(),
        }),
        SandstormCustomTransportDriver { shared },
    )
}

#[derive(Clone)]
pub struct SandstormCustomTransport {
    shared: Arc<Shared>,
}

impl fmt::Debug for SandstormCustomTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SandstormCustomTransport")
            .field("transport_id", &self.shared.transport_id)
            .field("local_addr", &self.shared.local_addr)
            .finish_non_exhaustive()
    }
}

impl CustomTransport for SandstormCustomTransport {
    fn bind(&self) -> io::Result<Box<dyn CustomEndpoint>> {
        Ok(Box::new(SandstormCustomEndpoint {
            shared: self.shared.clone(),
        }))
    }
}

#[derive(Clone)]
pub struct SandstormCustomTransportDriver {
    shared: Arc<Shared>,
}

impl fmt::Debug for SandstormCustomTransportDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SandstormCustomTransportDriver")
            .field("transport_id", &self.shared.transport_id)
            .field("local_addr", &self.shared.local_addr)
            .finish_non_exhaustive()
    }
}

impl SandstormCustomTransportDriver {
    pub async fn next_outgoing_packet(&self) -> io::Result<OwnedUdpPacket> {
        loop {
            {
                let mut send = self.shared.send.lock().map_err(poisoned_lock_to_io_error)?;
                if let Some(packet) = send.queue.pop_front() {
                    if let Some(waker) = send.writable_waker.take() {
                        waker.wake();
                    }
                    return Ok(packet);
                }
                if send.closed {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "sandstorm custom transport driver is closed",
                    ));
                }
            }

            self.shared.send_notify.notified().await;
        }
    }

    pub fn receive_packet(&self, packet: OwnedUdpPacket) {
        if let Ok(mut recv) = self.shared.recv.lock() {
            recv.queue.push_back(packet);
            if let Some(waker) = recv.recv_waker.take() {
                waker.wake();
            }
        }
    }

    pub fn close(&self) {
        if let Ok(mut recv) = self.shared.recv.lock() {
            recv.closed = true;
            if let Some(waker) = recv.recv_waker.take() {
                waker.wake();
            }
        }

        if let Ok(mut send) = self.shared.send.lock() {
            send.closed = true;
            if let Some(waker) = send.writable_waker.take() {
                waker.wake();
            }
        }

        self.shared.send_notify.notify_waiters();
    }
}

struct SandstormCustomEndpoint {
    shared: Arc<Shared>,
}

impl fmt::Debug for SandstormCustomEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SandstormCustomEndpoint")
            .field("transport_id", &self.shared.transport_id)
            .field("local_addr", &self.shared.local_addr)
            .finish_non_exhaustive()
    }
}

impl CustomEndpoint for SandstormCustomEndpoint {
    fn watch_local_addrs(&self) -> n0_watcher::Direct<Vec<CustomAddr>> {
        self.shared.local_addrs.watch()
    }

    fn create_sender(&self) -> Arc<dyn CustomSender> {
        Arc::new(SandstormCustomSender {
            shared: self.shared.clone(),
        })
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        metas: &mut [RecvMeta],
        source_addrs: &mut [Addr],
    ) -> Poll<io::Result<usize>> {
        let limit = bufs.len().min(metas.len()).min(source_addrs.len());
        let mut recv = self.shared.recv.lock().map_err(poisoned_lock_to_io_error)?;

        if recv.closed && recv.queue.is_empty() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "sandstorm custom transport endpoint is closed",
            )));
        }

        if recv.queue.is_empty() {
            recv.recv_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let mut count = 0;
        while count < limit {
            let Some(packet) = recv.queue.pop_front() else {
                break;
            };

            let copy_len = bufs[count].len().min(packet.payload.len());
            bufs[count][..copy_len].copy_from_slice(&packet.payload[..copy_len]);
            let mut meta = RecvMeta::default();
            meta.addr = packet.src;
            meta.len = copy_len;
            meta.stride = copy_len;
            meta.ecn = packet.ecn;
            meta.dst_ip = packet.dst_ip;
            metas[count] = meta;
            source_addrs[count] = Addr::Custom(socket_addr_to_custom_addr(
                self.shared.transport_id,
                packet.src,
            ));
            count += 1;
        }

        Poll::Ready(Ok(count))
    }

    fn max_transmit_segments(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }
}

struct SandstormCustomSender {
    shared: Arc<Shared>,
}

impl fmt::Debug for SandstormCustomSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SandstormCustomSender")
            .field("transport_id", &self.shared.transport_id)
            .field("local_addr", &self.shared.local_addr)
            .finish_non_exhaustive()
    }
}

impl CustomSender for SandstormCustomSender {
    fn is_valid_send_addr(&self, addr: &CustomAddr) -> bool {
        addr.id() == self.shared.transport_id && custom_addr_to_socket_addr(addr).is_ok()
    }

    fn poll_send(
        &self,
        cx: &mut Context,
        dst: &CustomAddr,
        transmit: &Transmit<'_>,
    ) -> Poll<io::Result<()>> {
        let dst = custom_addr_to_socket_addr(dst)?;
        let mut send = self.shared.send.lock().map_err(poisoned_lock_to_io_error)?;

        if send.closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "sandstorm custom transport sender is closed",
            )));
        }

        if send.queue.len() >= send.capacity {
            send.writable_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let packet = OwnedUdpPacket {
            payload: transmit.contents.to_vec(),
            src: self.shared.local_addr,
            dst,
            ecn: None,
            dst_ip: Some(dst.ip()),
        };
        send.queue.push_back(packet);
        drop(send);
        self.shared.send_notify.notify_one();
        Poll::Ready(Ok(()))
    }
}

struct Shared {
    transport_id: u64,
    local_addr: SocketAddr,
    local_addrs: Watchable<Vec<CustomAddr>>,
    recv: Mutex<RecvState>,
    send: Mutex<SendState>,
    send_notify: Notify,
}

#[derive(Default)]
struct RecvState {
    queue: VecDeque<OwnedUdpPacket>,
    recv_waker: Option<Waker>,
    closed: bool,
}

struct SendState {
    queue: VecDeque<OwnedUdpPacket>,
    capacity: usize,
    writable_waker: Option<Waker>,
    closed: bool,
}

pub fn socket_addr_to_custom_addr(transport_id: u64, addr: SocketAddr) -> CustomAddr {
    match addr {
        SocketAddr::V4(addr) => {
            let mut data = [0u8; 7];
            data[0] = 4;
            data[1..5].copy_from_slice(&addr.ip().octets());
            data[5..7].copy_from_slice(&addr.port().to_be_bytes());
            CustomAddr::from_parts(transport_id, &data)
        }
        SocketAddr::V6(addr) => {
            let mut data = [0u8; 19];
            data[0] = 6;
            data[1..17].copy_from_slice(&addr.ip().octets());
            data[17..19].copy_from_slice(&addr.port().to_be_bytes());
            CustomAddr::from_parts(transport_id, &data)
        }
    }
}

pub fn custom_addr_to_socket_addr(addr: &CustomAddr) -> io::Result<SocketAddr> {
    match addr.data() {
        [4, a, b, c, d, p0, p1] => Ok(SocketAddr::from((
            [*a, *b, *c, *d],
            u16::from_be_bytes([*p0, *p1]),
        ))),
        [6, bytes @ ..] if bytes.len() == 18 => {
            let mut ip = [0u8; 16];
            ip.copy_from_slice(&bytes[..16]);
            let port = u16::from_be_bytes([bytes[16], bytes[17]]);
            Ok(SocketAddr::new(IpAddr::from(ip), port))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid sandstorm custom transport address",
        )),
    }
}

fn poisoned_lock_to_io_error<T>(_: std::sync::PoisonError<T>) -> io::Error {
    io::Error::other("mutex lock poisoned")
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::noop_waker;

    fn test_context() -> Context<'static> {
        let waker = Box::leak(Box::new(noop_waker()));
        Context::from_waker(waker)
    }

    #[test]
    fn custom_addr_roundtrip_ipv4() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 9999));
        let custom = socket_addr_to_custom_addr(SANDSTORM_RAW_UDP_TRANSPORT_ID, addr);
        assert_eq!(custom_addr_to_socket_addr(&custom).unwrap(), addr);
    }

    #[test]
    fn custom_addr_roundtrip_ipv6() {
        let addr = "[2001:db8::42]:9999".parse().unwrap();
        let custom = socket_addr_to_custom_addr(SANDSTORM_RAW_UDP_TRANSPORT_ID, addr);
        assert_eq!(custom_addr_to_socket_addr(&custom).unwrap(), addr);
    }

    #[test]
    fn custom_addr_rejects_invalid_payload_shape() {
        let custom = CustomAddr::from_parts(SANDSTORM_RAW_UDP_TRANSPORT_ID, &[0x99, 0x00]);
        let err = custom_addr_to_socket_addr(&custom).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn endpoint_receives_packets_from_driver() {
        let local = SocketAddr::from(([127, 0, 0, 1], 4242));
        let remote = SocketAddr::from(([10, 0, 0, 7], 7777));
        let (transport, driver) =
            new_sandstorm_custom_transport(local, SANDSTORM_RAW_UDP_TRANSPORT_ID, 1);
        let mut endpoint = transport.bind().unwrap();
        let mut cx = test_context();

        driver.receive_packet(OwnedUdpPacket {
            payload: b"world".to_vec(),
            src: remote,
            dst: local,
            ecn: None,
            dst_ip: Some(local.ip()),
        });

        let mut recv_buf = [0u8; 32];
        let mut bufs = [IoSliceMut::new(&mut recv_buf)];
        let mut metas = [RecvMeta::default()];
        let mut addrs = [Addr::default()];
        let n = match endpoint.poll_recv(&mut cx, &mut bufs, &mut metas, &mut addrs) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("unexpected poll result: {other:?}"),
        };
        assert_eq!(n, 1);
        assert_eq!(&recv_buf[..metas[0].len], b"world");
        assert!(matches!(addrs[0], Addr::Custom(_)));
    }
}
