use std::collections::VecDeque;
use std::fmt::{self, Debug};
use std::io::{self, IoSliceMut};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use quinn::{AsyncUdpSocket, UdpPoller};
use quinn_udp::{EcnCodepoint, RecvMeta, Transmit};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, Default)]
pub struct SandstormUdpCapabilities {
    pub may_fragment: bool,
    pub max_receive_segments: usize,
    pub max_transmit_segments: usize,
}

#[derive(Clone, Debug)]
pub struct OwnedUdpPacket {
    pub payload: Vec<u8>,
    pub src: Option<SocketAddr>,
    pub dst: SocketAddr,
    pub ecn: Option<EcnCodepoint>,
    pub dst_ip: Option<IpAddr>,
}

#[derive(Clone, Debug)]
struct QueuedPacket {
    payload: Vec<u8>,
    src: SocketAddr,
    dst_ip: Option<IpAddr>,
    ecn: Option<EcnCodepoint>,
}

pub trait SandstormUdpReceiver: Send + Sync + Debug + 'static {
    fn receive_packet(&self, packet: OwnedUdpPacket);
    fn close(&self) {}
}

pub trait SandstormUdpSocketBackend: Send + Sync + Debug + 'static {
    fn register_receiver(&self, receiver: Arc<dyn SandstormUdpReceiver>) -> io::Result<()>;
    fn local_addr(&self) -> io::Result<SocketAddr>;
    fn capabilities(&self) -> SandstormUdpCapabilities;

    // This is intentionally split into `try_send_packet` and `poll_writable` because the Quinn
    // version currently resolved by `iroh-tunnel` expects socket I/O readiness to look like a
    // nonblocking UDP socket even when the real implementation is an RPC bridge.
    fn try_send_packet(&self, packet: &OwnedUdpPacket) -> io::Result<()>;
    fn poll_writable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
}

pub fn new_proxy_udp_backend(
    local_addr: SocketAddr,
    capabilities: SandstormUdpCapabilities,
    send_queue_capacity: usize,
) -> (Arc<ProxyUdpBackend>, ProxyUdpDriver) {
    let inner = Arc::new(ProxyUdpInner {
        local_addr,
        capabilities,
        send: Mutex::new(ProxySendState {
            queue: VecDeque::new(),
            capacity: send_queue_capacity.max(1),
            writable_waker: None,
            closed: false,
        }),
        send_notify: Notify::new(),
        receiver: Mutex::new(None),
    });

    (
        Arc::new(ProxyUdpBackend {
            inner: inner.clone(),
        }),
        ProxyUdpDriver { inner },
    )
}

pub struct ProxyUdpBackend {
    inner: Arc<ProxyUdpInner>,
}

impl Debug for ProxyUdpBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyUdpBackend")
            .field("local_addr", &self.inner.local_addr)
            .field("capabilities", &self.inner.capabilities)
            .finish_non_exhaustive()
    }
}

impl SandstormUdpSocketBackend for ProxyUdpBackend {
    fn register_receiver(&self, receiver: Arc<dyn SandstormUdpReceiver>) -> io::Result<()> {
        *self
            .inner
            .receiver
            .lock()
            .map_err(poisoned_lock_to_io_error)? = Some(receiver);
        Ok(())
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.inner.local_addr)
    }

    fn capabilities(&self) -> SandstormUdpCapabilities {
        self.inner.capabilities
    }

    fn try_send_packet(&self, packet: &OwnedUdpPacket) -> io::Result<()> {
        let mut send = self.inner.send.lock().map_err(poisoned_lock_to_io_error)?;

        if send.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "proxy udp backend is closed",
            ));
        }

        if send.queue.len() >= send.capacity {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        send.queue.push_back(packet.clone());
        drop(send);
        self.inner.send_notify.notify_one();
        Ok(())
    }

    fn poll_writable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut send = self.inner.send.lock().map_err(poisoned_lock_to_io_error)?;

        if send.closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "proxy udp backend is closed",
            )));
        }

        if send.queue.len() < send.capacity {
            Poll::Ready(Ok(()))
        } else {
            send.writable_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[derive(Clone)]
pub struct ProxyUdpDriver {
    inner: Arc<ProxyUdpInner>,
}

impl Debug for ProxyUdpDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyUdpDriver")
            .field("local_addr", &self.inner.local_addr)
            .finish_non_exhaustive()
    }
}

impl ProxyUdpDriver {
    pub async fn next_outgoing_packet(&self) -> io::Result<OwnedUdpPacket> {
        loop {
            {
                let mut send = self.inner.send.lock().map_err(poisoned_lock_to_io_error)?;
                if let Some(packet) = send.queue.pop_front() {
                    if let Some(waker) = send.writable_waker.take() {
                        waker.wake();
                    }
                    return Ok(packet);
                }
                if send.closed {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "proxy udp driver is closed",
                    ));
                }
            }

            self.inner.send_notify.notified().await;
        }
    }

    pub fn receive_packet(&self, packet: OwnedUdpPacket) {
        if let Ok(receiver) = self.inner.receiver.lock()
            && let Some(receiver) = receiver.as_ref()
        {
            receiver.receive_packet(packet);
        }
    }

    pub fn close(&self) {
        if let Ok(mut send) = self.inner.send.lock() {
            send.closed = true;
            if let Some(waker) = send.writable_waker.take() {
                waker.wake();
            }
        }

        self.inner.send_notify.notify_waiters();

        if let Ok(receiver) = self.inner.receiver.lock()
            && let Some(receiver) = receiver.as_ref()
        {
            receiver.close();
        }
    }
}

#[derive(Debug)]
struct ProxyUdpInner {
    local_addr: SocketAddr,
    capabilities: SandstormUdpCapabilities,
    send: Mutex<ProxySendState>,
    send_notify: Notify,
    receiver: Mutex<Option<Arc<dyn SandstormUdpReceiver>>>,
}

#[derive(Debug)]
struct ProxySendState {
    queue: VecDeque<OwnedUdpPacket>,
    capacity: usize,
    writable_waker: Option<Waker>,
    closed: bool,
}

pub struct SandstormQuinnUdpSocket {
    backend: Arc<dyn SandstormUdpSocketBackend>,
    recv: Arc<RecvState>,
    local_addr: SocketAddr,
    capabilities: SandstormUdpCapabilities,
}

impl SandstormQuinnUdpSocket {
    pub fn new(backend: Arc<dyn SandstormUdpSocketBackend>) -> io::Result<Self> {
        let local_addr = backend.local_addr()?;
        let capabilities = backend.capabilities();
        let recv = Arc::new(RecvState::default());
        backend.register_receiver(Arc::new(SandstormReceiver { recv: recv.clone() }))?;
        Ok(Self {
            backend,
            recv,
            local_addr,
            capabilities,
        })
    }
}

impl Debug for SandstormQuinnUdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SandstormQuinnUdpSocket")
            .field("local_addr", &self.local_addr)
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

impl AsyncUdpSocket for SandstormQuinnUdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(SandstormUdpPoller {
            backend: self.backend.clone(),
        })
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        self.backend.try_send_packet(&OwnedUdpPacket {
            payload: transmit.contents.to_vec(),
            src: Some(SocketAddr::new(
                transmit.src_ip.unwrap_or(self.local_addr.ip()),
                self.local_addr.port(),
            )),
            dst: transmit.destination,
            ecn: transmit.ecn,
            dst_ip: Some(transmit.destination.ip()),
        })
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.recv.state.lock().map_err(poisoned_lock_to_io_error)?;

        if state.closed && state.queue.is_empty() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Sandstorm UDP receiver closed",
            )));
        }

        if state.queue.is_empty() {
            state.recv_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let mut count = 0;
        while count < bufs.len() && count < meta.len() {
            let Some(packet) = state.queue.pop_front() else {
                break;
            };

            let copy_len = bufs[count].len().min(packet.payload.len());
            bufs[count][..copy_len].copy_from_slice(&packet.payload[..copy_len]);
            meta[count] = RecvMeta {
                addr: packet.src,
                len: copy_len,
                stride: copy_len,
                ecn: packet.ecn,
                dst_ip: packet.dst_ip,
            };
            count += 1;
        }

        Poll::Ready(Ok(count))
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }

    fn max_receive_segments(&self) -> usize {
        self.capabilities.max_receive_segments.max(1)
    }

    fn may_fragment(&self) -> bool {
        self.capabilities.may_fragment
    }

    fn max_transmit_segments(&self) -> usize {
        self.capabilities.max_transmit_segments.max(1)
    }
}

#[derive(Debug)]
struct SandstormUdpPoller {
    backend: Arc<dyn SandstormUdpSocketBackend>,
}

impl UdpPoller for SandstormUdpPoller {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.backend.poll_writable(cx)
    }
}

#[derive(Debug, Default)]
struct RecvState {
    state: Mutex<RecvStateInner>,
}

#[derive(Debug, Default)]
struct RecvStateInner {
    queue: VecDeque<QueuedPacket>,
    recv_waker: Option<Waker>,
    closed: bool,
}

#[derive(Debug)]
struct SandstormReceiver {
    recv: Arc<RecvState>,
}

impl SandstormUdpReceiver for SandstormReceiver {
    fn receive_packet(&self, packet: OwnedUdpPacket) {
        let Some(src) = packet.src else {
            return;
        };

        if let Ok(mut state) = self.recv.state.lock() {
            state.queue.push_back(QueuedPacket {
                payload: packet.payload,
                src,
                dst_ip: packet.dst_ip,
                ecn: packet.ecn,
            });
            if let Some(waker) = state.recv_waker.take() {
                waker.wake();
            }
        }
    }

    fn close(&self) {
        if let Ok(mut state) = self.recv.state.lock() {
            state.closed = true;
            if let Some(waker) = state.recv_waker.take() {
                waker.wake();
            }
        }
    }
}

fn poisoned_lock_to_io_error<T>(_: std::sync::PoisonError<T>) -> io::Error {
    io::Error::other("mutex lock poisoned")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures::task::noop_waker;

    #[derive(Debug)]
    struct MockBackend {
        receiver: Mutex<Option<Arc<dyn SandstormUdpReceiver>>>,
        sent_packets: Mutex<Vec<OwnedUdpPacket>>,
        local_addr: SocketAddr,
        capabilities: SandstormUdpCapabilities,
        writable: AtomicBool,
    }

    impl MockBackend {
        fn new(local_addr: SocketAddr) -> Self {
            Self {
                receiver: Mutex::new(None),
                sent_packets: Mutex::new(Vec::new()),
                local_addr,
                capabilities: SandstormUdpCapabilities {
                    may_fragment: false,
                    max_receive_segments: 1,
                    max_transmit_segments: 1,
                },
                writable: AtomicBool::new(true),
            }
        }

        fn deliver(&self, packet: OwnedUdpPacket) {
            let receiver = self
                .receiver
                .lock()
                .expect("receiver lock poisoned")
                .clone()
                .expect("receiver should be registered");
            receiver.receive_packet(packet);
        }

        fn close(&self) {
            if let Some(receiver) = self
                .receiver
                .lock()
                .expect("receiver lock poisoned")
                .clone()
            {
                receiver.close();
            }
        }
    }

    impl SandstormUdpSocketBackend for MockBackend {
        fn register_receiver(&self, receiver: Arc<dyn SandstormUdpReceiver>) -> io::Result<()> {
            *self.receiver.lock().map_err(poisoned_lock_to_io_error)? = Some(receiver);
            Ok(())
        }

        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(self.local_addr)
        }

        fn capabilities(&self) -> SandstormUdpCapabilities {
            self.capabilities
        }

        fn try_send_packet(&self, packet: &OwnedUdpPacket) -> io::Result<()> {
            if !self.writable.load(Ordering::SeqCst) {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            self.sent_packets
                .lock()
                .map_err(poisoned_lock_to_io_error)?
                .push(packet.clone());
            Ok(())
        }

        fn poll_writable(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.writable.load(Ordering::SeqCst) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }
    }

    fn test_context() -> Context<'static> {
        let waker = Box::leak(Box::new(noop_waker()));
        Context::from_waker(waker)
    }

    #[test]
    fn try_send_maps_transmit_to_owned_packet() {
        let backend = Arc::new(MockBackend::new(SocketAddr::from(([127, 0, 0, 1], 4242))));
        let socket = SandstormQuinnUdpSocket::new(backend.clone()).expect("socket should build");
        let payload = [1u8, 2, 3, 4];
        let transmit = Transmit {
            destination: SocketAddr::from(([10, 0, 0, 7], 9999)),
            ecn: Some(EcnCodepoint::Ect0),
            contents: &payload,
            segment_size: None,
            src_ip: None,
        };

        socket.try_send(&transmit).expect("send should succeed");

        let sent = backend
            .sent_packets
            .lock()
            .expect("sent packet lock poisoned");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload, payload);
        assert_eq!(sent[0].dst, transmit.destination);
        assert_eq!(sent[0].src, Some(SocketAddr::from(([127, 0, 0, 1], 4242))));
        assert_eq!(sent[0].ecn, Some(EcnCodepoint::Ect0));
    }

    #[test]
    fn poll_recv_drains_incoming_packets() {
        let backend = Arc::new(MockBackend::new(SocketAddr::from(([127, 0, 0, 1], 4242))));
        let socket = SandstormQuinnUdpSocket::new(backend.clone()).expect("socket should build");
        let mut cx = test_context();
        let mut storage = [0u8; 16];
        let mut bufs = [IoSliceMut::new(&mut storage)];
        let mut meta = [RecvMeta::default()];

        assert!(matches!(
            socket.poll_recv(&mut cx, &mut bufs, &mut meta),
            Poll::Pending
        ));

        backend.deliver(OwnedUdpPacket {
            payload: b"hello".to_vec(),
            src: Some(SocketAddr::from(([203, 0, 113, 9], 5353))),
            dst: SocketAddr::from(([127, 0, 0, 1], 4242)),
            ecn: Some(EcnCodepoint::Ce),
            dst_ip: Some(IpAddr::from([127, 0, 0, 1])),
        });

        match socket.poll_recv(&mut cx, &mut bufs, &mut meta) {
            Poll::Ready(Ok(1)) => {
                assert_eq!(&storage[..5], b"hello");
                assert_eq!(meta[0].addr, SocketAddr::from(([203, 0, 113, 9], 5353)));
                assert_eq!(meta[0].len, 5);
                assert_eq!(meta[0].stride, 5);
                assert_eq!(meta[0].ecn, Some(EcnCodepoint::Ce));
                assert_eq!(meta[0].dst_ip, Some(IpAddr::from([127, 0, 0, 1])));
            }
            other => panic!("unexpected recv result: {other:?}"),
        }
    }

    #[test]
    fn close_wakes_poll_recv_with_broken_pipe() {
        let backend = Arc::new(MockBackend::new(SocketAddr::from(([127, 0, 0, 1], 4242))));
        let socket = SandstormQuinnUdpSocket::new(backend.clone()).expect("socket should build");
        let mut cx = test_context();
        let mut storage = [0u8; 8];
        let mut bufs = [IoSliceMut::new(&mut storage)];
        let mut meta = [RecvMeta::default()];

        assert!(matches!(
            socket.poll_recv(&mut cx, &mut bufs, &mut meta),
            Poll::Pending
        ));

        backend.close();

        match socket.poll_recv(&mut cx, &mut bufs, &mut meta) {
            Poll::Ready(Err(err)) => assert_eq!(err.kind(), io::ErrorKind::BrokenPipe),
            other => panic!("unexpected recv result after close: {other:?}"),
        }
    }

    #[test]
    fn io_poller_tracks_backend_writability() {
        let backend = Arc::new(MockBackend::new(SocketAddr::from(([127, 0, 0, 1], 4242))));
        backend.writable.store(false, Ordering::SeqCst);
        let socket =
            Arc::new(SandstormQuinnUdpSocket::new(backend.clone()).expect("socket should build"));
        let mut poller = socket.create_io_poller();
        let mut cx = test_context();

        assert!(matches!(
            poller.as_mut().poll_writable(&mut cx),
            Poll::Pending
        ));

        backend.writable.store(true, Ordering::SeqCst);

        assert!(matches!(
            poller.as_mut().poll_writable(&mut cx),
            Poll::Ready(Ok(()))
        ));
    }
}
