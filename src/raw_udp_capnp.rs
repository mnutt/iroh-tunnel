use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use capnp::capability::{Promise, Rc};
use capnp_rpc::{new_client, pry};
use quinn_udp::EcnCodepoint;

use crate::ip_capnp;
use crate::quinn_adapter::{
    new_proxy_udp_backend, OwnedUdpPacket, ProxyUdpBackend, ProxyUdpDriver,
    SandstormUdpCapabilities, SandstormUdpReceiver,
};
use crate::sandstorm_custom_transport::{
    DEFAULT_SEND_QUEUE_CAPACITY as DEFAULT_CUSTOM_TRANSPORT_SEND_QUEUE_CAPACITY,
    SandstormCustomTransport, SandstormCustomTransportDriver, new_sandstorm_custom_transport,
};

pub const DEFAULT_SEND_QUEUE_CAPACITY: usize = 64;

pub fn new_raw_udp_receiver_client(
    receiver: Arc<dyn SandstormUdpReceiver>,
) -> ip_capnp::raw_udp_receiver::Client {
    new_client(CapnpRawUdpReceiverBridge { receiver })
}

#[derive(Debug)]
struct CapnpRawUdpReceiverBridge {
    receiver: Arc<dyn SandstormUdpReceiver>,
}

impl ip_capnp::raw_udp_receiver::Server for CapnpRawUdpReceiverBridge {
    fn receive(
        self: Rc<Self>,
        params: ip_capnp::raw_udp_receiver::ReceiveParams,
        _: ip_capnp::raw_udp_receiver::ReceiveResults,
    ) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        let packet = pry!(params.get_packet());
        let payload = pry!(packet.get_payload()).to_vec();
        let src = pry!(udp_endpoint_to_socket_addr(pry!(packet.get_src())));
        let dst = pry!(udp_endpoint_to_socket_addr(pry!(packet.get_dst())));
        let ecn = packet
            .get_ecn()
            .ok()
            .and_then(ecn_from_capnp)
            .or(None);

        self.receiver.receive_packet(OwnedUdpPacket {
            payload,
            src: Some(src),
            dst,
            ecn,
            dst_ip: normalize_received_dst_ip(dst),
        });

        Promise::ok(())
    }
}

fn normalize_received_dst_ip(dst: SocketAddr) -> Option<IpAddr> {
    if dst.ip().is_unspecified() {
        None
    } else {
        Some(dst.ip())
    }
}

pub fn udp_endpoint_to_socket_addr(
    endpoint: ip_capnp::udp_endpoint::Reader<'_>,
) -> Result<SocketAddr, capnp::Error> {
    let address = endpoint.get_address()?;
    Ok(SocketAddr::new(ip_address_to_ip_addr(address), endpoint.get_port()))
}

pub fn ip_address_to_ip_addr(address: ip_capnp::ip_address::Reader<'_>) -> IpAddr {
    let upper = address.get_upper64();
    let lower = address.get_lower64();

    if upper == 0 && (lower >> 32) == 0x0000_ffff {
        let ipv4 = (lower & 0xffff_ffff) as u32;
        return IpAddr::V4(Ipv4Addr::from(ipv4.to_be_bytes()));
    }

    let mut octets = [0u8; 16];
    octets[..8].copy_from_slice(&upper.to_be_bytes());
    octets[8..].copy_from_slice(&lower.to_be_bytes());
    IpAddr::V6(Ipv6Addr::from(octets))
}

pub fn write_ip_addr(mut builder: ip_capnp::ip_address::Builder<'_>, address: IpAddr) {
    match address {
        IpAddr::V4(ipv4) => {
            let value = u32::from_be_bytes(ipv4.octets()) as u64;
            builder.set_upper64(0);
            builder.set_lower64(0x0000_ffff_0000_0000 | value);
        }
        IpAddr::V6(ipv6) => {
            let octets = ipv6.octets();
            builder.set_upper64(u64::from_be_bytes(octets[..8].try_into().unwrap()));
            builder.set_lower64(u64::from_be_bytes(octets[8..].try_into().unwrap()));
        }
    }
}

pub fn write_socket_addr(mut builder: ip_capnp::udp_endpoint::Builder<'_>, address: SocketAddr) {
    builder.set_port(address.port());
    write_ip_addr(builder.reborrow().init_address(), address.ip());
}

pub fn ecn_from_capnp(ecn: ip_capnp::Ecn) -> Option<EcnCodepoint> {
    match ecn {
        ip_capnp::Ecn::NotEct => None,
        ip_capnp::Ecn::Ect0 => Some(EcnCodepoint::Ect0),
        ip_capnp::Ecn::Ect1 => Some(EcnCodepoint::Ect1),
        ip_capnp::Ecn::Ce => Some(EcnCodepoint::Ce),
    }
}

pub fn ecn_to_capnp(ecn: Option<EcnCodepoint>) -> ip_capnp::Ecn {
    match ecn {
        None => ip_capnp::Ecn::NotEct,
        Some(EcnCodepoint::Ect0) => ip_capnp::Ecn::Ect0,
        Some(EcnCodepoint::Ect1) => ip_capnp::Ecn::Ect1,
        Some(EcnCodepoint::Ce) => ip_capnp::Ecn::Ce,
    }
}

pub async fn new_capnp_raw_udp_backend(
    socket: ip_capnp::raw_udp_socket::Client,
) -> io::Result<Arc<ProxyUdpBackend>> {
    new_capnp_raw_udp_backend_with_capacity(socket, DEFAULT_SEND_QUEUE_CAPACITY).await
}

pub async fn new_capnp_raw_udp_custom_transport(
    socket: ip_capnp::raw_udp_socket::Client,
    transport_id: u64,
) -> io::Result<Arc<SandstormCustomTransport>> {
    new_capnp_raw_udp_custom_transport_with_capacity(
        socket,
        transport_id,
        DEFAULT_CUSTOM_TRANSPORT_SEND_QUEUE_CAPACITY,
    )
    .await
}

pub async fn new_capnp_raw_udp_custom_transport_with_capacity(
    socket: ip_capnp::raw_udp_socket::Client,
    transport_id: u64,
    send_queue_capacity: usize,
) -> io::Result<Arc<SandstormCustomTransport>> {
    let local_addr = get_local_endpoint(&socket).await?;
    eprintln!(
        "raw udp custom transport: local endpoint is {} transport_id=0x{transport_id:016x}",
        local_addr
    );
    let (transport, driver) =
        new_sandstorm_custom_transport(local_addr, transport_id, send_queue_capacity);
    tokio::task::spawn_local(run_capnp_raw_udp_custom_transport_driver(socket, driver));
    Ok(transport)
}

pub async fn new_capnp_raw_udp_backend_with_capacity(
    socket: ip_capnp::raw_udp_socket::Client,
    send_queue_capacity: usize,
) -> io::Result<Arc<ProxyUdpBackend>> {
    let local_addr = get_local_endpoint(&socket).await?;
    let capabilities = get_capabilities(&socket).await?;
    let (backend, driver) = new_proxy_udp_backend(local_addr, capabilities, send_queue_capacity);
    tokio::task::spawn_local(run_capnp_raw_udp_driver(socket, driver));

    Ok(backend)
}

pub async fn get_local_endpoint(socket: &ip_capnp::raw_udp_socket::Client) -> io::Result<SocketAddr> {
    let response = socket
        .get_local_endpoint_request()
        .send()
        .promise
        .await
        .map_err(capnp_to_io_error)?;
    let endpoint = response
        .get()
        .map_err(capnp_to_io_error)?
        .get_endpoint()
        .map_err(capnp_to_io_error)?;
    udp_endpoint_to_socket_addr(endpoint).map_err(capnp_to_io_error)
}

pub async fn get_capabilities(
    socket: &ip_capnp::raw_udp_socket::Client,
) -> io::Result<SandstormUdpCapabilities> {
    let response = socket
        .get_capabilities_request()
        .send()
        .promise
        .await
        .map_err(capnp_to_io_error)?;
    let capabilities = response
        .get()
        .map_err(capnp_to_io_error)?
        .get_capabilities()
        .map_err(capnp_to_io_error)?;
    Ok(SandstormUdpCapabilities {
        may_fragment: capabilities.get_may_fragment(),
        max_receive_segments: usize::from(capabilities.get_max_receive_segments()).max(1),
        max_transmit_segments: usize::from(capabilities.get_max_transmit_segments()).max(1),
    })
}

async fn run_capnp_raw_udp_driver(socket: ip_capnp::raw_udp_socket::Client, driver: ProxyUdpDriver) {
    let receiver_client = new_raw_udp_receiver_client(Arc::new(driver.clone()));
    let register_result = async {
        let mut request = socket.set_receiver_request();
        request.get().set_receiver(receiver_client);
        request.send().promise.await.map_err(capnp_to_io_error)
    }
    .await;

    if let Err(err) = register_result {
        close_driver(&driver, err);
        return;
    }

    loop {
        let packet = match driver.next_outgoing_packet().await {
            Ok(packet) => packet,
            Err(err) => {
                close_driver(&driver, err);
                return;
            }
        };

        if let Err(err) = send_packet(&socket, &packet).await {
            close_driver(&driver, err);
            return;
        }
    }
}

async fn run_capnp_raw_udp_custom_transport_driver(
    socket: ip_capnp::raw_udp_socket::Client,
    driver: SandstormCustomTransportDriver,
) {
    let receiver_client = new_raw_udp_receiver_client(Arc::new(driver.clone()));
    let register_result = async {
        let mut request = socket.set_receiver_request();
        request.get().set_receiver(receiver_client);
        request.send().promise.await.map_err(capnp_to_io_error)
    }
    .await;

    if let Err(err) = register_result {
        close_custom_transport_driver(&driver, err);
        return;
    }

    loop {
        let packet = match driver.next_outgoing_packet().await {
            Ok(packet) => packet,
            Err(err) => {
                close_custom_transport_driver(&driver, err);
                return;
            }
        };

        if let Err(err) = send_custom_transport_packet(&socket, &packet).await {
            close_custom_transport_driver(&driver, err);
            return;
        }
    }
}

async fn send_packet(
    socket: &ip_capnp::raw_udp_socket::Client,
    packet: &OwnedUdpPacket,
) -> io::Result<()> {
    let mut request = socket.send_request();
    {
        let params = request.get();
        let mut builder = params.init_packet();
        builder.set_payload(&packet.payload);
        builder.set_ecn(ecn_to_capnp(packet.ecn));
        write_socket_addr(builder.reborrow().init_dst(), packet.dst);
        write_socket_addr(
            builder.reborrow().init_src(),
            packet.src.unwrap_or(packet.dst),
        );
    }

    request.send().promise.await.map_err(capnp_to_io_error)?;
    Ok(())
}

async fn send_custom_transport_packet(
    socket: &ip_capnp::raw_udp_socket::Client,
    packet: &crate::sandstorm_custom_transport::OwnedUdpPacket,
) -> io::Result<()> {
    let mut request = socket.send_request();
    {
        let params = request.get();
        let mut builder = params.init_packet();
        builder.set_payload(&packet.payload);
        builder.set_ecn(ecn_to_capnp(noq_ecn_to_quinn_ecn(packet.ecn)));
        write_socket_addr(builder.reborrow().init_dst(), packet.dst);
        write_socket_addr(builder.reborrow().init_src(), packet.src);
    }

    request.send().promise.await.map_err(capnp_to_io_error)?;
    Ok(())
}

fn close_driver(driver: &ProxyUdpDriver, err: io::Error) {
    driver.close();
    eprintln!("raw udp backend closed: {err}");
}

fn close_custom_transport_driver(driver: &SandstormCustomTransportDriver, err: io::Error) {
    driver.close();
    eprintln!("raw udp custom transport closed: {err}");
}

fn noq_ecn_to_quinn_ecn(ecn: Option<noq_udp::EcnCodepoint>) -> Option<quinn_udp::EcnCodepoint> {
    match ecn {
        None => None,
        Some(noq_udp::EcnCodepoint::Ect0) => Some(quinn_udp::EcnCodepoint::Ect0),
        Some(noq_udp::EcnCodepoint::Ect1) => Some(quinn_udp::EcnCodepoint::Ect1),
        Some(noq_udp::EcnCodepoint::Ce) => Some(quinn_udp::EcnCodepoint::Ce),
    }
}

fn capnp_to_io_error(err: capnp::Error) -> io::Error {
    io::Error::other(err)
}

impl SandstormUdpReceiver for ProxyUdpDriver {
    fn receive_packet(&self, packet: OwnedUdpPacket) {
        ProxyUdpDriver::receive_packet(self, packet);
    }

    fn close(&self) {
        ProxyUdpDriver::close(self);
    }
}

impl SandstormUdpReceiver for SandstormCustomTransportDriver {
    fn receive_packet(&self, packet: OwnedUdpPacket) {
        SandstormCustomTransportDriver::receive_packet(
            self,
            crate::sandstorm_custom_transport::OwnedUdpPacket {
                payload: packet.payload,
                src: packet.src.unwrap_or(packet.dst),
                dst: packet.dst,
                ecn: quinn_ecn_to_noq_ecn(packet.ecn),
                dst_ip: packet.dst_ip,
            },
        );
    }

    fn close(&self) {
        SandstormCustomTransportDriver::close(self);
    }
}

fn quinn_ecn_to_noq_ecn(ecn: Option<quinn_udp::EcnCodepoint>) -> Option<noq_udp::EcnCodepoint> {
    match ecn {
        None => None,
        Some(quinn_udp::EcnCodepoint::Ect0) => Some(noq_udp::EcnCodepoint::Ect0),
        Some(quinn_udp::EcnCodepoint::Ect1) => Some(noq_udp::EcnCodepoint::Ect1),
        Some(quinn_udp::EcnCodepoint::Ce) => Some(noq_udp::EcnCodepoint::Ce),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::io::IoSliceMut;
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use futures::task::noop_waker;
    use iroh::endpoint::transports::CustomTransport;
    use n0_watcher::Watcher;
    use quinn::AsyncUdpSocket;
    use crate::quinn_adapter::{SandstormQuinnUdpSocket, SandstormUdpSocketBackend};
    use crate::sandstorm_custom_transport::SANDSTORM_RAW_UDP_TRANSPORT_ID;

    #[test]
    fn ip_addr_roundtrips_ipv4_through_capnp_wire_shape() {
        let mut message = capnp::message::Builder::new_default();
        let builder = message.init_root::<ip_capnp::ip_address::Builder<'_>>();
        write_ip_addr(builder, IpAddr::V4(Ipv4Addr::new(192, 0, 2, 99)));
        let reader = message
            .get_root_as_reader::<ip_capnp::ip_address::Reader<'_>>()
            .unwrap();
        assert_eq!(ip_address_to_ip_addr(reader), IpAddr::V4(Ipv4Addr::new(192, 0, 2, 99)));
    }

    #[test]
    fn ip_addr_roundtrips_ipv6_through_capnp_wire_shape() {
        let mut message = capnp::message::Builder::new_default();
        let builder = message.init_root::<ip_capnp::ip_address::Builder<'_>>();
        let addr = IpAddr::V6("2001:db8::1234".parse().unwrap());
        write_ip_addr(builder, addr);
        let reader = message
            .get_root_as_reader::<ip_capnp::ip_address::Reader<'_>>()
            .unwrap();
        assert_eq!(ip_address_to_ip_addr(reader), addr);
    }

    #[test]
    fn udp_endpoint_roundtrips_socket_addr() {
        let mut message = capnp::message::Builder::new_default();
        let builder = message.init_root::<ip_capnp::udp_endpoint::Builder<'_>>();
        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        write_socket_addr(builder, addr);
        let reader = message
            .get_root_as_reader::<ip_capnp::udp_endpoint::Reader<'_>>()
            .unwrap();
        assert_eq!(udp_endpoint_to_socket_addr(reader).unwrap(), addr);
    }

    #[test]
    fn normalize_received_dst_ip_drops_unspecified_addresses() {
        assert_eq!(
            normalize_received_dst_ip(SocketAddr::from(([0, 0, 0, 0], 9999))),
            None
        );
        assert_eq!(
            normalize_received_dst_ip("[::]:9999".parse().unwrap()),
            None
        );
        assert_eq!(
            normalize_received_dst_ip(SocketAddr::from(([127, 0, 0, 1], 9999))),
            Some(IpAddr::from([127, 0, 0, 1]))
        );
    }

    #[test]
    fn ecn_conversion_roundtrips() {
        let all = [
            None,
            Some(EcnCodepoint::Ect0),
            Some(EcnCodepoint::Ect1),
            Some(EcnCodepoint::Ce),
        ];
        for value in all {
            assert_eq!(ecn_from_capnp(ecn_to_capnp(value)), value);
        }
    }

    struct MockRawUdpSocket {
        local_addr: SocketAddr,
        capabilities: SandstormUdpCapabilities,
        state: Mutex<MockRawUdpSocketState>,
    }

    #[derive(Default)]
    struct MockRawUdpSocketState {
        receiver: Option<ip_capnp::raw_udp_receiver::Client>,
        sent_packets: VecDeque<RecordedPacket>,
        closed: bool,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RecordedPacket {
        payload: Vec<u8>,
        src: SocketAddr,
        dst: SocketAddr,
        ecn: Option<EcnCodepoint>,
    }

    impl MockRawUdpSocket {
        fn new(local_addr: SocketAddr, capabilities: SandstormUdpCapabilities) -> Rc<Self> {
            Rc::new(Self {
                local_addr,
                capabilities,
                state: Mutex::new(MockRawUdpSocketState::default()),
            })
        }

        fn client(self: &Rc<Self>) -> ip_capnp::raw_udp_socket::Client {
            new_client(MockRawUdpSocketHandle {
                inner: self.clone(),
            })
        }

        async fn inject_inbound(&self, packet: RecordedPacket) {
            let receiver = self
                .state
                .lock()
                .unwrap()
                .receiver
                .clone()
                .expect("receiver should be registered");
            let mut request = receiver.receive_request();
            {
                let params = request.get();
                let mut builder = params.init_packet();
                builder.set_payload(&packet.payload);
                builder.set_ecn(ecn_to_capnp(packet.ecn));
                write_socket_addr(builder.reborrow().init_src(), packet.src);
                write_socket_addr(builder.reborrow().init_dst(), packet.dst);
            }
            request.send().promise.await.unwrap();
        }

        fn take_sent_packet(&self) -> Option<RecordedPacket> {
            self.state.lock().unwrap().sent_packets.pop_front()
        }

        async fn wait_for_receiver_registration(&self) {
            for _ in 0..8 {
                if self.state.lock().unwrap().receiver.is_some() {
                    return;
                }
                tokio::task::yield_now().await;
            }
            panic!("receiver should be registered");
        }
    }

    #[derive(Clone)]
    struct MockRawUdpSocketHandle {
        inner: Rc<MockRawUdpSocket>,
    }

    impl ip_capnp::raw_udp_socket::Server for MockRawUdpSocketHandle {
        fn send(
            self: Rc<Self>,
            params: ip_capnp::raw_udp_socket::SendParams,
            _: ip_capnp::raw_udp_socket::SendResults,
        ) -> Promise<(), capnp::Error> {
            let params = pry!(params.get());
            let packet = pry!(params.get_packet());
            let payload = pry!(packet.get_payload()).to_vec();
            let src = pry!(udp_endpoint_to_socket_addr(pry!(packet.get_src())));
            let dst = pry!(udp_endpoint_to_socket_addr(pry!(packet.get_dst())));
            let ecn = packet.get_ecn().ok().and_then(ecn_from_capnp);
            self.inner
                .state
                .lock()
                .unwrap()
                .sent_packets
                .push_back(RecordedPacket {
                payload,
                src,
                dst,
                ecn,
            });
            Promise::ok(())
        }

        fn set_receiver(
            self: Rc<Self>,
            params: ip_capnp::raw_udp_socket::SetReceiverParams,
            _: ip_capnp::raw_udp_socket::SetReceiverResults,
        ) -> Promise<(), capnp::Error> {
            let params = pry!(params.get());
            self.inner.state.lock().unwrap().receiver = Some(pry!(params.get_receiver()));
            Promise::ok(())
        }

        fn get_local_endpoint(
            self: Rc<Self>,
            _: ip_capnp::raw_udp_socket::GetLocalEndpointParams,
            mut results: ip_capnp::raw_udp_socket::GetLocalEndpointResults,
        ) -> Promise<(), capnp::Error> {
            write_socket_addr(results.get().init_endpoint(), self.inner.local_addr);
            Promise::ok(())
        }

        fn get_capabilities(
            self: Rc<Self>,
            _: ip_capnp::raw_udp_socket::GetCapabilitiesParams,
            mut results: ip_capnp::raw_udp_socket::GetCapabilitiesResults,
        ) -> Promise<(), capnp::Error> {
            let mut capabilities = results.get().init_capabilities();
            capabilities.set_may_fragment(self.inner.capabilities.may_fragment);
            capabilities
                .set_max_receive_segments(self.inner.capabilities.max_receive_segments as u16);
            capabilities
                .set_max_transmit_segments(self.inner.capabilities.max_transmit_segments as u16);
            Promise::ok(())
        }

        fn close(
            self: Rc<Self>,
            _: ip_capnp::raw_udp_socket::CloseParams,
            _: ip_capnp::raw_udp_socket::CloseResults,
        ) -> Promise<(), capnp::Error> {
            self.inner.state.lock().unwrap().closed = true;
            Promise::ok(())
        }
    }

    fn test_context() -> Context<'static> {
        let waker = Box::leak(Box::new(noop_waker()));
        Context::from_waker(waker)
    }

    fn run_local_test<F>(future: F)
    where
        F: std::future::Future<Output = ()> + 'static,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(future));
    }

    #[test]
    fn capnp_raw_udp_backend_bridges_send_and_receive() {
        run_local_test(async {
            let local_addr = SocketAddr::from(([127, 0, 0, 1], 4000));
            let remote_addr = SocketAddr::from(([10, 0, 0, 9], 9000));
            let server = MockRawUdpSocket::new(
                local_addr,
                SandstormUdpCapabilities {
                    may_fragment: true,
                    max_receive_segments: 1,
                    max_transmit_segments: 1,
                },
            );
            let backend = new_capnp_raw_udp_backend_with_capacity(server.client(), 4)
                .await
                .unwrap();
            let socket = SandstormQuinnUdpSocket::new(backend.clone()).unwrap();
            server.wait_for_receiver_registration().await;

            backend
                .try_send_packet(&OwnedUdpPacket {
                    payload: b"ping".to_vec(),
                    src: Some(local_addr),
                    dst: remote_addr,
                    ecn: Some(EcnCodepoint::Ect0),
                    dst_ip: Some(remote_addr.ip()),
                })
                .unwrap();

            tokio::task::yield_now().await;

            let sent = server.take_sent_packet().expect("sent packet should be captured");
            assert_eq!(sent.payload, b"ping");
            assert_eq!(sent.src, local_addr);
            assert_eq!(sent.dst, remote_addr);
            assert_eq!(sent.ecn, Some(EcnCodepoint::Ect0));

            server
                .inject_inbound(RecordedPacket {
                    payload: b"pong".to_vec(),
                    src: remote_addr,
                    dst: local_addr,
                    ecn: Some(EcnCodepoint::Ce),
                })
                .await;

            let mut recv_buf = [0u8; 32];
            let mut bufs = [IoSliceMut::new(&mut recv_buf)];
            let mut metas = [quinn_udp::RecvMeta::default()];
            let mut cx = test_context();
            let n = match socket.poll_recv(&mut cx, &mut bufs, &mut metas) {
                Poll::Ready(Ok(n)) => n,
                other => panic!("unexpected poll result: {other:?}"),
            };
            assert_eq!(n, 1);
            assert_eq!(&recv_buf[..metas[0].len], b"pong");
            assert_eq!(metas[0].addr, remote_addr);
            assert_eq!(metas[0].dst_ip, Some(local_addr.ip()));
            assert_eq!(metas[0].ecn, Some(EcnCodepoint::Ce));
        });
    }

    #[test]
    fn capnp_raw_udp_custom_transport_registers_and_receives_packets() {
        run_local_test(async {
            let local_addr = SocketAddr::from(([127, 0, 0, 1], 5000));
            let remote_addr = SocketAddr::from(([10, 1, 2, 3], 6000));
            let server = MockRawUdpSocket::new(
                local_addr,
                SandstormUdpCapabilities {
                    may_fragment: false,
                    max_receive_segments: 1,
                    max_transmit_segments: 1,
                },
            );
            let transport = new_capnp_raw_udp_custom_transport_with_capacity(
                server.client(),
                SANDSTORM_RAW_UDP_TRANSPORT_ID,
                4,
            )
            .await
            .unwrap();
            let mut endpoint = transport.bind().unwrap();
            server.wait_for_receiver_registration().await;
            let local_addrs = endpoint.watch_local_addrs().get();
            assert_eq!(local_addrs.len(), 1);
            let mut cx = test_context();
            assert_eq!(
                crate::sandstorm_custom_transport::custom_addr_to_socket_addr(&local_addrs[0])
                    .unwrap(),
                local_addr
            );

            server
                .inject_inbound(RecordedPacket {
                    payload: b"reply".to_vec(),
                    src: remote_addr,
                    dst: local_addr,
                    ecn: Some(EcnCodepoint::Ect1),
                })
                .await;

            let mut recv_buf = [0u8; 32];
            let mut bufs = [IoSliceMut::new(&mut recv_buf)];
            let mut metas = [noq_udp::RecvMeta::default()];
            let mut addrs = [iroh::endpoint::transports::Addr::default()];
            let n = match endpoint.poll_recv(&mut cx, &mut bufs, &mut metas, &mut addrs) {
                Poll::Ready(Ok(n)) => n,
                other => panic!("unexpected poll result: {other:?}"),
            };
            assert_eq!(n, 1);
            assert_eq!(&recv_buf[..metas[0].len], b"reply");
            assert_eq!(metas[0].addr, remote_addr);
            assert!(matches!(addrs[0], iroh::endpoint::transports::Addr::Custom(_)));
        });
    }
}
