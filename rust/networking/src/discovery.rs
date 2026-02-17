use crate::ext::MultiaddrExt;
use crate::swarm::NetworkConfig;
use futures::FutureExt;
use futures_timer::Delay;
use libp2p::core::transport::PortUse;
use libp2p::core::{ConnectedPoint, Endpoint};
use libp2p::swarm::behaviour::ConnectionEstablished;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{
    CloseConnection, ConnectionClosed, ConnectionDenied, ConnectionId, FromSwarm,
    NetworkBehaviour, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{identity, ping, Multiaddr, PeerId};
use std::collections::HashMap;
use std::convert::Infallible;
use std::io;
use std::net::IpAddr;
use std::task::{Context, Poll};
use std::time::Duration;
use util::wakerdeque::WakerDeque;

const RETRY_CONNECT_INTERVAL: Duration = Duration::from_secs(5);
const PING_TIMEOUT: Duration = Duration::from_millis(2_500);
const PING_INTERVAL: Duration = Duration::from_millis(2_500);

/// Events for when a listening connection is truly established and truly closed.
#[derive(Debug, Clone)]
pub enum Event {
    ConnectionEstablished {
        peer_id: PeerId,
        connection_id: ConnectionId,
        remote_ip: IpAddr,
        remote_tcp_port: u16,
    },
    ConnectionClosed {
        peer_id: PeerId,
        connection_id: ConnectionId,
        remote_ip: IpAddr,
        remote_tcp_port: u16,
    },
}

/// Discovery behavior that uses static peer configuration to establish connections.
///
/// The behaviour operates as such:
///  1) All true (listening) connections/disconnections are tracked, emitting corresponding events
///     to the swarm.
///  2) Static peers are dialed on startup and periodically retried if disconnected.
///  3) Ping is used to detect unresponsive peers and close dead connections.
pub struct Behaviour {
    ping: ping::Behaviour,
    static_peers: Vec<(IpAddr, u16)>,
    connected_peers: HashMap<PeerId, ConnectionId>,
    retry_delay: Delay,
    pending_events: WakerDeque<ToSwarm<Event, Infallible>>,
    initial_dial_done: bool,
}

impl Behaviour {
    pub fn new(_keypair: &identity::Keypair, config: NetworkConfig) -> io::Result<Self> {
        Ok(Self {
            ping: ping::Behaviour::new(
                ping::Config::new()
                    .with_timeout(PING_TIMEOUT)
                    .with_interval(PING_INTERVAL),
            ),
            static_peers: config.peers,
            connected_peers: HashMap::new(),
            retry_delay: Delay::new(RETRY_CONNECT_INTERVAL),
            pending_events: WakerDeque::new(),
            initial_dial_done: false,
        })
    }

    fn dial_peer(&mut self, ip: IpAddr, port: u16) {
        use libp2p::multiaddr::Protocol;

        let addr = Multiaddr::empty()
            .with(Protocol::from(ip))
            .with(Protocol::Tcp(port));

        self.pending_events.push_back(ToSwarm::Dial {
            opts: DialOpts::unknown_peer_id().address(addr).build(),
        });
    }

    fn close_connection(&mut self, peer_id: PeerId, connection: ConnectionId) {
        self.pending_events.push_front(ToSwarm::CloseConnection {
            peer_id,
            connection: CloseConnection::One(connection),
        })
    }

    fn on_connection_established(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        remote_ip: IpAddr,
        remote_tcp_port: u16,
    ) {
        self.connected_peers.insert(peer_id, connection_id);
        self.pending_events
            .push_back(ToSwarm::GenerateEvent(Event::ConnectionEstablished {
                peer_id,
                connection_id,
                remote_ip,
                remote_tcp_port,
            }));
    }

    fn on_connection_closed(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        remote_ip: IpAddr,
        remote_tcp_port: u16,
    ) {
        self.connected_peers.remove(&peer_id);
        self.pending_events
            .push_back(ToSwarm::GenerateEvent(Event::ConnectionClosed {
                peer_id,
                connection_id,
                remote_ip,
                remote_tcp_port,
            }));
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = THandler<ping::Behaviour>;
    type ToSwarm = Event;

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.ping
            .handle_pending_inbound_connection(connection_id, local_addr, remote_addr)
    }

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr],
        effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        self.ping.handle_pending_outbound_connection(
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.ping.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
        port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.ping.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
            port_use,
        )
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.ping
            .on_connection_handler_event(peer_id, connection_id, event)
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        self.ping.on_swarm_event(event);

        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                ..
            }) => {
                let remote_address = match endpoint {
                    ConnectedPoint::Dialer { address, .. } => address,
                    ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
                };

                if let Some((ip, port)) = remote_address.try_to_tcp_addr() {
                    self.on_connection_established(peer_id, connection_id, ip, port)
                }
            }
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                connection_id,
                endpoint,
                ..
            }) => {
                let remote_address = match endpoint {
                    ConnectedPoint::Dialer { address, .. } => address,
                    ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
                };

                if let Some((ip, port)) = remote_address.try_to_tcp_addr() {
                    self.on_connection_closed(peer_id, connection_id, ip, port)
                }
            }
            FromSwarm::AddressChange(a) => {
                unreachable!("unhandlable: address change encountered: {:?}", a)
            }
            _ => {}
        }
    }

    fn poll(&mut self, cx: &mut Context) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        // Initial dial to all static peers (only on first poll)
        if !self.initial_dial_done {
            let peers = self.static_peers.clone();
            for (ip, port) in peers {
                self.dial_peer(ip, port);
            }
            self.initial_dial_done = true;
        }

        // Handle ping events
        loop {
            match self.ping.poll(cx) {
                Poll::Ready(ToSwarm::GenerateEvent(e)) => {
                    // If ping fails, close the connection
                    if e.result.is_err() {
                        self.close_connection(e.peer, e.connection);
                    }
                }
                Poll::Ready(e) => {
                    return Poll::Ready(e.map_out(|_| {
                        unreachable!("ping events should only be GenerateEvent")
                    }));
                }
                Poll::Pending => break,
            }
        }

        // Periodic retry for disconnected peers
        if self.retry_delay.poll_unpin(cx).is_ready() {
            let peers = self.static_peers.clone();
            for (ip, port) in peers {
                self.dial_peer(ip, port);
            }
            self.retry_delay.reset(RETRY_CONNECT_INTERVAL);
        }

        // Send out any pending events
        if let Some(e) = self.pending_events.pop_front(cx) {
            return Poll::Ready(e);
        }

        Poll::Pending
    }
}
