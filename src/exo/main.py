import argparse
import itertools
import json
import multiprocessing as mp
import os
import resource
import signal
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Self, cast

import anyio
import httpx
from anyio.abc import TaskGroup
from loguru import logger
from pydantic import PositiveInt, ValidationError

import exo.routing.topics as topics
from exo.download.coordinator import DownloadCoordinator
from exo.download.impl_shard_downloader import exo_shard_downloader
from exo.master.api import API  # TODO: should API be in master?
from exo.master.main import Master
from exo.routing.router import Router, get_node_id_keypair
from exo.shared.constants import EXO_CONFIG_HOME, EXO_LOG
from exo.shared.election import Election, ElectionResult
from exo.shared.logging import logger_cleanup, logger_setup
from exo.shared.types.common import NodeId, SessionId
from exo.shared.types.network_config import NetworkConfig
from exo.shared.types.state import State
from exo.utils.channels import Receiver, channel
from exo.utils.pydantic_ext import CamelCaseModel
from exo.worker.main import Worker


def get_node_host() -> str:
    """Get the node's external IP address by connecting to a public DNS server."""
    try:
        # Create a socket to determine the local IP used for external connections
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            # Connect to Google's public DNS (doesn't actually send data)
            s.connect(("8.8.8.8", 80))
            sockname = s.getsockname()  # pyright: ignore[reportAny]
            return str(sockname[0])  # pyright: ignore[reportAny]
    except Exception:
        # Fallback to localhost if detection fails
        return "127.0.0.1"


def get_active_models(state: State) -> list[str]:
    """Extract unique model names from active model instances."""
    return list({instance.shard_assignments.model_id for instance in state.instances.values()})


async def register_with_orchestrator(
    orchestrator: str,
    node_name: str,
    node_host: str,
    node_port: int,
    active_models: list[str] | None = None,
) -> NetworkConfig:
    """Register with orchestrator server and get peer configuration."""
    # Parse orchestrator address
    if ":" not in orchestrator:
        raise ValueError(
            f"Orchestrator address must be in format 'ip:port', got: {orchestrator}"
        )

    orch_host, orch_port_str = orchestrator.rsplit(":", 1)
    try:
        orch_port = int(orch_port_str)
    except ValueError as e:
        raise ValueError(
            f"Invalid orchestrator port '{orch_port_str}', must be an integer"
        ) from e

    orchestrator_url = f"http://{orch_host}:{orch_port}/register"

    # Prepare registration payload
    payload = {
        "name": node_name,
        "ip": node_host,
        "port": node_port,
        "active_models": active_models or [],
    }

    logger.info(
        f"Registering with orchestrator at {orchestrator_url} as {node_name} ({node_host}:{node_port})"
    )

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(orchestrator_url, json=payload)
            response.raise_for_status()
            data: object = response.json()  # pyright: ignore[reportAny]
            return NetworkConfig.model_validate(data)
    except httpx.HTTPError as e:
        raise RuntimeError(f"Failed to register with orchestrator: {e}") from e
    except ValidationError as e:
        raise ValueError(f"Invalid peer configuration from orchestrator: {e}") from e


async def load_network_config_from_file(peers_file: str | None) -> NetworkConfig:
    """Load and validate peer configuration from JSON file."""
    if peers_file is None:
        # Check default location
        default_path = Path(EXO_CONFIG_HOME) / "peers.json"
        if not default_path.exists():
            raise RuntimeError(
                "No peer configuration file found. "
                f"Create {default_path} or specify --peers-file. "
                "See docs/peers-config.md for format."
            )
        peers_file = str(default_path)

    path = Path(peers_file)
    if not path.exists():
        raise FileNotFoundError(f"Peer config file not found: {peers_file}")

    try:
        content = await anyio.Path(path).read_text()
        data: object = json.loads(content)  # pyright: ignore[reportAny]
        config = NetworkConfig.model_validate(data)

        # File mode requires at least one peer
        if len(config.peers) == 0:
            raise ValueError("At least one peer must be specified in file mode")

        return config
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in peer config: {e}") from e
    except ValidationError as e:
        raise ValueError(f"Invalid peer configuration: {e}") from e


async def load_network_config(args: "Args") -> NetworkConfig:
    """Load network configuration from orchestrator or file."""
    if args.orchestrator:
        # Orchestrator mode - register and get peers
        node_name = args.node_name or socket.gethostname()
        node_host = args.node_host or get_node_host()
        return await register_with_orchestrator(
            args.orchestrator,
            node_name,
            node_host,
            args.api_port,
        )
    else:
        # File mode - load from JSON file
        return await load_network_config_from_file(args.peers_file)


@dataclass
class Node:
    router: Router
    download_coordinator: DownloadCoordinator | None
    worker: Worker | None
    election: Election  # Every node participates in election, as we do want a node to become master even if it isn't a master candidate if no master candidates are present.
    election_result_receiver: Receiver[ElectionResult]
    master: Master | None
    api: API | None

    node_id: NodeId
    event_index_counter: Iterator[int]
    orchestrator: str | None  # Orchestrator address if in orchestrator mode
    node_name: str | None = None  # Node name for orchestrator registration
    node_host: str | None = None  # Node host IP for orchestrator registration
    known_peer_addresses: set[tuple[str, int]] = field(
        default_factory=set
    )  # Track known peers to avoid re-dialing
    active_models: list[str] = field(
        default_factory=list
    )  # List of active model names (updated from worker state)
    _tg: TaskGroup = field(init=False, default_factory=anyio.create_task_group)

    @classmethod
    async def create(cls, args: "Args") -> "Self":
        keypair = get_node_id_keypair()
        node_id = NodeId(keypair.to_peer_id().to_base58())
        session_id = SessionId(master_node_id=node_id, election_clock=0)

        # Store node name and host for orchestrator registration
        node_name = args.node_name or socket.gethostname() if args.orchestrator else None
        node_host = args.node_host or get_node_host() if args.orchestrator else None

        # In orchestrator mode, we need to get the listening port first before registering
        # So create router with empty peer list initially
        if args.orchestrator:
            from exo.shared.types.network_config import NetworkConfig, PeerAddress

            empty_config = NetworkConfig(peers=[PeerAddress(ip="127.0.0.1", port=1)])
            router = Router.create(keypair, empty_config)

            # Wait for router to start listening and get the port
            listening_port = None
            for _ in range(50):  # Try for up to 5 seconds
                listening_port = router._net.get_listening_port()  # type: ignore[attr-defined]
                if listening_port:
                    break
                await anyio.sleep(0.1)

            if not listening_port:
                raise RuntimeError(
                    "Failed to get libp2p listening port after 5 seconds"
                )

            # Type narrowing: listening_port is now definitely int
            libp2p_port = cast(int, listening_port)

            logger.info(f"libp2p listening on port {libp2p_port}")

            # Now register with orchestrator using the actual libp2p port
            assert node_name is not None
            assert node_host is not None
            network_config = await register_with_orchestrator(
                args.orchestrator,
                node_name,
                node_host,
                libp2p_port,
                active_models=[],  # No models loaded yet on initial registration
            )

            # Dial the peers we got from orchestrator
            for peer in network_config.peers:
                try:
                    await router.dial_peer(peer.ip, peer.port)
                    logger.info(f"Dialing peer from orchestrator: {peer.ip}:{peer.port}")
                except Exception as e:
                    logger.error(f"Failed to dial peer {peer.ip}:{peer.port}: {e}")
        else:
            # File mode - load config and create router
            network_config = await load_network_config_from_file(args.peers_file)
            router = Router.create(keypair, network_config)
        await router.register_topic(topics.GLOBAL_EVENTS)
        await router.register_topic(topics.LOCAL_EVENTS)
        await router.register_topic(topics.COMMANDS)
        await router.register_topic(topics.ELECTION_MESSAGES)
        await router.register_topic(topics.CONNECTION_MESSAGES)
        await router.register_topic(topics.DOWNLOAD_COMMANDS)

        logger.info(f"Starting node {node_id}")

        # Create shared event index counter for Worker and DownloadCoordinator
        event_index_counter = itertools.count()

        # Create DownloadCoordinator (unless --no-downloads)
        if not args.no_downloads:
            download_coordinator = DownloadCoordinator(
                node_id,
                session_id,
                exo_shard_downloader(),
                download_command_receiver=router.receiver(topics.DOWNLOAD_COMMANDS),
                local_event_sender=router.sender(topics.LOCAL_EVENTS),
                event_index_counter=event_index_counter,
            )
        else:
            download_coordinator = None

        if args.spawn_api:
            api = API(
                node_id,
                session_id,
                port=args.api_port,
                global_event_receiver=router.receiver(topics.GLOBAL_EVENTS),
                command_sender=router.sender(topics.COMMANDS),
                download_command_sender=router.sender(topics.DOWNLOAD_COMMANDS),
                election_receiver=router.receiver(topics.ELECTION_MESSAGES),
            )
        else:
            api = None

        if not args.no_worker:
            worker = Worker(
                node_id,
                session_id,
                global_event_receiver=router.receiver(topics.GLOBAL_EVENTS),
                local_event_sender=router.sender(topics.LOCAL_EVENTS),
                command_sender=router.sender(topics.COMMANDS),
                download_command_sender=router.sender(topics.DOWNLOAD_COMMANDS),
                event_index_counter=event_index_counter,
            )
        else:
            worker = None

        # We start every node with a master
        master = Master(
            node_id,
            session_id,
            global_event_sender=router.sender(topics.GLOBAL_EVENTS),
            local_event_receiver=router.receiver(topics.LOCAL_EVENTS),
            command_receiver=router.receiver(topics.COMMANDS),
            download_command_sender=router.sender(topics.DOWNLOAD_COMMANDS),
        )

        er_send, er_recv = channel[ElectionResult]()
        election = Election(
            node_id,
            # If someone manages to assemble 1 MILLION devices into an exo cluster then. well done. good job champ.
            seniority=1_000_000 if args.force_master else 0,
            # nb: this DOES feedback right now. i have thoughts on how to address this,
            # but ultimately it seems not worth the complexity
            election_message_sender=router.sender(topics.ELECTION_MESSAGES),
            election_message_receiver=router.receiver(topics.ELECTION_MESSAGES),
            connection_message_receiver=router.receiver(topics.CONNECTION_MESSAGES),
            command_receiver=router.receiver(topics.COMMANDS),
            election_result_sender=er_send,
        )

        # Track initial peers
        known_peer_addresses = {(peer.ip, peer.port) for peer in network_config.peers}

        return cls(
            router,
            download_coordinator,
            worker,
            election,
            er_recv,
            master,
            api,
            node_id,
            event_index_counter,
            orchestrator=args.orchestrator,
            node_name=node_name,
            node_host=node_host,
            known_peer_addresses=known_peer_addresses,
        )

    async def run(self):
        async with self._tg as tg:
            tg.start_soon(self.router.run)
            tg.start_soon(self.election.run)
            if self.download_coordinator:
                tg.start_soon(self.download_coordinator.run)
            if self.worker:
                tg.start_soon(self.worker.run)
            if self.master:
                tg.start_soon(self.master.run)
            if self.api:
                tg.start_soon(self.api.run)
            tg.start_soon(self._elect_loop)
            if self.orchestrator:
                tg.start_soon(self._peer_refresh_loop)
            signal.signal(signal.SIGINT, lambda _, __: self.shutdown())
            signal.signal(signal.SIGTERM, lambda _, __: self.shutdown())

    def shutdown(self):
        # if this is our second call to shutdown, just sys.exit
        if self._tg.cancel_scope.cancel_called:
            import sys

            sys.exit(1)
        self._tg.cancel_scope.cancel()

    async def _elect_loop(self):
        with self.election_result_receiver as results:
            async for result in results:
                # This function continues to have a lot of very specific entangled logic
                # At least it's somewhat contained

                # I don't like this duplication, but it's manageable for now.
                # TODO: This function needs refactoring generally

                # Ok:
                # On new master:
                # - Elect master locally if necessary
                # - Shutdown and re-create the worker
                # - Shut down and re-create the API

                if (
                    result.session_id.master_node_id == self.node_id
                    and self.master is not None
                ):
                    logger.info("Node elected Master")
                elif (
                    result.session_id.master_node_id == self.node_id
                    and self.master is None
                ):
                    logger.info("Node elected Master - promoting self")
                    self.master = Master(
                        self.node_id,
                        result.session_id,
                        global_event_sender=self.router.sender(topics.GLOBAL_EVENTS),
                        local_event_receiver=self.router.receiver(topics.LOCAL_EVENTS),
                        command_receiver=self.router.receiver(topics.COMMANDS),
                        download_command_sender=self.router.sender(
                            topics.DOWNLOAD_COMMANDS
                        ),
                    )
                    self._tg.start_soon(self.master.run)
                elif (
                    result.session_id.master_node_id != self.node_id
                    and self.master is not None
                ):
                    logger.info(
                        f"Node {result.session_id.master_node_id} elected master - demoting self"
                    )
                    await self.master.shutdown()
                    self.master = None
                else:
                    logger.info(
                        f"Node {result.session_id.master_node_id} elected master"
                    )
                if result.is_new_master:
                    await anyio.sleep(0)
                    # Fresh counter for new session (buffer expects indices from 0)
                    self.event_index_counter = itertools.count()
                    if self.download_coordinator:
                        self.download_coordinator.shutdown()
                        self.download_coordinator = DownloadCoordinator(
                            self.node_id,
                            result.session_id,
                            exo_shard_downloader(),
                            download_command_receiver=self.router.receiver(
                                topics.DOWNLOAD_COMMANDS
                            ),
                            local_event_sender=self.router.sender(topics.LOCAL_EVENTS),
                            event_index_counter=self.event_index_counter,
                        )
                        self._tg.start_soon(self.download_coordinator.run)
                    if self.worker:
                        self.worker.shutdown()
                        # TODO: add profiling etc to resource monitor
                        self.worker = Worker(
                            self.node_id,
                            result.session_id,
                            global_event_receiver=self.router.receiver(
                                topics.GLOBAL_EVENTS
                            ),
                            local_event_sender=self.router.sender(topics.LOCAL_EVENTS),
                            command_sender=self.router.sender(topics.COMMANDS),
                            download_command_sender=self.router.sender(
                                topics.DOWNLOAD_COMMANDS
                            ),
                            event_index_counter=self.event_index_counter,
                        )
                        self._tg.start_soon(self.worker.run)
                    if self.api:
                        self.api.reset(result.session_id, result.won_clock)
                else:
                    if self.api:
                        self.api.unpause(result.won_clock)

    async def _peer_refresh_loop(self):
        """Periodically fetch updated peer list from orchestrator and connect to new peers."""
        if not self.orchestrator:
            return

        logger.info("Starting peer refresh loop (every 30 seconds)")

        # Get node info for registration (use stored values from initial registration)
        assert self.node_name is not None
        assert self.node_host is not None
        libp2p_port = self.router._net.get_listening_port()  # type: ignore[attr-defined]

        while True:
            try:
                # Wait 30 seconds before checking for new peers
                await anyio.sleep(30)

                # Get current active models from worker state
                current_models: list[str] = []
                if self.worker:
                    current_models = get_active_models(self.worker.state)
                    logger.debug(f"Current active models: {current_models}")

                # Re-register with orchestrator (includes active models and returns peer list)
                logger.debug(
                    f"Re-registering with orchestrator: {self.orchestrator} (models: {current_models})"
                )
                network_config = await register_with_orchestrator(
                    self.orchestrator,
                    self.node_name,
                    self.node_host,
                    libp2p_port,  # type: ignore[arg-type]
                    active_models=current_models,
                )

                # Check for new peers
                from exo.shared.types.network_config import PeerAddress

                new_peers: list[PeerAddress] = []
                for peer in network_config.peers:
                    peer_addr = (peer.ip, peer.port)
                    if peer_addr not in self.known_peer_addresses:
                        new_peers.append(peer)
                        self.known_peer_addresses.add(peer_addr)

                # Dial new peers
                if new_peers:
                    logger.info(f"Found {len(new_peers)} new peer(s), connecting...")
                    for peer in new_peers:
                        try:
                            await self.router.dial_peer(peer.ip, peer.port)
                            logger.info(f"Dialing new peer: {peer.ip}:{peer.port}")
                        except Exception as e:
                            logger.error(
                                f"Failed to dial peer {peer.ip}:{peer.port}: {e}"
                            )
                else:
                    logger.debug("No new peers found")

            except Exception as e:
                logger.error(f"Error in peer refresh loop: {e}")
                # Continue the loop even if there's an error


def main():
    args = Args.parse()
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = min(max(soft, 65535), hard)
    resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))

    mp.set_start_method("spawn")
    # TODO: Refactor the current verbosity system
    logger_setup(EXO_LOG, args.verbosity)
    logger.info("Starting EXO")
    logger.info(f"EXO_LIBP2P_NAMESPACE: {os.getenv('EXO_LIBP2P_NAMESPACE')}")

    # Set FAST_SYNCH override env var for runner subprocesses
    if args.fast_synch is True:
        os.environ["EXO_FAST_SYNCH"] = "on"
        logger.info("FAST_SYNCH forced ON")
    elif args.fast_synch is False:
        os.environ["EXO_FAST_SYNCH"] = "off"
        logger.info("FAST_SYNCH forced OFF")

    node = anyio.run(Node.create, args)
    anyio.run(node.run)
    logger.info("EXO Shutdown complete")
    logger_cleanup()


class Args(CamelCaseModel):
    verbosity: int = 0
    force_master: bool = False
    spawn_api: bool = False
    api_port: PositiveInt = 52415
    tb_only: bool = False
    no_worker: bool = False
    no_downloads: bool = False
    fast_synch: bool | None = None  # None = auto, True = force on, False = force off
    peers_file: str | None = None  # Path to peers.json config
    orchestrator: str | None = None  # Orchestrator server address (ip:port)
    node_name: str | None = None  # Node name for orchestrator registration
    node_host: str | None = None  # Node's external IP address for orchestrator registration

    @classmethod
    def parse(cls) -> Self:
        parser = argparse.ArgumentParser(prog="EXO")
        default_verbosity = 0
        parser.add_argument(
            "-q",
            "--quiet",
            action="store_const",
            const=-1,
            dest="verbosity",
            default=default_verbosity,
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="count",
            dest="verbosity",
            default=default_verbosity,
        )
        parser.add_argument(
            "-m",
            "--force-master",
            action="store_true",
            dest="force_master",
        )
        parser.add_argument(
            "--no-api",
            action="store_false",
            dest="spawn_api",
        )
        parser.add_argument(
            "--api-port",
            type=int,
            dest="api_port",
            default=52415,
        )
        parser.add_argument(
            "--no-worker",
            action="store_true",
        )
        parser.add_argument(
            "--no-downloads",
            action="store_true",
            help="Disable the download coordinator (node won't download models)",
        )
        fast_synch_group = parser.add_mutually_exclusive_group()
        fast_synch_group.add_argument(
            "--fast-synch",
            action="store_true",
            dest="fast_synch",
            default=None,
            help="Force MLX FAST_SYNCH on (for JACCL backend)",
        )
        fast_synch_group.add_argument(
            "--no-fast-synch",
            action="store_false",
            dest="fast_synch",
            help="Force MLX FAST_SYNCH off",
        )
        parser.add_argument(
            "--peers-file",
            type=str,
            default=None,
            help="Path to JSON file containing peer configuration",
        )
        parser.add_argument(
            "--orchestrator",
            type=str,
            default=None,
            help="Orchestrator server address (format: ip:port)",
        )
        parser.add_argument(
            "--node-name",
            type=str,
            default=None,
            help="Node name for orchestrator registration (defaults to hostname)",
        )
        parser.add_argument(
            "--node-host",
            type=str,
            default=None,
            help="Node's external IP address for orchestrator registration (auto-detected if not specified)",
        )

        args = parser.parse_args()
        return cls(**vars(args))  # pyright: ignore[reportAny] - We are intentionally validating here, we can't do it statically
