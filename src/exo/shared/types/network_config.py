from pydantic import field_validator

from exo.utils.pydantic_ext import FrozenModel


class PeerAddress(FrozenModel):
    ip: str  # IPv4 or IPv6 address
    port: int  # TCP port (1-65535)

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError("Port must be between 1 and 65535")
        return v

    @field_validator("ip")
    @classmethod
    def validate_ip(cls, v: str) -> str:
        import ipaddress

        try:
            ipaddress.ip_address(v)
        except ValueError as e:
            raise ValueError(f"Invalid IP address: {v}") from e
        return v


class NetworkConfig(FrozenModel):
    peers: list[PeerAddress]

    @field_validator("peers")
    @classmethod
    def validate_peers(cls, v: list[PeerAddress]) -> list[PeerAddress]:
        if len(v) == 0:
            raise ValueError("At least one peer must be specified")
        return v
