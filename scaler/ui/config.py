import dataclasses


@dataclasses.dataclass
class WebUIConfig:
    address: str
    host: str = "0.0.0.0"
    port: int = 50001
