import dataclasses
from typing import Any


@dataclasses.dataclass(frozen=True)
class EtlConfig:
    """ETL Config output type"""

    @dataclasses.dataclass(frozen=True)
    class EtlConfigType:
        """ETL config type"""

        parser_name: str
        default_parameters: dict[str, Any] | None
        users: list[str]

    by_type: dict[str, 'EtlConfigType']

    def to_dict(self) -> dict[str, dict[str, Any]]:
        """
        Convert the config to a dictionary
        """
        return {'by_type': {k: dataclasses.asdict(v) for k, v in self.by_type.items()}}
