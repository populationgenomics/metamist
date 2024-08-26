import dataclasses
from typing import Any

from cpg_infra.config.deserializabledataclass import DeserializableDataclass


@dataclasses.dataclass(frozen=True)
class EtlConfig(DeserializableDataclass):
    """ETL Config output type"""

    @dataclasses.dataclass(frozen=True)
    class EtlConfigType(DeserializableDataclass):
        """ETL config type"""

        parser_name: str
        default_parameters: dict[str, Any] | None
        users: list[str]

    by_type: dict[str, 'EtlConfigType']

    def to_dict(self) -> dict[str, dict[str, Any]]:
        """
        Convert the config to a dictionary
        """
        return {k: dataclasses.asdict(v) for k, v in self.by_type.items()}

    @classmethod
    def from_dict(cls, data: dict[str, dict[str, Any]]) -> 'EtlConfig':
        """
        Create an EtlConfig from a dictionary
        """
        by_type = {k: cls.EtlConfigType(**v) for k, v in data.items()}
        return cls(by_type=by_type)
