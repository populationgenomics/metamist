from models.base import SMBase


class ProjectSettings(SMBase):
    """Settings for a project, eg: sample_prefix"""

    def __init__(self, sample_prefix: str) -> None:
        super().__init__()

        self.sample_prefix = sample_prefix
