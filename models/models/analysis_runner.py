import datetime

from models.base import SMBase
from models.models.project import ProjectId


class AnalysisRunnerInternal(SMBase):
    """
    Internal class for AnalysisRunner records
    """

    ar_guid: str
    project: ProjectId
    output_path: str
    timestamp: datetime.datetime
    access_level: str
    repository: str
    commit: str
    script: str
    description: str
    driver_image: str
    config_path: str | None
    cwd: str | None
    environment: str
    hail_version: str | None
    batch_url: str
    submitting_user: str
    meta: dict[str, str]

    # on insert
    audit_log_id: int | None = None

    def to_external(self, project_map: dict[ProjectId, str]):
        """Convert to transport model"""
        return AnalysisRunner(
            ar_guid=self.ar_guid,
            project=project_map.get(self.project, str(self.project)),
            timestamp=self.timestamp.isoformat(),
            output_path=self.output_path,
            access_level=self.access_level,
            repository=self.repository,
            commit=self.commit,
            script=self.script,
            description=self.description,
            driver_image=self.driver_image,
            config_path=self.config_path,
            cwd=self.cwd,
            environment=self.environment,
            hail_version=self.hail_version,
            batch_url=self.batch_url,
            submitting_user=self.submitting_user,
            meta=self.meta,
            audit_log_id=self.audit_log_id,
        )


class AnalysisRunner(SMBase):
    """
    Transport / external class for AnalysisRunner records
    """

    ar_guid: str
    output_path: str
    timestamp: str
    access_level: str
    repository: str
    commit: str
    script: str
    description: str
    driver_image: str
    config_path: str | None
    cwd: str | None
    environment: str
    hail_version: str | None
    batch_url: str
    submitting_user: str
    meta: dict[str, str]
    project: str
    audit_log_id: int | None
