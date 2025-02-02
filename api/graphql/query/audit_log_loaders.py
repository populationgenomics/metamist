from api.graphql.utils.loaders import connected_data_loader
from db.python.connect import Connection
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.audit_log import AuditLogLayer
from models.models.audit_log import AuditLogInternal


class AuditLogLoaderKeys:
    """
    Keys for the data loaders, define them to it's clearer when we add / remove
    them, and reduces the chance of typos
    """

    AUDIT_LOGS_BY_IDS = 'audit_logs_by_ids'
    AUDIT_LOGS_BY_ANALYSIS_IDS = 'audit_logs_by_analysis_ids'


@connected_data_loader(AuditLogLoaderKeys.AUDIT_LOGS_BY_IDS)
async def load_audit_logs_by_ids(
    audit_log_ids: list[int], connection: Connection
) -> list[AuditLogInternal | None]:
    """
    DataLoader: get_audit_logs_by_ids
    """
    alayer = AuditLogLayer(connection)
    logs = await alayer.get_for_ids(audit_log_ids)
    logs_by_id = {log.id: log for log in logs}
    return [logs_by_id.get(a) for a in audit_log_ids]


@connected_data_loader(AuditLogLoaderKeys.AUDIT_LOGS_BY_ANALYSIS_IDS)
async def load_audit_logs_by_analysis_ids(
    analysis_ids: list[int], connection: Connection
) -> list[list[AuditLogInternal]]:
    """
    DataLoader: get_audit_logs_by_analysis_ids
    """
    alayer = AnalysisLayer(connection)
    logs = await alayer.get_audit_logs_by_analysis_ids(analysis_ids)
    return [logs.get(a) or [] for a in analysis_ids]
