# pylint: disable=no-value-for-parameter,redefined-builtin
# ^ Do this because of the loader decorator
from collections import defaultdict


from api.graphql.utils.loaders import connected_data_loader_with_params
from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers import (
    AnalysisLayer,
)
from db.python.tables.analysis import AnalysisFilter
from models.models import (
    AnalysisInternal,
)


class AnalysisLoaderKeys:
    """
    Keys for the data loaders, define them to it's clearer when we add / remove
    them, and reduces the chance of typos
    """

    ANALYSES_FOR_SEQUENCING_GROUPS = 'analyses_for_sequencing_groups'


@connected_data_loader_with_params(
    AnalysisLoaderKeys.ANALYSES_FOR_SEQUENCING_GROUPS, default_factory=list
)
async def load_analyses_for_sequencing_groups(
    ids: list[int],
    filter_: AnalysisFilter,
    connection: Connection,
) -> dict[int, list[AnalysisInternal]]:
    """
    Type: (sequencing_group_id: int, status?: AnalysisStatus, type?: str)
        -> list[list[AnalysisInternal]]
    """
    alayer = AnalysisLayer(connection)
    filter_.sequencing_group_id = GenericFilter(in_=ids)
    analyses = await alayer.query(filter_)
    by_sg_id: dict[int, list[AnalysisInternal]] = defaultdict(list)
    for a in analyses:
        for sg in a.sequencing_group_ids:
            by_sg_id[sg].append(a)
    return by_sg_id
