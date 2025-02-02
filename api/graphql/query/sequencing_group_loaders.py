import dataclasses


from api.graphql.utils.loaders import (
    connected_data_loader,
    connected_data_loader_with_params,
)
from api.utils import group_by
from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers import (
    SequencingGroupLayer,
)
from db.python.tables.sequencing_group import SequencingGroupFilter
from models.models import (
    SequencingGroupInternal,
)


class SequencingGroupLoaderKeys:
    SEQUENCING_GROUPS_FOR_IDS = 'sequencing_groups_for_ids'
    SEQUENCING_GROUPS_FOR_SAMPLES = 'sequencing_groups_for_samples'
    SEQUENCING_GROUPS_FOR_PROJECTS = 'sequencing_groups_for_projects'
    SEQUENCING_GROUPS_FOR_ANALYSIS = 'sequencing_groups_for_analysis'


@connected_data_loader(SequencingGroupLoaderKeys.SEQUENCING_GROUPS_FOR_IDS)
async def load_sequencing_groups_for_ids(
    sequencing_group_ids: list[int], connection: Connection
) -> list[SequencingGroupInternal]:
    """
    DataLoader: get_sequencing_groups_by_ids
    """
    sequencing_groups = await SequencingGroupLayer(
        connection
    ).get_sequencing_groups_by_ids(sequencing_group_ids)
    # in case it's not ordered
    sequencing_groups_map = {sg.id: sg for sg in sequencing_groups}
    return [sequencing_groups_map.get(sg) for sg in sequencing_group_ids]


@connected_data_loader_with_params(
    SequencingGroupLoaderKeys.SEQUENCING_GROUPS_FOR_SAMPLES, default_factory=list
)
async def load_sequencing_groups_for_samples(
    connection: Connection, ids: list[int], filter: SequencingGroupFilter
) -> dict[int, list[SequencingGroupInternal]]:
    """
    Has format [(sample_id: int, sequencing_type?: string)]
    """
    sglayer = SequencingGroupLayer(connection)
    _filter = dataclasses.replace(filter) if filter else SequencingGroupFilter()
    if not _filter.sample:
        _filter.sample = SequencingGroupFilter.SequencingGroupSampleFilter(
            id=GenericFilter(in_=ids)
        )
    else:
        _filter.sample.id = GenericFilter(in_=ids)

    sequencing_groups = await sglayer.query(_filter)
    sg_map = group_by(sequencing_groups, lambda sg: sg.sample_id)
    return sg_map


@connected_data_loader(SequencingGroupLoaderKeys.SEQUENCING_GROUPS_FOR_ANALYSIS)
async def load_sequencing_groups_for_analysis_ids(
    analysis_ids: list[int], connection: Connection
) -> list[list[SequencingGroupInternal]]:
    """
    DataLoader: get_samples_for_analysis_ids
    """
    sglayer = SequencingGroupLayer(connection)
    analysis_sg_map = await sglayer.get_sequencing_groups_by_analysis_ids(analysis_ids)

    return [analysis_sg_map.get(aid, []) for aid in analysis_ids]


@connected_data_loader_with_params(
    SequencingGroupLoaderKeys.SEQUENCING_GROUPS_FOR_PROJECTS, default_factory=list
)
async def load_sequencing_groups_for_project_ids(
    ids: list[int], filter: SequencingGroupFilter, connection: Connection
) -> dict[int, list[SequencingGroupInternal]]:
    """
    DataLoader: get_sequencing_groups_for_project_ids
    """
    sglayer = SequencingGroupLayer(connection)
    filter.project = GenericFilter(in_=ids)
    sequencing_groups = await sglayer.query(filter_=filter)
    seq_group_map = group_by(sequencing_groups, lambda sg: sg.project)

    return seq_group_map
