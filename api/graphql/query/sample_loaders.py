# pylint: disable=no-value-for-parameter,redefined-builtin
# ^ Do this because of the loader decorator
import copy


from api.graphql.utils.loaders import (
    connected_data_loader,
    connected_data_loader_with_params,
)
from api.utils import group_by
from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers import (
    SampleLayer,
)
from db.python.tables.sample import SampleFilter
from models.models import (
    ProjectId,
    SampleInternal,
)


class SampleLoaderKeys:
    SAMPLES_FOR_IDS = 'samples_for_ids'
    SAMPLES_FOR_PARTICIPANTS = 'samples_for_participants'
    SAMPLES_FOR_PROJECTS = 'samples_for_projects'
    SAMPLES_FOR_PARENTS = 'samples_for_parents'


@connected_data_loader_with_params(
    SampleLoaderKeys.SAMPLES_FOR_PARTICIPANTS, default_factory=list
)
async def load_samples_for_participant_ids(
    ids: list[int], filter: SampleFilter, connection: Connection
) -> dict[int, list[SampleInternal]]:
    """
    DataLoader: get_samples_for_participant_ids
    """
    filter.participant_id = GenericFilter(in_=ids)
    samples = await SampleLayer(connection).query(filter)
    samples_by_pid = group_by(samples, lambda s: s.participant_id)
    return samples_by_pid


@connected_data_loader(SampleLoaderKeys.SAMPLES_FOR_IDS)
async def load_samples_for_ids(
    sample_ids: list[int], connection: Connection
) -> list[SampleInternal]:
    """
    DataLoader: get_samples_for_ids
    """
    slayer = SampleLayer(connection)
    samples = await slayer.query(SampleFilter(id=GenericFilter(in_=sample_ids)))
    # in case it's not ordered
    samples_map = {s.id: s for s in samples}
    return [samples_map.get(s) for s in sample_ids]


@connected_data_loader_with_params(
    SampleLoaderKeys.SAMPLES_FOR_PROJECTS, default_factory=list
)
async def load_samples_for_projects(
    connection: Connection, ids: list[ProjectId], filter: SampleFilter
):
    """
    DataLoader: get_samples_for_project_ids
    """
    # maybe handle the external_ids here
    filter.project = GenericFilter(in_=ids)
    samples = await SampleLayer(connection).query(filter)
    samples_by_project = group_by(samples, lambda s: s.project)
    return samples_by_project


@connected_data_loader_with_params(
    SampleLoaderKeys.SAMPLES_FOR_PARENTS, default_factory=list
)
async def load_nested_samples_for_parents(
    connection: Connection, ids: list[int], filter_: SampleFilter
):
    """
    DataLoader: get_nested_samples_for_parents
    """
    filter_ = copy.copy(filter_)

    filter_.sample_parent_id = GenericFilter(in_=ids)
    samples = await SampleLayer(connection).query(filter_)
    samples_by_parent = group_by(samples, lambda s: s.sample_parent_id)
    return samples_by_parent
