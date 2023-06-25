# pylint: disable=no-value-for-parameter,redefined-builtin
# ^ Do this because of the loader decorator
import copy
import dataclasses
import enum
from collections import defaultdict
from typing import Any

from fastapi import Request
from strawberry.dataloader import DataLoader

from api.utils import get_projectless_db_connection, group_by
from db.python.connect import NotFoundError
from db.python.layers import (
    AnalysisLayer,
    SampleLayer,
    AssayLayer,
    ParticipantLayer,
    SequencingGroupLayer,
    FamilyLayer,
)
from db.python.tables.analysis import AnalysisFilter
from db.python.tables.assay import AssayFilter
from db.python.tables.project import ProjectPermissionsTable
from db.python.tables.sample import SampleFilter
from db.python.tables.sequencing_group import SequencingGroupFilter
from db.python.utils import ProjectId, GenericFilter
from models.models import (
    AssayInternal,
    SampleInternal,
    SequencingGroupInternal,
    AnalysisInternal,
    ParticipantInternal,
    FamilyInternal,
    Project,
)


class LoaderKeys(enum.Enum):
    """
    Keys for the data loaders, define them to it's clearer when we add / remove
    them, and reduces the chance of typos
    """

    PROJECTS_FOR_IDS = 'projects_for_id'

    ANALYSES_FOR_SEQUENCING_GROUPS = 'analyses_for_sequencing_groups'

    ASSAYS_FOR_SAMPLES = 'sequences_for_samples'
    ASSAYS_FOR_SEQUENCING_GROUPS = 'assays_for_sequencing_groups'

    SAMPLES_FOR_IDS = 'samples_for_ids'
    SAMPLES_FOR_PARTICIPANTS = 'samples_for_participants'
    SAMPLES_FOR_PROJECTS = 'samples_for_projects'

    PARTICIPANTS_FOR_IDS = 'participants_for_ids'
    PARTICIPANTS_FOR_FAMILIES = 'participants_for_families'
    PARTICIPANTS_FOR_PROJECTS = 'participants_for_projects'

    FAMILIES_FOR_PARTICIPANTS = 'families_for_participants'

    SEQUENCING_GROUPS_FOR_IDS = 'sequencing_groups_for_ids'
    SEQUENCING_GROUPS_FOR_SAMPLES = 'sequencing_groups_for_samples'
    SEQUENCING_GROUPS_FOR_PROJECTS = 'sequencing_groups_for_projects'
    SEQUENCING_GROUPS_FOR_ANALYSIS = 'sequencing_groups_for_analysis'


loaders = {}


def connected_data_loader(id_: LoaderKeys, cache=True):
    """Provide connection to a data loader"""

    def connected_data_loader_caller(fn):
        def inner(connection):
            async def wrapped(*args, **kwargs):
                return await fn(*args, **kwargs, connection=connection)

            return DataLoader(wrapped, cache=cache)

        loaders[id_] = inner
        return inner

    return connected_data_loader_caller


def _prepare_partial_value_for_hashing(value):
    if value is None:
        return None
    if isinstance(value, (int, str, float, bool)):
        return value
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, list):
        # let's see if later we need to prepare the values in the list
        return tuple(value)
    if isinstance(value, dict):
        return tuple(
            sorted(
                ((k, _prepare_partial_value_for_hashing(v)) for k, v in value.items()),
                key=lambda x: x[0],
            )
        )

    return hash(value)


def _get_connected_data_loader_partial_key(kwargs):
    return _prepare_partial_value_for_hashing(
        {k: v for k, v in kwargs.items() if k != 'id'}
    )


def connected_data_loader_with_params(
    id_: LoaderKeys, default_factory=None, copy_args=True
):
    """
    DataLoader Decorator for allowing DB connection to be bound to a loader
    """

    def connected_data_loader_caller(fn):
        def inner(connection):
            async def wrapped(query: list[dict[str, Any]]) -> list[Any]:
                by_key: dict[tuple, Any] = {}

                if any('connection' in q for q in query):
                    raise ValueError('Cannot pass connection in query')
                if any('id' not in q for q in query):
                    raise ValueError('Must pass id in query')

                # group by all last fields (except the first which is always ID
                grouped = group_by(query, _get_connected_data_loader_partial_key)
                for extra_args, chunk in grouped.items():
                    # ie: matrix transform
                    ids = [row['id'] for row in chunk]
                    kwargs = {
                        k: copy.copy(v) if copy_args else v
                        for k, v in chunk[0].items()
                        if k != 'id'
                    }
                    value_map = await fn(connection=connection, ids=ids, **kwargs)
                    if not isinstance(value_map, dict):
                        raise ValueError(
                            f'Expected dict from {fn.__name__}, got {type(value_map)}'
                        )
                    for returned_id, value in value_map.items():
                        by_key[(returned_id, *extra_args)] = value

                return [
                    by_key.get(
                        (q['id'], *_get_connected_data_loader_partial_key(q)),
                        default_factory() if default_factory else None,
                    )
                    for q in query
                ]

            return DataLoader(
                wrapped,
                # don't cache function calls
                cache=False,
            )

        loaders[id_] = inner
        return inner

    return connected_data_loader_caller


@connected_data_loader_with_params(LoaderKeys.ASSAYS_FOR_SAMPLES, default_factory=list)
async def load_assays_by_samples(
    connection, ids, filter: AssayFilter
) -> dict[int, list[AssayInternal]]:
    """
    DataLoader: get_assays_for_sample_ids
    """

    assaylayer = AssayLayer(connection)
    # maybe this is dangerous, but I don't think it should matter
    filter.sample_id = GenericFilter(in_=ids)
    assays = await assaylayer.query(filter)
    assay_map = group_by(assays, lambda a: a.sample_id)
    return assay_map


@connected_data_loader(LoaderKeys.ASSAYS_FOR_SEQUENCING_GROUPS)
async def load_assays_by_sequencing_groups(
    sequencing_group_ids: list[int], connection
) -> list[list[AssayInternal]]:
    """
    Get all assays belong to the sequencing groups
    """
    assaylayer = AssayLayer(connection)

    # group by all last fields, in case we add more
    assays = await assaylayer.get_assays_for_sequencing_group_ids(
        sequencing_group_ids=sequencing_group_ids, check_project_ids=False
    )

    return [assays.get(sg, []) for sg in sequencing_group_ids]


@connected_data_loader_with_params(
    LoaderKeys.SAMPLES_FOR_PARTICIPANTS, default_factory=list
)
async def load_samples_for_participant_ids(
    ids: list[int], filter: SampleFilter, connection
) -> dict[int, list[SampleInternal]]:
    """
    DataLoader: get_samples_for_participant_ids
    """
    filter.participant_id = GenericFilter(in_=ids)
    samples = await SampleLayer(connection).query(filter)
    samples_by_pid = group_by(samples, lambda s: s.participant_id)
    return samples_by_pid


@connected_data_loader(LoaderKeys.SEQUENCING_GROUPS_FOR_IDS)
async def load_sequencing_groups_for_ids(
    sequencing_group_ids: list[int], connection
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
    LoaderKeys.SEQUENCING_GROUPS_FOR_SAMPLES, default_factory=list
)
async def load_sequencing_groups_for_samples(
    connection, ids: list[int], filter: SequencingGroupFilter
) -> dict[int, list[SequencingGroupInternal]]:
    """
    Has format [(sample_id: int, sequencing_type?: string)]
    """
    sglayer = SequencingGroupLayer(connection)
    _filter = dataclasses.replace(filter) if filter else SequencingGroupFilter()
    _filter.sample_id = GenericFilter(in_=ids)

    sequencing_groups = await sglayer.query(_filter)
    sg_map = group_by(sequencing_groups, lambda sg: sg.sample_id)
    return sg_map


@connected_data_loader(LoaderKeys.SAMPLES_FOR_IDS)
async def load_samples_for_ids(
    sample_ids: list[int], connection
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
    LoaderKeys.SAMPLES_FOR_PROJECTS, default_factory=list
)
async def load_samples_for_projects(
    connection, ids: list[ProjectId], filter: SampleFilter
):
    """
    DataLoader: get_samples_for_project_ids
    """
    # maybe handle the external_ids here
    filter.project = GenericFilter(in_=ids)
    samples = await SampleLayer(connection).query(filter)
    samples_by_project = group_by(samples, lambda s: s.project)
    return samples_by_project


@connected_data_loader(LoaderKeys.PARTICIPANTS_FOR_IDS)
async def load_participants_for_ids(
    participant_ids: list[int], connection
) -> list[ParticipantInternal]:
    """
    DataLoader: get_participants_by_ids
    """
    player = ParticipantLayer(connection)
    persons = await player.get_participants_by_ids(
        [p for p in participant_ids if p is not None]
    )
    p_by_id = {p.id: p for p in persons}
    missing_pids = set(participant_ids) - set(p_by_id.keys())
    if missing_pids:
        raise NotFoundError(
            f'Could not find participants with ids {missing_pids}'
        )
    return [p_by_id.get(p) for p in participant_ids]


@connected_data_loader(LoaderKeys.SEQUENCING_GROUPS_FOR_ANALYSIS)
async def load_sequencing_groups_for_analysis_ids(
    analysis_ids: list[int], connection
) -> list[list[SequencingGroupInternal]]:
    """
    DataLoader: get_samples_for_analysis_ids
    """
    sglayer = SequencingGroupLayer(connection)
    analysis_sg_map = await sglayer.get_sequencing_groups_by_analysis_ids(analysis_ids)

    return [analysis_sg_map.get(aid, []) for aid in analysis_ids]


@connected_data_loader_with_params(
    LoaderKeys.SEQUENCING_GROUPS_FOR_PROJECTS, default_factory=list
)
async def load_sequencing_groups_for_project_ids(
    ids: list[int], filter: SequencingGroupFilter, connection
) -> dict[int, list[SequencingGroupInternal]]:
    """
    DataLoader: get_sequencing_groups_for_project_ids
    """
    sglayer = SequencingGroupLayer(connection)
    filter.project = GenericFilter(in_=ids)
    sequencing_groups = await sglayer.query(filter_=filter)
    seq_group_map = group_by(sequencing_groups, lambda sg: sg.project)

    return seq_group_map


@connected_data_loader(LoaderKeys.PROJECTS_FOR_IDS)
async def load_projects_for_ids(project_ids: list[int], connection) -> list[Project]:
    """
    Get projects by IDs
    """
    pttable = ProjectPermissionsTable(connection.connection)
    projects = await pttable.get_projects_by_ids(project_ids)
    p_by_id = {p.id: p for p in projects}
    return [p_by_id.get(p) for p in project_ids]


@connected_data_loader(LoaderKeys.FAMILIES_FOR_PARTICIPANTS)
async def load_families_for_participants(
    participant_ids: list[int], connection
) -> list[list[FamilyInternal]]:
    """
    Get families of participants, noting a participant can be in multiple families
    """
    flayer = FamilyLayer(connection)
    fam_map = await flayer.get_families_by_participants(participant_ids=participant_ids)
    return [fam_map.get(p, []) for p in participant_ids]


@connected_data_loader(LoaderKeys.PARTICIPANTS_FOR_FAMILIES)
async def load_participants_for_families(
    family_ids: list[int], connection
) -> list[list[ParticipantInternal]]:
    """Get all participants in a family, doesn't include affected statuses"""
    player = ParticipantLayer(connection)
    pmap = await player.get_participants_by_families(family_ids)
    return [pmap.get(fid, []) for fid in family_ids]


@connected_data_loader(LoaderKeys.PARTICIPANTS_FOR_PROJECTS)
async def load_participants_for_projects(
    project_ids: list[ProjectId], connection
) -> list[list[ParticipantInternal]]:
    """
    Get all participants in a project
    """

    retval: list[list[ParticipantInternal]] = []

    for project in project_ids:
        retval.append(
            await ParticipantLayer(connection).get_participants(project=project)
        )

    return retval


@connected_data_loader_with_params(
    LoaderKeys.ANALYSES_FOR_SEQUENCING_GROUPS, default_factory=list
)
async def load_analyses_for_sequencing_groups(
    ids: list[int],
    filter_: AnalysisFilter,
    connection,
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


async def get_context(request: Request, connection=get_projectless_db_connection):  # pylint: disable=unused-argument
    """Get loaders / cache context for strawberyy GraphQL"""
    mapped_loaders = {k: fn(connection) for k, fn in loaders.items()}
    return {
        'connection': connection,
        **mapped_loaders,
    }
