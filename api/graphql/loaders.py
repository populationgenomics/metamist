# pylint: disable=no-value-for-parameter,redefined-builtin
# ^ Do this because of the loader decorator
import copy
import dataclasses
import enum
from collections import defaultdict
from typing import Any, TypedDict

from fastapi import Request
from strawberry.dataloader import DataLoader

from api.utils import group_by
from api.utils.db import get_projectless_db_connection
from db.python.connect import Connection
from db.python.filters import GenericFilter, get_hashable_value
from db.python.layers import (
    AnalysisLayer,
    AssayLayer,
    AuditLogLayer,
    FamilyLayer,
    ParticipantLayer,
    SampleLayer,
    SequencingGroupLayer,
)
from db.python.layers.comment import CommentLayer
from db.python.tables.analysis import AnalysisFilter
from db.python.tables.assay import AssayFilter
from db.python.tables.family import FamilyFilter
from db.python.tables.participant import ParticipantFilter
from db.python.tables.sample import SampleFilter
from db.python.tables.sequencing_group import SequencingGroupFilter
from db.python.utils import NotFoundError
from models.models import (
    AnalysisInternal,
    AssayInternal,
    FamilyInternal,
    ParticipantInternal,
    Project,
    ProjectId,
    SampleInternal,
    SequencingGroupInternal,
)
from models.models.audit_log import AuditLogInternal
from models.models.comment import CommentEntityType, DiscussionInternal
from models.models.family import PedRowInternal


class LoaderKeys(enum.Enum):
    """
    Keys for the data loaders, define them to it's clearer when we add / remove
    them, and reduces the chance of typos
    """

    PROJECTS_FOR_IDS = 'projects_for_id'

    AUDIT_LOGS_BY_IDS = 'audit_logs_by_ids'
    AUDIT_LOGS_BY_ANALYSIS_IDS = 'audit_logs_by_analysis_ids'

    ANALYSES_FOR_SEQUENCING_GROUPS = 'analyses_for_sequencing_groups'

    ASSAYS_FOR_IDS = 'assays_for_ids'
    ASSAYS_FOR_SAMPLES = 'sequences_for_samples'
    ASSAYS_FOR_SEQUENCING_GROUPS = 'assays_for_sequencing_groups'

    SAMPLES_FOR_IDS = 'samples_for_ids'
    SAMPLES_FOR_PARTICIPANTS = 'samples_for_participants'
    SAMPLES_FOR_PROJECTS = 'samples_for_projects'
    SAMPLES_FOR_PARENTS = 'samples_for_parents'

    PHENOTYPES_FOR_PARTICIPANTS = 'phenotypes_for_participants'

    PARTICIPANTS_FOR_IDS = 'participants_for_ids'
    PARTICIPANTS_FOR_FAMILIES = 'participants_for_families'
    PARTICIPANTS_FOR_PROJECTS = 'participants_for_projects'

    FAMILIES_FOR_PARTICIPANTS = 'families_for_participants'
    FAMILY_PARTICIPANTS_FOR_FAMILIES = 'family_participants_for_families'
    FAMILY_PARTICIPANTS_FOR_PARTICIPANTS = 'family_participants_for_participants'
    FAMILIES_FOR_IDS = 'families_for_ids'

    SEQUENCING_GROUPS_FOR_IDS = 'sequencing_groups_for_ids'
    SEQUENCING_GROUPS_FOR_SAMPLES = 'sequencing_groups_for_samples'
    SEQUENCING_GROUPS_FOR_PROJECTS = 'sequencing_groups_for_projects'
    SEQUENCING_GROUPS_FOR_ANALYSIS = 'sequencing_groups_for_analysis'

    COMMENTS_FOR_SAMPLE_IDS = 'comments_for_sample_ids'
    COMMENTS_FOR_PARTICIPANT_IDS = 'comments_for_participant_ids'
    COMMENTS_FOR_ASSAY_IDS = 'comments_for_assay_ids'
    COMMENTS_FOR_PROJECT_IDS = 'comments_for_project_ids'
    COMMENTS_FOR_SEQUENCING_GROUP_IDS = 'comments_for_sequencing_group_ids'
    COMMENTS_FOR_FAMILY_IDS = 'comments_for_family_ids'


loaders: dict[LoaderKeys, Any] = {}


def connected_data_loader(id_: LoaderKeys, cache: bool = True):
    """Provide connection to a data loader"""

    def connected_data_loader_caller(fn):
        def inner(connection: Connection):
            async def wrapped(*args, **kwargs):
                return await fn(*args, **kwargs, connection=connection)

            return DataLoader(wrapped, cache=cache)

        loaders[id_] = inner
        return inner

    return connected_data_loader_caller


def _get_connected_data_loader_partial_key(kwargs) -> tuple:
    return get_hashable_value({k: v for k, v in kwargs.items() if k != 'id'})  # type: ignore


def connected_data_loader_with_params(
    id_: LoaderKeys, default_factory=None, copy_args=True
):
    """
    DataLoader Decorator for allowing DB connection to be bound to a loader
    """

    def connected_data_loader_caller(fn):
        def inner(connection: Connection):
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


@connected_data_loader(LoaderKeys.AUDIT_LOGS_BY_IDS)
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


@connected_data_loader(LoaderKeys.AUDIT_LOGS_BY_ANALYSIS_IDS)
async def load_audit_logs_by_analysis_ids(
    analysis_ids: list[int], connection: Connection
) -> list[list[AuditLogInternal]]:
    """
    DataLoader: get_audit_logs_by_analysis_ids
    """
    alayer = AnalysisLayer(connection)
    logs = await alayer.get_audit_logs_by_analysis_ids(analysis_ids)
    return [logs.get(a) or [] for a in analysis_ids]


@connected_data_loader(LoaderKeys.ASSAYS_FOR_IDS)
async def load_assays_for_ids(
    assay_ids: list[int], connection: Connection
) -> list[AssayInternal]:
    """
    DataLoader: get_samples_for_ids
    """
    assaylayer = AssayLayer(connection)
    assays = await assaylayer.query(AssayFilter(id=GenericFilter(in_=assay_ids)))
    # in case it's not ordered
    assays_map = {a.id: a for a in assays}
    return [assays_map.get(a) for a in assay_ids]


@connected_data_loader_with_params(LoaderKeys.ASSAYS_FOR_SAMPLES, default_factory=list)
async def load_assays_by_samples(
    connection: Connection, ids, filter: AssayFilter
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
    sequencing_group_ids: list[int], connection: Connection
) -> list[list[AssayInternal]]:
    """
    Get all assays belong to the sequencing groups
    """
    assaylayer = AssayLayer(connection)

    # group by all last fields, in case we add more
    assays = await assaylayer.get_assays_for_sequencing_group_ids(
        sequencing_group_ids=sequencing_group_ids
    )

    return [assays.get(sg, []) for sg in sequencing_group_ids]


@connected_data_loader_with_params(
    LoaderKeys.SAMPLES_FOR_PARTICIPANTS, default_factory=list
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


@connected_data_loader(LoaderKeys.SEQUENCING_GROUPS_FOR_IDS)
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
    LoaderKeys.SEQUENCING_GROUPS_FOR_SAMPLES, default_factory=list
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


@connected_data_loader(LoaderKeys.SAMPLES_FOR_IDS)
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
    LoaderKeys.SAMPLES_FOR_PROJECTS, default_factory=list
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


@connected_data_loader_with_params(LoaderKeys.SAMPLES_FOR_PARENTS, default_factory=list)
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


@connected_data_loader(LoaderKeys.PARTICIPANTS_FOR_IDS)
async def load_participants_for_ids(
    participant_ids: list[int], connection: Connection
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
        raise NotFoundError(f'Could not find participants with ids {missing_pids}')
    return [p_by_id.get(p) for p in participant_ids]


@connected_data_loader(LoaderKeys.SEQUENCING_GROUPS_FOR_ANALYSIS)
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
    LoaderKeys.SEQUENCING_GROUPS_FOR_PROJECTS, default_factory=list
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


@connected_data_loader(LoaderKeys.PROJECTS_FOR_IDS)
async def load_projects_for_ids(
    project_ids: list[int], connection: Connection
) -> list[Project]:
    """
    Get projects by IDs
    """
    projects = [connection.project_id_map.get(p) for p in project_ids]

    return [p for p in projects if p is not None]


@connected_data_loader(LoaderKeys.FAMILIES_FOR_PARTICIPANTS)
async def load_families_for_participants(
    participant_ids: list[int], connection: Connection
) -> list[list[FamilyInternal]]:
    """
    Get families of participants, noting a participant can be in multiple families
    """
    flayer = FamilyLayer(connection)

    fam_map = await flayer.get_families_by_participants(participant_ids=participant_ids)
    return [fam_map.get(p, []) for p in participant_ids]


@connected_data_loader(LoaderKeys.PARTICIPANTS_FOR_FAMILIES)
async def load_participants_for_families(
    family_ids: list[int], connection: Connection
) -> list[list[ParticipantInternal]]:
    """Get all participants in a family, doesn't include affected statuses"""
    player = ParticipantLayer(connection)
    pmap = await player.get_participants_by_families(family_ids)
    return [pmap.get(fid, []) for fid in family_ids]


@connected_data_loader_with_params(
    LoaderKeys.PARTICIPANTS_FOR_PROJECTS, default_factory=list
)
async def load_participants_for_projects(
    ids: list[ProjectId], filter_: ParticipantFilter, connection: Connection
) -> dict[ProjectId, list[ParticipantInternal]]:
    """
    Get all participants in a project
    """

    f = copy.copy(filter_)
    f.project = GenericFilter(in_=ids)
    participants = await ParticipantLayer(connection).query(f)

    pmap = group_by(participants, lambda p: p.project)
    return pmap


@connected_data_loader_with_params(
    LoaderKeys.ANALYSES_FOR_SEQUENCING_GROUPS, default_factory=list
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


@connected_data_loader(LoaderKeys.PHENOTYPES_FOR_PARTICIPANTS)
async def load_phenotypes_for_participants(
    participant_ids: list[int], connection: Connection
) -> list[dict]:
    """
    Data loader for phenotypes for participants
    """
    player = ParticipantLayer(connection)
    participant_phenotypes = await player.get_phenotypes_for_participants(
        participant_ids=participant_ids
    )
    return [participant_phenotypes.get(pid, {}) for pid in participant_ids]


@connected_data_loader(LoaderKeys.FAMILIES_FOR_IDS)
async def load_families_for_ids(
    family_ids: list[int], connection: Connection
) -> list[FamilyInternal]:
    """
    DataLoader: get_families_for_ids
    """
    flayer = FamilyLayer(connection)
    families = await flayer.query(FamilyFilter(id=GenericFilter(in_=family_ids)))
    f_by_id = {f.id: f for f in families}
    return [f_by_id[f] for f in family_ids]


@connected_data_loader(LoaderKeys.FAMILY_PARTICIPANTS_FOR_FAMILIES)
async def load_family_participants_for_families(
    family_ids: list[int], connection: Connection
) -> list[list[PedRowInternal]]:
    """
    DataLoader: get_family_participants_for_families
    """
    flayer = FamilyLayer(connection)
    fp_map = await flayer.get_family_participants_by_family_ids(family_ids)

    return [fp_map.get(fid, []) for fid in family_ids]


@connected_data_loader(LoaderKeys.FAMILY_PARTICIPANTS_FOR_PARTICIPANTS)
async def load_family_participants_for_participants(
    participant_ids: list[int], connection: Connection
) -> list[list[PedRowInternal]]:
    """data loader for family participants for participants

    Args:
        participant_ids (list[int]): list of internal participant ids
        connection (_type_): (this is automatically filled in by the loader decorator)

    Returns:
        list[list[PedRowInternal]]: list of family participants for each participant
            (in order)
    """
    flayer = FamilyLayer(connection)
    family_participants = await flayer.get_family_participants_for_participants(
        participant_ids
    )
    fp_map = group_by(family_participants, lambda fp: fp.individual_id)

    return [fp_map.get(pid, []) for pid in participant_ids]


@connected_data_loader(LoaderKeys.COMMENTS_FOR_SAMPLE_IDS)
async def load_comments_for_sample_ids(
    sample_ids: list[int], connection: Connection
) -> list[DiscussionInternal | None]:
    """
    DataLoader: load_comments_for_sample_ids
    """
    clayer = CommentLayer(connection)
    comments = await clayer.get_discussion_for_entity_ids(
        entity=CommentEntityType.sample, entity_ids=sample_ids
    )
    return comments


@connected_data_loader(LoaderKeys.COMMENTS_FOR_PARTICIPANT_IDS)
async def load_comments_for_participant_ids(
    participant_ids: list[int], connection: Connection
) -> list[DiscussionInternal | None]:
    """
    DataLoader: load_comments_for_participant_ids
    """
    clayer = CommentLayer(connection)
    comments = await clayer.get_discussion_for_entity_ids(
        entity=CommentEntityType.participant, entity_ids=participant_ids
    )
    return comments


@connected_data_loader(LoaderKeys.COMMENTS_FOR_FAMILY_IDS)
async def load_comments_for_family_ids(
    family_ids: list[int], connection: Connection
) -> list[DiscussionInternal | None]:
    """
    DataLoader: load_comments_for_family_ids
    """
    clayer = CommentLayer(connection)
    comments = await clayer.get_discussion_for_entity_ids(
        entity=CommentEntityType.family, entity_ids=family_ids
    )
    return comments


@connected_data_loader(LoaderKeys.COMMENTS_FOR_ASSAY_IDS)
async def load_comments_for_assay_ids(
    assay_ids: list[int], connection: Connection
) -> list[DiscussionInternal | None]:
    """
    DataLoader: load_comments_for_assay_ids
    """
    clayer = CommentLayer(connection)
    comments = await clayer.get_discussion_for_entity_ids(
        entity=CommentEntityType.assay, entity_ids=assay_ids
    )
    return comments


@connected_data_loader(LoaderKeys.COMMENTS_FOR_PROJECT_IDS)
async def load_comments_for_project_ids(
    project_ids: list[int], connection: Connection
) -> list[DiscussionInternal | None]:
    """
    DataLoader: load_comments_for_project_ids
    """
    clayer = CommentLayer(connection)
    comments = await clayer.get_discussion_for_entity_ids(
        entity=CommentEntityType.project, entity_ids=project_ids
    )
    return comments


@connected_data_loader(LoaderKeys.COMMENTS_FOR_SEQUENCING_GROUP_IDS)
async def load_comments_for_sequencing_group_ids(
    sequencing_group_ids: list[int], connection: Connection
) -> list[DiscussionInternal | None]:
    """
    DataLoader: load_comments_for_sequencing_group_ids
    """
    clayer = CommentLayer(connection)
    comments = await clayer.get_discussion_for_entity_ids(
        entity=CommentEntityType.sequencing_group, entity_ids=sequencing_group_ids
    )
    return comments


class GraphQLContext(TypedDict):
    """Basic dict type for GraphQL context to be passed to resolvers"""

    loaders: dict[LoaderKeys, Any]
    connection: Connection


async def get_context(
    request: Request,  # pylint: disable=unused-argument
    connection: Connection = get_projectless_db_connection,
) -> GraphQLContext:
    """Get loaders / cache context for strawberyy GraphQL"""
    mapped_loaders = {k: fn(connection) for k, fn in loaders.items()}

    return {
        'connection': connection,
        'loaders': mapped_loaders,
    }
