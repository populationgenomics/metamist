# ^ Do this because of the loader decorator
import copy
import dataclasses
from collections import defaultdict
from collections.abc import Awaitable, Callable
from datetime import date
from typing import Any, TypeVar

from fastapi import Request
from strawberry.dataloader import DataLoader
from strawberry.fastapi import BaseContext

from api.utils import ensure_nonnone, group_by
from api.utils.db import GetConnection, get_projectless_db_connection_getter
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
from db.python.tables.sequencing_group import (
    SequencingGroupFilter,
    SequencingGroupTable,
)
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


K = TypeVar('K')
V = TypeVar('V')


def connected_data_loader(
    cache: bool = True,
) -> Callable[
    [Callable[[list[K], GetConnection], Awaitable[list[V]]]],
    Callable[[GetConnection], DataLoader[K, V]],
]:
    """Provide connection to a data loader"""

    def connected_data_loader_caller(
        fn: Callable[[list[K], GetConnection], Awaitable[list[V]]],
    ) -> Callable[[GetConnection], DataLoader[K, V]]:
        def inner(get_connection: GetConnection) -> DataLoader[K, V]:
            async def wrapped(keys: list[K]) -> list[V]:
                return await fn(keys, get_connection)

            return DataLoader(wrapped, cache=cache)

        return inner

    return connected_data_loader_caller


def _get_connected_data_loader_partial_key(kwargs) -> tuple:
    return get_hashable_value({k: v for k, v in kwargs.items() if k != 'id'})  # type: ignore


def connected_data_loader_with_params(
    default_factory: Callable[[], Any] | None = None, copy_args: bool = True
) -> Callable[
    [Callable[..., Awaitable[dict[Any, V]]]],
    Callable[[GetConnection], DataLoader[Any, V | None]],
]:
    """
    DataLoader Decorator for allowing DB connection to be bound to a loader
    """

    def connected_data_loader_caller(
        fn: Callable[..., Awaitable[dict[Any, V]]],
    ) -> Callable[[GetConnection], DataLoader[Any, V | None]]:
        def inner(get_connection: GetConnection) -> DataLoader[Any, V | None]:
            async def wrapped(query: list[Any]) -> list[V | None]:
                by_key: dict[tuple, V | None] = {}

                if any('connection' in q or 'get_connection' in q for q in query):
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
                    value_map = await fn(
                        get_connection=get_connection, ids=ids, **kwargs
                    )
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

        return inner

    return connected_data_loader_caller


@connected_data_loader()
async def load_audit_logs_by_ids(
    audit_log_ids: list[int], get_connection: GetConnection
) -> list[AuditLogInternal | None]:
    """
    DataLoader: get_audit_logs_by_ids
    """
    async with get_connection() as connection:
        alayer = AuditLogLayer(connection)
        logs = await alayer.get_for_ids(audit_log_ids)
        logs_by_id = {log.id: log for log in logs}
        return [logs_by_id.get(a) for a in audit_log_ids]


@connected_data_loader()
async def load_audit_logs_by_analysis_ids(
    analysis_ids: list[int], get_connection: GetConnection
) -> list[list[AuditLogInternal]]:
    """
    DataLoader: get_audit_logs_by_analysis_ids
    """
    async with get_connection() as connection:
        alayer = AnalysisLayer(connection)
        logs = await alayer.get_audit_logs_by_analysis_ids(analysis_ids)
        return [logs.get(a) or [] for a in analysis_ids]


@connected_data_loader()
async def load_assays_for_ids(
    assay_ids: list[int], get_connection: GetConnection
) -> list[AssayInternal]:
    """
    DataLoader: get_samples_for_ids
    """
    async with get_connection() as connection:
        assaylayer = AssayLayer(connection)
        assays = await assaylayer.query(AssayFilter(id=GenericFilter(in_=assay_ids)))
        # in case it's not ordered
        assays_map = {a.id: a for a in assays}
        return [assays_map[a] for a in assay_ids]


@connected_data_loader_with_params(default_factory=list)
async def load_assays_by_samples(
    get_connection: GetConnection, ids, filter: AssayFilter
) -> dict[int, list[AssayInternal]]:
    """
    DataLoader: get_assays_for_sample_ids
    """
    async with get_connection() as connection:
        assaylayer = AssayLayer(connection)
        # maybe this is dangerous, but I don't think it should matter
        filter.sample_id = GenericFilter(in_=ids)
        assays = await assaylayer.query(filter)
        assay_map = group_by(assays, lambda a: a.sample_id)
    return assay_map


@connected_data_loader()
async def load_assays_by_sequencing_groups(
    sequencing_group_ids: list[int], get_connection: GetConnection
) -> list[list[AssayInternal]]:
    """
    Get all assays belong to the sequencing groups
    """
    async with get_connection() as connection:
        assaylayer = AssayLayer(connection)

        # group by all last fields, in case we add more
        assays = await assaylayer.get_assays_for_sequencing_group_ids(
            sequencing_group_ids=sequencing_group_ids
        )

        return [assays.get(sg, []) for sg in sequencing_group_ids]


@connected_data_loader_with_params(default_factory=list)
async def load_samples_for_participant_ids(
    ids: list[int], filter: SampleFilter, get_connection: GetConnection
) -> dict[int, list[SampleInternal]]:
    """
    DataLoader: get_samples_for_participant_ids
    """
    filter.participant_id = GenericFilter(in_=ids)
    async with get_connection() as connection:
        samples = await SampleLayer(connection).query(filter)
        samples_by_pid = group_by(samples, lambda s: ensure_nonnone(s.participant_id))
    return samples_by_pid


@connected_data_loader()
async def load_sequencing_groups_for_ids(
    sequencing_group_ids: list[int], get_connection: GetConnection
) -> list[SequencingGroupInternal]:
    """
    DataLoader: get_sequencing_groups_by_ids
    """
    async with get_connection() as connection:
        sequencing_groups = await SequencingGroupLayer(
            connection
        ).get_sequencing_groups_by_ids(sequencing_group_ids)
        # in case it's not ordered
        sequencing_groups_map = {sg.id: sg for sg in sequencing_groups}
        return [sequencing_groups_map[sg] for sg in sequencing_group_ids]


@connected_data_loader_with_params(default_factory=list)
async def load_sequencing_groups_for_samples(
    get_connection: GetConnection, ids: list[int], filter: SequencingGroupFilter
) -> dict[int, list[SequencingGroupInternal]]:
    """
    Has format [(sample_id: int, sequencing_type?: string)]
    """
    async with get_connection() as connection:
        sglayer = SequencingGroupLayer(connection)
        _filter = dataclasses.replace(filter) if filter else SequencingGroupFilter()
        if not _filter.sample:
            _filter.sample = SequencingGroupFilter.SequencingGroupSampleFilter(
                id=GenericFilter(in_=ids)
            )
        else:
            _filter.sample.id = GenericFilter(in_=ids)

        sequencing_groups = await sglayer.query(_filter)
        sg_map = group_by(sequencing_groups, lambda sg: ensure_nonnone(sg.sample_id))
        return sg_map


@connected_data_loader()
async def load_sequencing_group_counts_by_month(
    ids: list[ProjectId], get_connection: GetConnection
) -> list[dict[date, dict[str, int]]]:
    """
    DataLoader: get_sequencing_group_counts_by_month
    """
    async with get_connection() as connection:
        sgt = SequencingGroupTable(connection)
        counts_by_month = await sgt.get_sequencing_group_counts_by_month(ids)

        return [counts_by_month[id] for id in ids]  # noqa: A001


@connected_data_loader()
async def load_samples_for_ids(
    sample_ids: list[int], get_connection: GetConnection
) -> list[SampleInternal]:
    """
    DataLoader: get_samples_for_ids
    """
    async with get_connection() as connection:
        slayer = SampleLayer(connection)
        samples = await slayer.query(SampleFilter(id=GenericFilter(in_=sample_ids)))
        # in case it's not ordered
        samples_map = {s.id: s for s in samples}
        return [samples_map[s] for s in sample_ids]


@connected_data_loader_with_params(default_factory=list)
async def load_samples_for_projects(
    get_connection: GetConnection, ids: list[ProjectId], filter: SampleFilter
):
    """
    DataLoader: get_samples_for_project_ids
    """
    async with get_connection() as connection:
        # maybe handle the external_ids here
        filter.project = GenericFilter(in_=ids)
        samples = await SampleLayer(connection).query(filter)
        samples_by_project = group_by(samples, lambda s: s.project)
        return samples_by_project


@connected_data_loader_with_params(default_factory=list)
async def load_nested_samples_for_parents(
    get_connection: GetConnection, ids: list[int], filter_: SampleFilter
):
    """
    DataLoader: get_nested_samples_for_parents
    """
    async with get_connection() as connection:
        filter_ = copy.copy(filter_)

        filter_.sample_parent_id = GenericFilter(in_=ids)
        samples = await SampleLayer(connection).query(filter_)
        samples_by_parent = group_by(samples, lambda s: s.sample_parent_id)
        return samples_by_parent


@connected_data_loader()
async def load_participants_for_ids(
    participant_ids: list[int], get_connection: GetConnection
) -> list[ParticipantInternal]:
    """
    DataLoader: get_participants_by_ids
    """
    async with get_connection() as connection:
        player = ParticipantLayer(connection)
        persons = await player.get_participants_by_ids(
            [p for p in participant_ids if p is not None]
        )
        p_by_id = {p.id: p for p in persons}
        missing_pids = set(participant_ids) - set(p_by_id.keys())
        if missing_pids:
            raise NotFoundError(f'Could not find participants with ids {missing_pids}')
        return [p_by_id[p] for p in participant_ids]


@connected_data_loader()
async def load_sequencing_groups_for_analysis_ids(
    analysis_ids: list[int], get_connection: GetConnection
) -> list[list[SequencingGroupInternal]]:
    """
    DataLoader: get_samples_for_analysis_ids
    """
    async with get_connection() as connection:
        sglayer = SequencingGroupLayer(connection)
        analysis_sg_map = await sglayer.get_sequencing_groups_by_analysis_ids(
            analysis_ids
        )

        return [analysis_sg_map.get(aid, []) for aid in analysis_ids]


@connected_data_loader_with_params(default_factory=list)
async def load_sequencing_groups_for_project_ids(
    get_connection: GetConnection, ids: list[int], filter: SequencingGroupFilter
) -> dict[int, list[SequencingGroupInternal]]:
    """
    DataLoader: get_sequencing_groups_for_project_ids
    """
    async with get_connection() as connection:
        sglayer = SequencingGroupLayer(connection)
        filter.project = GenericFilter(in_=ids)
        sequencing_groups = await sglayer.query(filter_=filter)
        sg_map = group_by(sequencing_groups, lambda sg: ensure_nonnone(sg.project))
        return sg_map


@connected_data_loader()
async def load_projects_for_ids(
    project_ids: list[int], get_connection: GetConnection
) -> list[Project]:
    """
    Get projects by IDs
    """
    async with get_connection() as connection:
        projects = [connection.project_id_map.get(p) for p in project_ids]

        return [p for p in projects if p is not None]


@connected_data_loader()
async def load_families_for_participants(
    participant_ids: list[int], get_connection: GetConnection
) -> list[list[FamilyInternal]]:
    """
    Get families of participants, noting a participant can be in multiple families
    """
    async with get_connection() as connection:
        flayer = FamilyLayer(connection)

        fam_map = await flayer.get_families_by_participants(
            participant_ids=participant_ids
        )
        return [fam_map.get(p, []) for p in participant_ids]


@connected_data_loader()
async def load_participants_for_families(
    family_ids: list[int], get_connection: GetConnection
) -> list[list[ParticipantInternal]]:
    """Get all participants in a family, doesn't include affected statuses"""
    async with get_connection() as connection:
        player = ParticipantLayer(connection)
        pmap = await player.get_participants_by_families(family_ids)
        return [pmap.get(fid, []) for fid in family_ids]


@connected_data_loader_with_params(default_factory=list)
async def load_participants_for_projects(
    get_connection: GetConnection, ids: list[ProjectId], filter_: ParticipantFilter
) -> dict[ProjectId, list[ParticipantInternal]]:
    """
    Get all participants in a project
    """
    async with get_connection() as connection:
        f = copy.copy(filter_)
        f.project = GenericFilter(in_=ids)
        participants = await ParticipantLayer(connection).query(f)

        pmap = group_by(participants, lambda p: p.project)
        return pmap


@connected_data_loader_with_params(default_factory=list)
async def load_analyses_for_projects(
    get_connection: GetConnection,
    ids: list[int],
    filter_: AnalysisFilter,
) -> dict[int, list[AnalysisInternal]]:
    """
    Data loader for loading analyses from projects.
    """
    async with get_connection() as connection:
        alayer = AnalysisLayer(connection)
        filter_.project = GenericFilter(in_=ids)
        analyses = await alayer.query(filter_)
        by_project_id: dict[int, list[AnalysisInternal]] = defaultdict(list)
        for a in analyses:
            assert a.project
            by_project_id[a.project].append(a)

        return by_project_id


@connected_data_loader_with_params(default_factory=list)
async def load_analyses_for_sequencing_groups(
    get_connection: GetConnection,
    ids: list[int],
    filter_: AnalysisFilter,
) -> dict[int, list[AnalysisInternal]]:
    """
    Type: (sequencing_group_id: int, status?: AnalysisStatus, type?: str)
        -> list[list[AnalysisInternal]]
    """
    async with get_connection() as connection:
        alayer = AnalysisLayer(connection)
        filter_.sequencing_group_id = GenericFilter(in_=ids)
        analyses = await alayer.query(filter_)
        by_sg_id: dict[int, list[AnalysisInternal]] = defaultdict(list)
        for a in analyses:
            assert a.sequencing_group_ids
            for sg in a.sequencing_group_ids:
                by_sg_id[sg].append(a)
        return by_sg_id


@connected_data_loader()
async def load_phenotypes_for_participants(
    participant_ids: list[int], get_connection: GetConnection
) -> list[dict]:
    """
    Data loader for phenotypes for participants
    """
    async with get_connection() as connection:
        player = ParticipantLayer(connection)
        participant_phenotypes = await player.get_phenotypes_for_participants(
            participant_ids=participant_ids
        )
        return [participant_phenotypes.get(pid, {}) for pid in participant_ids]


@connected_data_loader()
async def load_families_for_ids(
    family_ids: list[int], get_connection: GetConnection
) -> list[FamilyInternal]:
    """
    DataLoader: get_families_for_ids
    """
    async with get_connection() as connection:
        flayer = FamilyLayer(connection)
        families = await flayer.query(FamilyFilter(id=GenericFilter(in_=family_ids)))
        f_by_id = {f.id: f for f in families}
        return [f_by_id[f] for f in family_ids]


@connected_data_loader()
async def load_family_participants_for_families(
    family_ids: list[int], get_connection: GetConnection
) -> list[list[PedRowInternal]]:
    """
    DataLoader: get_family_participants_for_families
    """
    async with get_connection() as connection:
        flayer = FamilyLayer(connection)
        fp_map = await flayer.get_family_participants_by_family_ids(family_ids)

        return [fp_map.get(fid, []) for fid in family_ids]


@connected_data_loader()
async def load_family_participants_for_participants(
    participant_ids: list[int], get_connection: GetConnection
) -> list[list[PedRowInternal]]:
    """
    data loader for family participants for participants

    Args:
        participant_ids (list[int]): list of internal participant ids
        get_connection (GetConnection): (this is automatically filled in by the loader decorator)

    Returns:
        list[list[PedRowInternal]]: list of family participants for each participant
            (in order)
    """
    async with get_connection() as connection:
        flayer = FamilyLayer(connection)
        family_participants = await flayer.get_family_participants_for_participants(
            participant_ids
        )
        fp_map = group_by(family_participants, lambda fp: fp.individual_id)

        return [fp_map.get(pid, []) for pid in participant_ids]


@connected_data_loader()
async def load_comments_for_sample_ids(
    sample_ids: list[int], get_connection: GetConnection
) -> list[DiscussionInternal | None]:
    """
    DataLoader: load_comments_for_sample_ids
    """
    async with get_connection() as connection:
        clayer = CommentLayer(connection)
        comments = await clayer.get_discussion_for_entity_ids(
            entity=CommentEntityType.sample, entity_ids=sample_ids
        )
        return comments


@connected_data_loader()
async def load_comments_for_participant_ids(
    participant_ids: list[int], get_connection: GetConnection
) -> list[DiscussionInternal | None]:
    """
    DataLoader: load_comments_for_participant_ids
    """
    async with get_connection() as connection:
        clayer = CommentLayer(connection)
        comments = await clayer.get_discussion_for_entity_ids(
            entity=CommentEntityType.participant, entity_ids=participant_ids
        )
        return comments


@connected_data_loader()
async def load_comments_for_family_ids(
    family_ids: list[int], get_connection: GetConnection
) -> list[DiscussionInternal | None]:
    """
    DataLoader: load_comments_for_family_ids
    """
    async with get_connection() as connection:
        clayer = CommentLayer(connection)
        comments = await clayer.get_discussion_for_entity_ids(
            entity=CommentEntityType.family, entity_ids=family_ids
        )
        return comments


@connected_data_loader()
async def load_comments_for_assay_ids(
    assay_ids: list[int], get_connection: GetConnection
) -> list[DiscussionInternal | None]:
    """
    DataLoader: load_comments_for_assay_ids
    """
    async with get_connection() as connection:
        clayer = CommentLayer(connection)
        comments = await clayer.get_discussion_for_entity_ids(
            entity=CommentEntityType.assay, entity_ids=assay_ids
        )
        return comments


@connected_data_loader()
async def load_comments_for_project_ids(
    project_ids: list[int], get_connection: GetConnection
) -> list[DiscussionInternal | None]:
    """
    DataLoader: load_comments_for_project_ids
    """
    async with get_connection() as connection:
        clayer = CommentLayer(connection)
        comments = await clayer.get_discussion_for_entity_ids(
            entity=CommentEntityType.project, entity_ids=project_ids
        )
        return comments


@connected_data_loader()
async def load_comments_for_sequencing_group_ids(
    sequencing_group_ids: list[int], get_connection: GetConnection
) -> list[DiscussionInternal | None]:
    """
    DataLoader: load_comments_for_sequencing_group_ids
    """
    async with get_connection() as connection:
        clayer = CommentLayer(connection)
        comments = await clayer.get_discussion_for_entity_ids(
            entity=CommentEntityType.sequencing_group, entity_ids=sequencing_group_ids
        )
        return comments


class LoaderContext:
    """A context object for all data loaders."""

    def __init__(self, connection_getter: GetConnection):
        self.load_audit_logs_by_ids = load_audit_logs_by_ids(connection_getter)
        self.load_audit_logs_by_analysis_ids = load_audit_logs_by_analysis_ids(
            connection_getter
        )
        self.load_assays_for_ids = load_assays_for_ids(connection_getter)
        self.load_assays_by_samples = load_assays_by_samples(connection_getter)
        self.load_assays_by_sequencing_groups = load_assays_by_sequencing_groups(
            connection_getter
        )
        self.load_samples_for_participant_ids = load_samples_for_participant_ids(
            connection_getter
        )
        self.load_sequencing_groups_for_ids = load_sequencing_groups_for_ids(
            connection_getter
        )
        self.load_sequencing_groups_for_samples = load_sequencing_groups_for_samples(
            connection_getter
        )
        self.load_sequencing_group_counts_by_month = (
            load_sequencing_group_counts_by_month(connection_getter)
        )
        self.load_samples_for_ids = load_samples_for_ids(connection_getter)
        self.load_samples_for_projects = load_samples_for_projects(connection_getter)
        self.load_nested_samples_for_parents = load_nested_samples_for_parents(
            connection_getter
        )
        self.load_participants_for_ids = load_participants_for_ids(connection_getter)
        self.load_sequencing_groups_for_analysis_ids = (
            load_sequencing_groups_for_analysis_ids(connection_getter)
        )
        self.load_sequencing_groups_for_project_ids = (
            load_sequencing_groups_for_project_ids(connection_getter)
        )
        self.load_projects_for_ids = load_projects_for_ids(connection_getter)
        self.load_families_for_participants = load_families_for_participants(
            connection_getter
        )
        self.load_participants_for_families = load_participants_for_families(
            connection_getter
        )
        self.load_participants_for_projects = load_participants_for_projects(
            connection_getter
        )
        self.load_analyses_for_projects = load_analyses_for_projects(connection_getter)
        self.load_analyses_for_sequencing_groups = load_analyses_for_sequencing_groups(
            connection_getter
        )
        self.load_phenotypes_for_participants = load_phenotypes_for_participants(
            connection_getter
        )
        self.load_families_for_ids = load_families_for_ids(connection_getter)
        self.load_family_participants_for_families = (
            load_family_participants_for_families(connection_getter)
        )
        self.load_family_participants_for_participants = (
            load_family_participants_for_participants(connection_getter)
        )
        self.load_comments_for_sample_ids = load_comments_for_sample_ids(
            connection_getter
        )
        self.load_comments_for_participant_ids = load_comments_for_participant_ids(
            connection_getter
        )
        self.load_comments_for_family_ids = load_comments_for_family_ids(
            connection_getter
        )
        self.load_comments_for_assay_ids = load_comments_for_assay_ids(
            connection_getter
        )
        self.load_comments_for_project_ids = load_comments_for_project_ids(
            connection_getter
        )
        self.load_comments_for_sequencing_group_ids = (
            load_comments_for_sequencing_group_ids(connection_getter)
        )


class GraphQLContext(BaseContext):
    """Custom graphql context with loaders and db connection"""

    def __init__(self, loaders: LoaderContext, get_connection: GetConnection):
        self.loaders = loaders
        self.get_connection = get_connection


async def get_context(
    request: Request,  # noqa: ARG001
    connection_getter: GetConnection = get_projectless_db_connection_getter,
) -> GraphQLContext:
    """Get loaders / cache context for strawberyy GraphQL"""
    return GraphQLContext(
        loaders=LoaderContext(connection_getter),
        get_connection=connection_getter,
    )
