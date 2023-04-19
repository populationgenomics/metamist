# pylint: disable=no-value-for-parameter
# ^ Do this because of the loader decorator
import enum
from collections import defaultdict
from strawberry.dataloader import DataLoader
from api.utils import get_projectless_db_connection, group_by

from db.python.layers import (
    AnalysisLayer,
    SampleLayer,
    AssayLayer,
    ParticipantLayer,
    SequencingGroupLayer,
    FamilyLayer,
)
from db.python.tables.project import ProjectPermissionsTable
from db.python.utils import ProjectId
from models.enums import AnalysisStatus
from models.models import (
    AssayInternal,
    SampleInternal,
    SequencingGroupInternal,
    AnalysisInternal,
    ParticipantInternal,
    FamilyInternal,
    Project,
)


def connected_data_loader(fn):
    """
    DataLoader Decorator for allowing DB connection to be bound to a loader
    """

    def inner(connection):
        async def wrapped(*args, **kwargs):
            return await fn(*args, **kwargs, connection=connection)

        return wrapped

    return inner


@connected_data_loader
async def load_assays_by_samples(
    query: list[tuple[int, str | None]], connection
) -> list[list[AssayInternal]]:
    """
    DataLoader: get_sequences_for_sample_ids
    """

    assaylayer = AssayLayer(connection)

    by_key: dict[tuple[int, str | None], list[AssayInternal]] = defaultdict(list)

    # group by all last fields, in case we add more
    for chunk in group_by(query, lambda x: x[1:]).values():
        assay_type = chunk[0][1]
        sample_ids = [x[0] for x in chunk]

        assays = await assaylayer.get_assays_for_sample_ids(
            sample_ids=sample_ids, assay_type=assay_type
        )
        assay_map = group_by(assays, lambda a: a.sample_id)
        for key in chunk:
            sample_id = key[0]
            if sample_id in assay_map:
                by_key[key].extend(assay_map[sample_id])

    return [by_key.get(q, []) for q in query]


@connected_data_loader
async def load_assays_by_sequencing_groups(
    sequencing_group_ids: list[int], connection
) -> list[list[AssayInternal]]:
    """
    Has format (sequencing_group_id: int, sequencing_type?: string)
    """
    assaylayer = AssayLayer(connection)

    # group by all last fields, in case we add more
    assays = await assaylayer.get_assays_for_sequencing_group_ids(
        sequencing_group_ids=sequencing_group_ids, check_project_ids=False
    )

    return [assays.get(sg, []) for sg in sequencing_group_ids]


@connected_data_loader
async def load_samples_for_participant_ids(
    participant_ids: list[int], connection
) -> list[list[SampleInternal]]:
    """
    DataLoader: get_samples_for_participant_ids
    """
    sample_map = await SampleLayer(connection).get_samples_by_participants(
        participant_ids
    )

    return [sample_map.get(pid, []) for pid in participant_ids]


@connected_data_loader
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


@connected_data_loader
async def load_sequencing_groups_for_samples(
    query: list[tuple[int, str | None]], connection
) -> list[list[SequencingGroupInternal]]:
    """
    Has format [(sample_id: int, sequencing_type?: string)]
    """
    sglayer = SequencingGroupLayer(connection)

    # group by all last fields, in case we add more
    by_key: dict[tuple[int, str | None], list[SequencingGroupInternal]] = defaultdict(
        list
    )
    for chunk in group_by(query, lambda x: x[1:]).values():
        sequencing_type = chunk[0][1]
        sample_ids = [x[0] for x in chunk]

        sequencing_groups = await sglayer.query(
            sample_ids=sample_ids,
            types=[sequencing_type] if sequencing_type else None,
        )
        sg_map = group_by(sequencing_groups, lambda sg: sg.sample_id)
        for key in chunk:
            sample_id = key[0]
            if sample_id in sg_map:
                by_key[key].extend(sg_map[sample_id])

    return [by_key.get(q, []) for q in query]


@connected_data_loader
async def load_samples_for_ids(
    sample_ids: list[int], connection
) -> list[SampleInternal]:
    """
    DataLoader: get_samples_for_ids
    """
    samples = await SampleLayer(connection).get_samples_by(sample_ids=sample_ids)
    # in case it's not ordered
    samples_map = {s.id: s for s in samples}
    return [samples_map.get(s) for s in sample_ids]


@connected_data_loader
async def load_samples_for_projects(project_ids: list[ProjectId], connection):
    """
    DataLoader: get_samples_for_project_ids
    """
    samples = await SampleLayer(connection).get_samples_by(project_ids=project_ids)
    samples_by_project = group_by(samples, lambda s: s.project)
    return [samples_by_project.get(pid, []) for pid in project_ids]


@connected_data_loader
async def load_participants_for_ids(
    participant_ids: list[int], connection
) -> list[list[ParticipantInternal]]:
    """
    DataLoader: get_participants_by_ids
    """
    player = ParticipantLayer(connection)
    persons = await player.get_participants_by_ids(
        [p for p in participant_ids if p is not None]
    )
    p_by_id = {p.id: p for p in persons}
    return [p_by_id.get(p) for p in participant_ids]


@connected_data_loader
async def load_samples_for_analysis_ids(
    analysis_ids: list[int], connection
) -> list[list[SampleInternal]]:
    """
    DataLoader: get_samples_for_analysis_ids
    """
    slayer = SampleLayer(connection)
    analysis_sample_map = await slayer.get_samples_by_analysis_ids(analysis_ids)

    return [analysis_sample_map.get(aid, []) for aid in analysis_ids]


@connected_data_loader
async def load_sequencing_groups_for_analysis_ids(
    analysis_ids: list[int], connection
) -> list[list[SequencingGroupInternal]]:
    """
    DataLoader: get_samples_for_analysis_ids
    """
    sglayer = SequencingGroupLayer(connection)
    analysis_sg_map = await sglayer.get_sequencing_groups_by_analysis_ids(analysis_ids)

    return [analysis_sg_map.get(aid, []) for aid in analysis_ids]


@connected_data_loader
async def load_sequencing_groups_for_project_ids(
    project_ids: list[int], connection
) -> list[list[SequencingGroupInternal]]:
    """
    DataLoader: get_sequencing_groups_for_project_ids
    """
    sglayer = SequencingGroupLayer(connection)
    sequencing_groups = await sglayer.query(project_ids=project_ids)
    seq_group_map = group_by(sequencing_groups, lambda sg: sg.project)

    return [seq_group_map.get(p, []) for p in project_ids]


@connected_data_loader
async def load_projects_for_ids(project_ids: list[int], connection) -> list[Project]:
    """
    Get projects by IDs
    """
    pttable = ProjectPermissionsTable(connection.connection)
    projects = await pttable.get_projects_by_ids(project_ids)
    p_by_id = {p.id: p for p in projects}
    return [p_by_id.get(p) for p in project_ids]


@connected_data_loader
async def load_families_for_participants(
    participant_ids: list[int], connection
) -> list[list[FamilyInternal]]:
    """
    Get families of participants, noting a participant can be in multiple families
    """
    flayer = FamilyLayer(connection)
    fam_map = await flayer.get_families_by_participants(participant_ids=participant_ids)
    return [fam_map.get(p, []) for p in participant_ids]


@connected_data_loader
async def load_participants_for_families(
    family_ids: list[int], connection
) -> list[list[ParticipantInternal]]:
    """Get all participants in a family, doesn't include affected statuses"""
    player = ParticipantLayer(connection)
    pmap = await player.get_participants_by_families(family_ids)
    return [pmap.get(fid, []) for fid in family_ids]


@connected_data_loader
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


@connected_data_loader
async def load_analyses_for_sequencing_groups(
    query: list[tuple[int, AnalysisStatus | None, str | None]], connection
) -> list[list[AnalysisInternal]]:
    """
    Type: (sequencing_group_id: int, status?: AnalysisStatus, type?: str)
        -> list[list[AnalysisInternal]]
    """
    alayer = AnalysisLayer(connection)
    by_key: dict[
        tuple[int, AnalysisStatus | None, str | None], list[AnalysisInternal]
    ] = defaultdict(list)
    for chunk in group_by(query, lambda x: x[1:]).values():
        sequencing_group_ids = [x[0] for x in chunk]
        status = chunk[0][1]
        analysis_type = chunk[0][2]

        analyses = await alayer.query_analysis(
            sequencing_group_ids=sequencing_group_ids,
            status=status,
            analysis_type=analysis_type,
        )
        for analysis in analyses:
            for sequencing_group_id in analysis.sequencing_group_ids:
                by_key[(sequencing_group_id, status, analysis_type)].append(analysis)

    return [by_key.get(q, []) for q in query]


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
    SAMPLES_FOR_ANALYSIS = 'samples_for_analysis'
    SAMPLES_FOR_PROJECTS = 'samples_for_projects'

    PARTICIPANTS_FOR_IDS = 'participants_for_ids'
    PARTICIPANTS_FOR_FAMILIES = 'participants_for_families'
    PARTICIPANTS_FOR_PROJECTS = 'participants_for_projects'

    FAMILIES_FOR_PARTICIPANTS = 'families_for_participants'

    SEQUENCING_GROUPS_FOR_IDS = 'sequencing_groups_for_ids'
    SEQUENCING_GROUPS_FOR_SAMPLES = 'sequencing_groups_for_samples'
    SEQUENCING_GROUPS_FOR_PROJECTS = 'sequencing_groups_for_projects'
    SEQUENCING_GROUPS_FOR_ANALYSIS = 'sequencing_groups_for_analysis'


async def get_context(connection=get_projectless_db_connection):
    return {
        'connection': connection,
        # projects
        LoaderKeys.PROJECTS_FOR_IDS: DataLoader(load_projects_for_ids(connection)),
        # participants
        LoaderKeys.PARTICIPANTS_FOR_IDS: DataLoader(
            load_participants_for_ids(connection)
        ),
        LoaderKeys.PARTICIPANTS_FOR_PROJECTS: DataLoader(
            load_participants_for_projects(connection)
        ),
        LoaderKeys.PARTICIPANTS_FOR_FAMILIES: DataLoader(
            load_participants_for_families(connection)
        ),
        # samples
        LoaderKeys.SAMPLES_FOR_IDS: DataLoader(load_samples_for_ids(connection)),
        LoaderKeys.SAMPLES_FOR_PROJECTS: DataLoader(
            load_samples_for_projects(connection)
        ),
        LoaderKeys.SAMPLES_FOR_PARTICIPANTS: DataLoader(
            load_samples_for_participant_ids(connection)
        ),
        LoaderKeys.SAMPLES_FOR_ANALYSIS: DataLoader(
            load_samples_for_analysis_ids(connection)
        ),
        # sequencing groups
        LoaderKeys.SEQUENCING_GROUPS_FOR_IDS: DataLoader(
            load_sequencing_groups_for_ids(connection)
        ),
        LoaderKeys.SEQUENCING_GROUPS_FOR_SAMPLES: DataLoader(
            load_sequencing_groups_for_samples(connection)
        ),
        LoaderKeys.SEQUENCING_GROUPS_FOR_PROJECTS: DataLoader(
            load_sequencing_groups_for_project_ids(connection)
        ),
        LoaderKeys.SEQUENCING_GROUPS_FOR_ANALYSIS: DataLoader(
            load_sequencing_groups_for_analysis_ids(connection)
        ),
        # assays
        LoaderKeys.ASSAYS_FOR_SAMPLES: DataLoader(load_assays_by_samples(connection)),
        LoaderKeys.ASSAYS_FOR_SEQUENCING_GROUPS: DataLoader(
            load_assays_by_sequencing_groups(connection)
        ),
        # analyses
        LoaderKeys.ANALYSES_FOR_SEQUENCING_GROUPS: DataLoader(
            load_analyses_for_sequencing_groups(connection), cache=False
        ),
        # families
        LoaderKeys.FAMILIES_FOR_PARTICIPANTS: DataLoader(
            load_families_for_participants(connection)
        ),
    }
