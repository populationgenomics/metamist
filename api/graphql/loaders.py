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
from models.models import (
    Participant,
    Sample,
    AssayInternal,
    SampleInternal,
    SequencingGroupInternal,
    AnalysisInternal,
    ParticipantInternal,
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
    sample_ids: list[int], assay_type: str | None, connection
) -> list[list[AssayInternal]]:
    """
    DataLoader: get_sequences_for_sample_ids
    """
    assaylayer = AssayLayer(connection)
    assays = await assaylayer.get_assays_for_sample_ids(
        sample_ids=sample_ids, assay_type=assay_type
    )
    seq_map: dict[int, list[AssayInternal]] = group_by(
        assays, lambda assay: int(assay.sample_id)
    )

    return [seq_map.get(sid, []) for sid in sample_ids]


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
    sequencing_groups = await SequencingGroupLayer(connection).get_sequencing_groups_by_ids(
        sequencing_group_ids
    )
    # in case it's not ordered
    sequencing_groups_map = {sg.id: sg for sg in sequencing_groups}
    return [sequencing_groups_map.get(sg) for sg in sequencing_group_ids]

@connected_data_loader
async def load_sequencing_groups_for_samples(
        sample_ids: list[int], connection
) -> list[list[SequencingGroupInternal]]:
    """
    DataLoader: get_sequencing_groups_for_sample_ids
    """
    sequencing_groups = await SequencingGroupLayer(connection).query(
        sample_ids=sample_ids
    )
    sequencing_group_map: dict[int, list[SequencingGroupInternal]] = group_by(
        sequencing_groups, lambda sequencing_group: int(sequencing_group.sample_id)
    )

    return [sequencing_group_map.get(sid, []) for sid in sample_ids]

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
async def load_participants_for_ids(
    participant_ids: list[int], connection
) -> list['ParticipantInternal']:
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
) -> list['Sample']:
    """
    DataLoader: get_samples_for_analysis_ids
    """
    slayer = SampleLayer(connection)
    analysis_sample_map = await slayer.get_samples_by_analysis_ids(analysis_ids)

    return [analysis_sample_map.get(aid, []) for aid in analysis_ids]


@connected_data_loader
async def load_sequencing_groups_for_analysis_ids(
    analysis_ids: list[int], connection
) -> list['Sample']:
    """
    DataLoader: get_samples_for_analysis_ids
    """
    sglayer = SequencingGroupLayer(connection)
    analysis_sg_map = await sglayer.get_sequencing_groups_by_analysis_ids(analysis_ids)

    return [analysis_sg_map.get(aid, []) for aid in analysis_ids]

@connected_data_loader
async def load_sequencing_groups_for_project_ids(
    project_ids: list[int], connection
) -> list['SequencingGroupInternal']:
    """
    DataLoader: get_sequencing_groups_for_project_ids
    """
    sglayer = SequencingGroupLayer(connection)
    sequencing_groups = await sglayer.query(project_ids=project_ids)

    return sequencing_groups

@connected_data_loader
async def load_projects_for_ids(project_ids: list[int], connection) -> list['Project']:
    pttable = ProjectPermissionsTable(connection.connection)
    projects = await pttable.get_projects_by_ids(project_ids)
    p_by_id = {p.id: p for p in projects}
    return [p_by_id.get(p) for p in project_ids]


@connected_data_loader
async def load_families_for_participants(
    participant_ids: list[int], connection
) -> list[list['Family']]:
    flayer = FamilyLayer(connection)
    fam_map = await flayer.get_families_by_participants(participant_ids=participant_ids)
    return [fam_map.get(p, []) for p in participant_ids]


@connected_data_loader
async def load_participants_for_families(
    family_ids: list[int], connection
) -> list[list[ParticipantInternal]]:
    player = ParticipantLayer(connection)
    pmap = await player.get_participants_by_families(family_ids)
    return [pmap.get(fid, []) for fid in family_ids]


@connected_data_loader
async def load_analyses_for_samples(queries: list[dict], connection):
    """
    It's a little awkward, but we want extra parameter,
    """
    # supported params
    supported_params = ['type', 'status']
    ordered_lookup = ['sample', *supported_params]

    grouped_by_params = group_by(
        queries, lambda q: tuple((k, q.get(k)) for k in supported_params)
    )
    alayer = AnalysisLayer(connection)
    cached_for_return = defaultdict(list)
    for group in grouped_by_params.values():
        analysis_type = group[0].get('type')
        status = group[0].get('status')
        sample_ids = [r['sample'] for r in group]

        by_sample = defaultdict(list)

        analyses = await alayer.get_analyses_for_samples(
            sample_ids,
            analysis_type=analysis_type,
            status=status,
        )

        for a in analyses:
            for sample_id in a.sample_ids:
                by_sample[sample_id].append(a)

        for row in group:
            # use tuple
            if result_for_sample := by_sample.get(row['sample']):
                key = tuple(row.get(k) for k in ordered_lookup)
                cached_for_return[key] = result_for_sample

    return [
        cached_for_return.get(tuple(row.get(k) for k in ordered_lookup), [])
        for row in queries
    ]


class LoaderKeys(enum.Enum):
    ASSAYS_FOR_SAMPLES = 'sequences_for_samples'
    SAMPLES_FOR_PARTICIPANT = 'samples_for_participants'
    SAMPLES_FOR_IDS = 'samples_for_ids'
    PARTICIPANTS_FOR_IDS = 'participants_for_ids'
    SAMPLES_FOR_ANALYSIS = 'samples_for_analysis'
    PROJECTS_FOR_IDS = 'projects_for_id'
    FAMILIES_FOR_PARTICIPANTS = 'families_for_participants'
    PARTICIPANTS_FOR_FAMILIES = 'participants_for_families'
    ANALYSES_FOR_SAMPLES = 'analyses_for_samples'
    SEQUENCING_GROUPS_FOR_IDS = 'sequencing_groups_for_ids'
    SEQUENCING_GROUPS_FOR_SAMPLES = 'sequencing_groups_for_samples'
    SEQUENCING_GROUPS_FOR_PROJECTS = 'sequencing_groups_for_projects'


async def get_context(connection=get_projectless_db_connection):
    return {
        'connection': connection,
        LoaderKeys.SAMPLES_FOR_PARTICIPANT: DataLoader(
            load_samples_for_participant_ids(connection)
        ),
        LoaderKeys.SAMPLES_FOR_IDS: DataLoader(load_samples_for_ids(connection)),
        LoaderKeys.ASSAYS_FOR_SAMPLES: DataLoader(load_assays_by_samples(connection)),
        LoaderKeys.SAMPLES_FOR_ANALYSIS: DataLoader(
            load_samples_for_analysis_ids(connection)
        ),
        LoaderKeys.PARTICIPANTS_FOR_IDS: DataLoader(load_participants_for_ids(connection)),
        LoaderKeys.PROJECTS_FOR_IDS: DataLoader(load_projects_for_ids(connection)),
        LoaderKeys.ANALYSES_FOR_SAMPLES: DataLoader(
            load_analyses_for_samples(connection), cache=False
        ),
        LoaderKeys.FAMILIES_FOR_PARTICIPANTS: DataLoader(
            load_families_for_participants(connection)
        ),
        LoaderKeys.PARTICIPANTS_FOR_FAMILIES: DataLoader(
            load_participants_for_families(connection)
        ),
        LoaderKeys.SEQUENCING_GROUPS_FOR_IDS: DataLoader(
            load_sequencing_groups_for_ids(connection)
        ),
        LoaderKeys.SEQUENCING_GROUPS_FOR_SAMPLES: DataLoader(
            load_sequencing_groups_for_samples(connection)
        ),
        LoaderKeys.SEQUENCING_GROUPS_FOR_PROJECTS: DataLoader(
            load_sequencing_groups_for_project_ids(connection)
        )
    }
