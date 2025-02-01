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
    ParticipantLayer,
)
from db.python.tables.participant import ParticipantFilter
from db.python.utils import NotFoundError
from models.models import (
    ParticipantInternal,
    ProjectId,
)


class ParticipantLoaderKeys:
    """Participant loader keys"""

    PARTICIPANTS_FOR_IDS = 'participants_for_ids'
    PARTICIPANTS_FOR_FAMILIES = 'participants_for_families'
    PARTICIPANTS_FOR_PROJECTS = 'participants_for_projects'
    PHENOTYPES_FOR_PARTICIPANTS = 'phenotypes_for_participants'


@connected_data_loader(ParticipantLoaderKeys.PARTICIPANTS_FOR_FAMILIES)
async def load_participants_for_families(
    family_ids: list[int], connection: Connection
) -> list[list[ParticipantInternal]]:
    """Get all participants in a family, doesn't include affected statuses"""
    player = ParticipantLayer(connection)
    pmap = await player.get_participants_by_families(family_ids)
    return [pmap.get(fid, []) for fid in family_ids]


@connected_data_loader_with_params(
    ParticipantLoaderKeys.PARTICIPANTS_FOR_PROJECTS, default_factory=list
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


@connected_data_loader(ParticipantLoaderKeys.PHENOTYPES_FOR_PARTICIPANTS)
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


@connected_data_loader(ParticipantLoaderKeys.PARTICIPANTS_FOR_IDS)
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
