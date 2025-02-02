from api.graphql.utils.loaders import connected_data_loader
from api.utils import group_by
from db.python.connect import Connection
from db.python.filters.generic import GenericFilter
from db.python.layers.family import FamilyLayer
from db.python.tables.family import FamilyFilter
from models.models.family import FamilyInternal, PedRowInternal


class FamilyLoaderKeys:
    FAMILIES_FOR_PARTICIPANTS = 'families_for_participants'
    FAMILY_PARTICIPANTS_FOR_FAMILIES = 'family_participants_for_families'
    FAMILY_PARTICIPANTS_FOR_PARTICIPANTS = 'family_participants_for_participants'
    FAMILIES_FOR_IDS = 'families_for_ids'


@connected_data_loader(FamilyLoaderKeys.FAMILIES_FOR_PARTICIPANTS)
async def load_families_for_participants(
    participant_ids: list[int], connection: Connection
) -> list[list[FamilyInternal]]:
    """
    Get families of participants, noting a participant can be in multiple families
    """
    flayer = FamilyLayer(connection)

    fam_map = await flayer.get_families_by_participants(participant_ids=participant_ids)
    return [fam_map.get(p, []) for p in participant_ids]


@connected_data_loader(FamilyLoaderKeys.FAMILIES_FOR_IDS)
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


@connected_data_loader(FamilyLoaderKeys.FAMILY_PARTICIPANTS_FOR_FAMILIES)
async def load_family_participants_for_families(
    family_ids: list[int], connection: Connection
) -> list[list[PedRowInternal]]:
    """
    DataLoader: get_family_participants_for_families
    """
    flayer = FamilyLayer(connection)
    fp_map = await flayer.get_family_participants_by_family_ids(family_ids)

    return [fp_map.get(fid, []) for fid in family_ids]


@connected_data_loader(FamilyLoaderKeys.FAMILY_PARTICIPANTS_FOR_PARTICIPANTS)
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
