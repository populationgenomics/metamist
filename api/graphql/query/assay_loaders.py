from api.graphql.utils.loaders import (
    connected_data_loader,
    connected_data_loader_with_params,
)
from api.utils import group_by
from db.python.connect import Connection
from db.python.filters.generic import GenericFilter
from db.python.layers.assay import AssayLayer
from db.python.tables.assay import AssayFilter
from models.models.assay import AssayInternal


class AssayLoaderKeys:
    ASSAYS_FOR_IDS = 'assays_for_ids'
    ASSAYS_FOR_SAMPLES = 'sequences_for_samples'
    ASSAYS_FOR_SEQUENCING_GROUPS = 'assays_for_sequencing_groups'


@connected_data_loader(AssayLoaderKeys.ASSAYS_FOR_IDS)
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


@connected_data_loader_with_params(
    AssayLoaderKeys.ASSAYS_FOR_SAMPLES, default_factory=list
)
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


@connected_data_loader(AssayLoaderKeys.ASSAYS_FOR_SEQUENCING_GROUPS)
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
