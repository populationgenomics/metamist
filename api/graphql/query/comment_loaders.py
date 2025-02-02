from api.graphql.utils.loaders import connected_data_loader
from db.python.connect import Connection
from db.python.layers.comment import CommentLayer
from models.models.comment import CommentEntityType, DiscussionInternal


class CommentLoaderKeys:
    COMMENTS_FOR_SAMPLE_IDS = 'comments_for_sample_ids'
    COMMENTS_FOR_PARTICIPANT_IDS = 'comments_for_participant_ids'
    COMMENTS_FOR_ASSAY_IDS = 'comments_for_assay_ids'
    COMMENTS_FOR_PROJECT_IDS = 'comments_for_project_ids'
    COMMENTS_FOR_SEQUENCING_GROUP_IDS = 'comments_for_sequencing_group_ids'
    COMMENTS_FOR_FAMILY_IDS = 'comments_for_family_ids'


@connected_data_loader(CommentLoaderKeys.COMMENTS_FOR_SAMPLE_IDS)
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


@connected_data_loader(CommentLoaderKeys.COMMENTS_FOR_PARTICIPANT_IDS)
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


@connected_data_loader(CommentLoaderKeys.COMMENTS_FOR_FAMILY_IDS)
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


@connected_data_loader(CommentLoaderKeys.COMMENTS_FOR_ASSAY_IDS)
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


@connected_data_loader(CommentLoaderKeys.COMMENTS_FOR_PROJECT_IDS)
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


@connected_data_loader(CommentLoaderKeys.COMMENTS_FOR_SEQUENCING_GROUP_IDS)
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
