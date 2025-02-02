# # pylint: disable=no-value-for-parameter,redefined-builtin
# # ^ Do this because of the loader decorator
# import copy
# import dataclasses
# import enum
# from collections import defaultdict
# from typing import Any, TypedDict

# from fastapi import Request
# from strawberry.dataloader import DataLoader

# from api.utils import group_by
# from api.utils.db import get_projectless_db_connection
# from db.python.connect import Connection
# from db.python.filters import GenericFilter, get_hashable_value
# from db.python.layers import (
#     AnalysisLayer,
#     AssayLayer,
#     AuditLogLayer,
#     FamilyLayer,
#     ParticipantLayer,
#     SampleLayer,
#     SequencingGroupLayer,
# )
# from db.python.layers.comment import CommentLayer
# from db.python.tables.analysis import AnalysisFilter
# from db.python.tables.assay import AssayFilter
# from db.python.tables.family import FamilyFilter
# from db.python.tables.participant import ParticipantFilter
# from db.python.tables.sample import SampleFilter
# from db.python.tables.sequencing_group import SequencingGroupFilter
# from db.python.utils import NotFoundError
# from models.models import (
#     AnalysisInternal,
#     AssayInternal,
#     FamilyInternal,
#     ParticipantInternal,
#     Project,
#     ProjectId,
#     SampleInternal,
#     SequencingGroupInternal,
# )
# from models.models.audit_log import AuditLogInternal
# from models.models.comment import CommentEntityType, DiscussionInternal
# from models.models.family import PedRowInternal


# class LoaderKeys:
#     """
#     Keys for the data loaders, define them to it's clearer when we add / remove
#     them, and reduces the chance of typos
#     """

#     ANALYSES_FOR_SEQUENCING_GROUPS = 'analyses_for_sequencing_groups'


# @connected_data_loader_with_params(
#     LoaderKeys.ANALYSES_FOR_SEQUENCING_GROUPS, default_factory=list
# )
# async def load_analyses_for_sequencing_groups(
#     ids: list[int],
#     filter_: AnalysisFilter,
#     connection: Connection,
# ) -> dict[int, list[AnalysisInternal]]:
#     """
#     Type: (sequencing_group_id: int, status?: AnalysisStatus, type?: str)
#         -> list[list[AnalysisInternal]]
#     """
#     alayer = AnalysisLayer(connection)
#     filter_.sequencing_group_id = GenericFilter(in_=ids)
#     analyses = await alayer.query(filter_)
#     by_sg_id: dict[int, list[AnalysisInternal]] = defaultdict(list)
#     for a in analyses:
#         for sg in a.sequencing_group_ids:
#             by_sg_id[sg].append(a)
#     return by_sg_id
