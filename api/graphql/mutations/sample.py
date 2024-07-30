import strawberry
from strawberry.types import Info

from db.python.layers.sample import SampleLayer
from models.utils.sample_id_format import sample_id_transform_to_raw


@strawberry.type
class SampleMutations:
    """Sample mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: str,
        info: Info,
    ) -> int:
        """Add a comment to a sample"""
        connection = info.context['connection']
        sample_layer = SampleLayer(connection)
        result = await sample_layer.add_comment_to_sample(
            content=content, sample_id=sample_id_transform_to_raw(id)
        )
        return result

    # @strawberry.mutation
    # async def remove_sample(
    #     self,

    # @strawberry.mutation
    # async def edit_sample(
    #     self,
    #     sample_id: int,
    #     message: str,
    #     info: Info,
    # ) -> int:
    #     """Create a new sample"""
    #     # connection: Connection = info.context['connection']

    #     print(message)
    #     return 1
