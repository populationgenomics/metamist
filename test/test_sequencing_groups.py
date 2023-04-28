from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers import SequencingGroupLayer, SampleLayer
from models.models import (
    SequencingGroupUpsertInternal,
    AssayUpsertInternal,
    SampleUpsertInternal,
)


def get_sample_model():
    """
    Get sample model with sequencing-groups, return in a function
    to protect against any mutation to this model
    """
    return SampleUpsertInternal(
        meta={},
        external_id='EX_ID',
        sequencing_groups=[
            SequencingGroupUpsertInternal(
                type='genome',
                technology='short-read',
                platform='ILLUMINA',
                meta={
                    'meta-key': 'meta-value',
                },
                external_ids={},
                assays=[
                    AssayUpsertInternal(
                        type='sequencing',
                        external_ids={},
                        meta={
                            'sequencing_type': 'genome',
                            'sequencing_platform': 'short-read',
                            'sequencing_technology': 'illumina',
                        },
                    )
                ],
            )
        ],
    )


class TestSequencingGroup(DbIsolatedTest):
    """Test sequencing groups business logic"""

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()
        self.sglayer = SequencingGroupLayer(self.connection)
        self.slayer = SampleLayer(self.connection)

    @run_as_sync
    async def test_insert_sequencing_group(self):
        """Test inserting and fetching a sequencing group"""
        sample_to_insert = get_sample_model()
        sample = await self.slayer.upsert_sample(sample_to_insert)
        sg_id = sample.sequencing_groups[0].id
        sg = await self.sglayer.get_sequencing_group_by_id(sg_id)

        inserted_sg = sample_to_insert.sequencing_groups[0]
        self.assertEqual(inserted_sg.id, sg_id)
        self.assertEqual(inserted_sg.type, sg.type)
        self.assertEqual(inserted_sg.technology, sg.technology)
        self.assertEqual(inserted_sg.platform.lower(), sg.platform.lower())
        self.assertDictEqual(inserted_sg.meta, sg.meta)

    # @run_as_sync
    # async def test_update_sequencing_group(self):
    #     sample = await self.slayer.upsert_sample(get_sample_model())
    #
    #     upsert_sg = SequencingGroupUpsertInternal(
    #         id=sample.sequencing_groups[0].id,
    #         type='transcriptome',
    #     )
