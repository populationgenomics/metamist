from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.participant import ParticipantLayer
from db.python.tables.participant import ParticipantFilter
from db.python.utils import GenericFilter
from models.models.assay import AssayUpsertInternal
from models.models.participant import ParticipantUpsertInternal
from models.models.sample import SampleUpsertInternal
from models.models.sequencing_group import SequencingGroupUpsertInternal


def get_participant_to_insert():
    return ParticipantUpsertInternal(
        external_id="P01",
        meta={'pmeta': 'pvalue'},
        reported_sex=2,
        reported_gender='FEMALE',
        karyotype='XX',
        samples=[
            SampleUpsertInternal(
                external_id="S01",
                type='blood',
                meta={'smeta': 'svalue'},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        external_ids={'default': 'SG01'},
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={'sgmeta': 'sgvalue'},
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'A01'},
                                meta={
                                    'ameta': 'avalue',
                                    'sequencing_type': 'genome',
                                    'sequencing_platform': 'illumina',
                                    'sequencing_technology': 'short-read',
                                },
                            )
                        ],
                    )
                ],
            )
        ],
    )


class TestParticipant(DbIsolatedTest):
    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        self.player = ParticipantLayer(self.connection)

    @run_as_sync
    async def test_query_by_ids(self):
        """Test query"""

        p = await self.player.upsert_participant(get_participant_to_insert())

        ps = await self.player.query(ParticipantFilter(id=GenericFilter(eq=p.id)))

        self.assertEqual(len(ps), 1)
        self.assertEqual(ps[0].id, p.id)

        ps = await self.player.query(ParticipantFilter(id=GenericFilter(in_=[-1])))
        self.assertEqual(len(ps), 0)

    @run_as_sync
    async def test_graphql_query_by_id(self):

        p = await self.player.upsert_participant(get_participant_to_insert())

        q = """
query TestGraphqlQueryById($projectName: String!, $pid: Int!) {
    project(name: $projectName) {
        participants(id: { in_: [$pid] }) {
            id
        }
    }
}
"""
        resp = await self.run_graphql_query_async(
            q, {'projectName': self.project_name, 'pid': p.id}
        )
        assert resp is not None

        self.assertEqual(1, len(resp['project']['participants']))

    @run_as_sync
    async def test_query_by_sample(self):
        """Test query"""

        p = await self.player.upsert_participant(get_participant_to_insert())

        ps = await self.player.query(
            ParticipantFilter(
                sample=ParticipantFilter.ParticipantSampleFilter(
                    external_id=GenericFilter(in_=['S01'])
                )
            )
        )

        self.assertEqual(len(ps), 1)
        self.assertEqual(ps[0].id, p.id)

        ps = await self.player.query(
            ParticipantFilter(
                sample=ParticipantFilter.ParticipantSampleFilter(
                    external_id=GenericFilter(in_=['S01-NOT_PRESENT'])
                )
            )
        )
        self.assertEqual(len(ps), 0)
