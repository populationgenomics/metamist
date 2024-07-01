from test.testbase import DbIsolatedTest, run_as_sync

from db.python.filters import GenericFilter
from db.python.layers.participant import ParticipantLayer
from db.python.tables.participant import ParticipantFilter
from models.base import PRIMARY_EXTERNAL_ORG
from models.models.assay import AssayUpsertInternal
from models.models.participant import ParticipantUpsertInternal
from models.models.sample import SampleUpsertInternal
from models.models.sequencing_group import SequencingGroupUpsertInternal


def get_participant_to_insert(id_suffix='1'):
    """Helper function to create a participant object for insertion into the database"""
    return ParticipantUpsertInternal(
        external_ids={PRIMARY_EXTERNAL_ORG: 'P0' + id_suffix},
        meta={'pmeta': 'pvalue'},
        reported_sex=2,
        reported_gender='FEMALE',
        karyotype='XX',
        samples=[
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'S0' + id_suffix},
                type='blood',
                meta={'smeta': 'svalue'},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        external_ids={'default': 'SG0' + id_suffix},
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={'sgmeta': 'sgvalue'},
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'A0' + id_suffix},
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
    """Test participant related functionality"""

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()  # type: ignore

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
    async def test_query_by_exids(self):
        """Test query"""

        p = get_participant_to_insert()
        p.external_ids = {
            PRIMARY_EXTERNAL_ORG: 'P01',
            'external_org': 'ex01',
        }

        p = await self.player.upsert_participant(p)

        ps = await self.player.query(
            ParticipantFilter(external_id=GenericFilter(eq='P01'))
        )

        self.assertEqual(len(ps), 1)
        self.assertEqual(ps[0].id, p.id)

        ps = await self.player.query(
            ParticipantFilter(external_id=GenericFilter(in_=['ex01']))
        )
        self.assertEqual(len(ps), 1)
        self.assertEqual(ps[0].id, p.id)

        ps = await self.player.query(
            ParticipantFilter(external_id=GenericFilter(in_=['ex02']))
        )
        self.assertEqual(len(ps), 0)

    @run_as_sync
    async def test_graphql_query_by_id(self):
        """Test query by id using graphql"""
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

    @run_as_sync
    async def test_query_with_offset(self):
        """Test query providing an offset and a limit"""

        p1 = await self.player.upsert_participant(get_participant_to_insert('1'))
        p2 = await self.player.upsert_participant(get_participant_to_insert('2'))

        participants = await self.player.query(
            ParticipantFilter(project=GenericFilter(eq=self.project_id)),
            limit=1,
        )

        self.assertEqual(len(participants), 1)
        self.assertEqual(participants[0].id, p1.id)

        participants = await self.player.query(
            ParticipantFilter(project=GenericFilter(eq=self.project_id)),
            limit=1,
            skip=1,
        )

        self.assertEqual(len(participants), 1)
        self.assertEqual(participants[0].id, p2.id)
