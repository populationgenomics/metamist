from test.testbase import DbIsolatedTest, run_as_sync

from graphql.error import GraphQLError, GraphQLSyntaxError

import api.graphql.schema
from db.python.layers import AnalysisLayer, ParticipantLayer
from metamist.graphql import configure_sync_client, gql, validate
from models.enums import AnalysisStatus
from models.models import (
    AnalysisInternal,
    AssayUpsertInternal,
    ParticipantUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from models.utils.sequencing_group_id_format import sequencing_group_id_format

default_assay_meta = {
    'sequencing_type': 'genome',
    'sequencing_technology': 'short-read',
    'sequencing_platform': 'illumina',
}


def _get_single_participant_upsert():
    return ParticipantUpsertInternal(
        external_id='Demeter',
        meta={},
        samples=[
            SampleUpsertInternal(
                external_id='sample_id001',
                meta={},
                type='blood',
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'reads': [
                                        {
                                            'basename': 'sample_id001.filename-R1.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id001.filename-R1.fastq.gz',
                                            'size': 111,
                                        },
                                        {
                                            'basename': 'sample_id001.filename-R2.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id001.filename-R2.fastq.gz',
                                            'size': 111,
                                        },
                                    ],
                                    'reads_type': 'fastq',
                                    'batch': 'M001',
                                    **default_assay_meta,
                                },
                            ),
                        ],
                    )
                ],
            )
        ],
    )


TEST_QUERY = gql(
    """
query MyQuery($project: String!) {
  project(name: $project) {
    participants {
      id
      samples {
        id
        sequencingGroups {
          id
          assays {
            id
          }
        }
      }
    }
  }
}"""
)


class TestGraphQL(DbIsolatedTest):
    """Test graphql functionality"""

    @run_as_sync
    async def setUp(self) -> None:
        """Setup the tests"""
        super().setUp()
        self.player = ParticipantLayer(self.connection)

    def test_validate_local_schema(self):
        """
        test using the bundled schema file (from regenerateapi.py)
         to make sure people can validate with authentication
        """
        validate(TEST_QUERY, use_local_schema=True)

    def test_validate_provided_schema(self):
        """
        Validate using schema directly from api.graphql.schema
        (strawberry has an as_str() method)
        """
        client = configure_sync_client(
            schema=api.graphql.schema.schema.as_str(), auth_token='FAKE'  # type: ignore
        )
        validate(TEST_QUERY, client=client)

    def test_bad_syntax_query(self):
        """Fail on bad syntax"""
        with self.assertRaises(GraphQLSyntaxError):
            gql(
                """
            query MyQuery(badtoken $project: String!) {
                project(name: $project) {
                    name
                }
            }"""
            )

    def test_bad_field_query(self):
        """Fail because the field doesn't exist"""
        # query syntactically validates
        query = gql(
            """
            query MyQuery($project: String!) {
                project(name: $project) {
                    thisFieldDoesntExist
                }
        }"""
        )
        with self.assertRaises(GraphQLError):
            validate(query, use_local_schema=True)

    @run_as_sync
    async def test_basic_graphql_query(self):
        """Test getting the summary for a project"""
        p = (await self.player.upsert_participants([_get_single_participant_upsert()]))[
            0
        ]

        query = """
query MyQuery($project: String!) {
  project(name: $project) {
    participants {
      id
      samples {
        id
        sequencingGroups {
          id
          assays {
            id
          }
        }
      }
    }
  }
}"""
        data = await self.run_graphql_query_async(
            query, variables={'project': self.project_name}
        )
        participants = data['project']['participants']
        self.assertEqual(1, len(participants))
        self.assertEqual(p.id, participants[0]['id'])
        samples = participants[0]['samples']
        self.assertEqual(1, len(samples))

        self.assertEqual(p.samples[0].to_external().id, samples[0]['id'])
        sequencing_groups = samples[0]['sequencingGroups']
        self.assertEqual(1, len(sequencing_groups))
        self.assertEqual(
            p.samples[0].sequencing_groups[0].to_external().id,
            sequencing_groups[0]['id'],
        )
        assays = sequencing_groups[0]['assays']
        self.assertEqual(
            1, len(participants[0]['samples'][0]['sequencingGroups'][0]['assays'])
        )
        self.assertEqual(
            p.samples[0].sequencing_groups[0].assays[0].id, assays[0]['id']
        )

    @run_as_sync
    async def test_sg_analyses_query(self):
        """Example graphql query of analyses from sequencing-group"""
        p = await self.player.upsert_participant(_get_single_participant_upsert())
        sg_id = p.samples[0].sequencing_groups[0].id

        alayer = AnalysisLayer(self.connection)
        await alayer.create_analysis(
            AnalysisInternal(
                sequencing_group_ids=[sg_id],
                type='cram',
                status=AnalysisStatus.COMPLETED,
                meta={},
                output='some-output',
            )
        )

        q = """
query MyQuery($sg_id: String!) {
  sequencingGroups(id: {in_: [$sg_id]}) {
    analyses {
      id
      meta
      output
    }
  }
}"""

        resp = await self.run_graphql_query_async(
            q, {'sg_id': sequencing_group_id_format(sg_id)}
        )
        self.assertIn('sequencingGroups', resp)
        self.assertEqual(1, len(resp['sequencingGroups']))
        self.assertIn('analyses', resp['sequencingGroups'][0])
        self.assertEqual(1, len(resp['sequencingGroups'][0]['analyses']))
        analyses = resp['sequencingGroups'][0]['analyses']
        self.assertIn('id', analyses[0])
        self.assertIn('meta', analyses[0])
        self.assertIn('output', analyses[0])

    @run_as_sync
    async def test_participant_phenotypes(self):
        """
        Test getting participant phentypes in graphql
        """
        # insert participant
        p = await self.player.upsert_participant(
            ParticipantUpsertInternal(external_id='Demeter', meta={}, samples=[])
        )

        phenotypes = {'phenotype1': 'value1', 'phenotype2': {'number': 123}}
        # insert participant_phenotypes
        await self.player.insert_participant_phenotypes({p.id: phenotypes})

        q = """
query MyQuery($pid: Int!) {
  participant(id: $pid) {
    phenotypes
  }
}"""

        resp = await self.run_graphql_query_async(q, {'pid': p.id})

        self.assertIn('participant', resp)
        self.assertIn('phenotypes', resp['participant'])
        self.assertDictEqual(phenotypes, resp['participant']['phenotypes'])
