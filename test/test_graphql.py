from test.testbase import DbIsolatedTest, run_as_sync

from graphql.error import GraphQLError, GraphQLSyntaxError

import api.graphql.schema
from db.python.layers import AnalysisLayer, ParticipantLayer
from db.python.layers.family import FamilyLayer
from metamist.graphql import configure_sync_client, gql, validate
from models.enums import AnalysisStatus
from models.models import (
    PRIMARY_EXTERNAL_ORG,
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
        external_ids={PRIMARY_EXTERNAL_ORG: 'Demeter'},
        meta={},
        samples=[
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'sample_id001'},
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
    async def test_query_sample_by_meta(self):
        """Test querying a participant"""
        await self.player.upsert_participant(
            ParticipantUpsertInternal(
                meta={},
                external_ids={PRIMARY_EXTERNAL_ORG: 'Demeter'},
                samples=[
                    SampleUpsertInternal(
                        external_ids={PRIMARY_EXTERNAL_ORG: 'sample_id001'},
                        meta={'thisKey': 'value'},
                    )
                ],
            )
        )
        q = """
    query MyQuery($project: String!, $meta: JSON!) {
        project(name: $project) {
            participants {
                samples(meta: $meta) {
                    id
                }
            }
        }
    }"""
        values = await self.run_graphql_query_async(
            q, {'project': self.project_name, 'meta': {'thisKey': 'value'}}
        )
        assert values

        self.assertEqual(1, len(values['project']['participants'][0]['samples']))

        values2 = await self.run_graphql_query_async(
            q, {'project': self.project_name, 'meta': {'thisKeyDoesNotExistEver': '-1'}}
        )
        assert values2

        self.assertEqual(0, len(values2['project']['participants'][0]['samples']))

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
query MyQuery($sg_id: String!, $project: String!) {
  sequencingGroups(id: {in_: [$sg_id]}, project: {eq: $project}) {
    analyses(project: {eq: $project}) {
      id
      meta
      output
    }
  }
}"""

        resp = await self.run_graphql_query_async(
            q,
            {'sg_id': sequencing_group_id_format(sg_id), 'project': self.project_name},
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
            ParticipantUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'Demeter'}, meta={}, samples=[])
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

    @run_as_sync
    async def test_family_participants(self):
        """Test inserting + querying family participants from different directions"""
        family_layer = FamilyLayer(self.connection)

        family_eid = 'family1'

        rows = [
            [family_eid, 'individual1', 'paternal1', 'maternal1', 'm', '1', 'note1'],
            [family_eid, 'paternal1', None, None, 'm', '0', 'note2'],
            [family_eid, 'maternal1', None, None, 'f', '1', 'note3'],
        ]

        await family_layer.import_pedigree(None, rows, create_missing_participants=True)

        q = """
query MyQuery($project: String!) {
    project(name: $project) {
        participants {
            externalId
            familyParticipants {
                affected
                notes
                family {
                    externalId
                }
            }
            families {
                externalId
            }
        }
        families {
            externalId
            familyParticipants {
                affected
                notes
                participant {
                    externalId
                }
            }
        }
    }
}
"""

        resp = await self.run_graphql_query_async(q, {'project': self.project_name})
        assert resp is not None

        family_simple_obj = {'family': {'externalId': family_eid}}

        participants = resp['project']['participants']
        families = resp['project']['families']

        participants_by_eid = {p['externalId']: p for p in participants}
        self.assertEqual(3, len(participants))

        self.assertDictEqual(
            {
                'externalId': 'individual1',
                'families': [{'externalId': family_eid}],
                'familyParticipants': [
                    {'affected': 1, 'notes': 'note1', **family_simple_obj}
                ],
            },
            participants_by_eid['individual1'],
        )
        self.assertEqual(1, len(participants_by_eid['individual1']['families']))

        self.assertEqual(1, len(families))
        self.assertEqual(family_eid, families[0]['externalId'])

        sorted_fps = sorted(
            families[0]['familyParticipants'],
            key=lambda x: x['participant']['externalId'],
        )
        self.assertListEqual(
            sorted_fps,
            [
                {
                    'affected': 1,
                    'notes': 'note1',
                    'participant': {'externalId': 'individual1'},
                },
                {
                    'affected': 1,
                    'notes': 'note3',
                    'participant': {'externalId': 'maternal1'},
                },
                {
                    'affected': 0,
                    'notes': 'note2',
                    'participant': {'externalId': 'paternal1'},
                },
            ],
        )

    @run_as_sync
    async def test_get_project_name_from_analysis(self):
        """Test getting project name from analysis"""
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
  sequencingGroups(id: {eq: $sg_id}) {
    analyses {
      id
      project {
        name
      }
    }
  }
}"""

        resp = await self.run_graphql_query_async(
            q, {'sg_id': sequencing_group_id_format(sg_id)}
        )
        self.assertIn('sequencingGroups', resp)
        project_name = resp['sequencingGroups'][0]['analyses'][0]['project']['name']
        self.assertEqual(self.project_name, project_name)
