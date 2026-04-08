import pytest
from graphql.error import GraphQLError, GraphQLSyntaxError

from metamist.graphql import configure_sync_client, gql, validate

import api.graphql.schema
from db.python.connect import Connection
from db.python.layers import AnalysisLayer, ParticipantLayer
from db.python.layers.family import FamilyLayer
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
from test.conftest import GraphQLQueryFunction


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


class TestGraphQL:
    """Test graphql functionality"""

    @pytest.fixture(autouse=True)
    async def set_up(self, connection_with_project: Connection) -> None:
        """Setup the tests"""
        self.player = ParticipantLayer(connection_with_project)
        assert connection_with_project.project is not None
        self.project_name = connection_with_project.project.name
        self.flayer = FamilyLayer(connection_with_project)
        self.alayer = AnalysisLayer(connection_with_project)

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
            schema=api.graphql.schema.schema.as_str(),
            auth_token='FAKE',  # type: ignore
        )
        validate(TEST_QUERY, client=client)

    def test_bad_syntax_query(self):
        """Fail on bad syntax"""
        with pytest.raises(GraphQLSyntaxError):
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
        with pytest.raises(GraphQLError):
            validate(query, use_local_schema=True)

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_basic_graphql_query(self, graphql_query: GraphQLQueryFunction):
        """Test getting the summary for a project"""
        p = (await self.player.upsert_participants([_get_single_participant_upsert()]))[
            0
        ]
        assert p.samples is not None
        assert p.samples[0].sequencing_groups is not None
        assert p.samples[0].sequencing_groups[0].assays is not None

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
        data = (await graphql_query(query, variables={'project': self.project_name}))[
            'data'
        ]

        participants = data['project']['participants']
        assert len(participants) == 1
        assert p.id == participants[0]['id']
        samples = participants[0]['samples']
        assert 1, len(samples)

        assert p.samples[0].to_external().id == samples[0]['id']
        sequencing_groups = samples[0]['sequencingGroups']
        assert len(sequencing_groups) == 1
        assert (
            p.samples[0].sequencing_groups[0].to_external().id
            == sequencing_groups[0]['id']
        )

        assays = sequencing_groups[0]['assays']
        assert len(participants[0]['samples'][0]['sequencingGroups'][0]['assays']) == 1
        assert p.samples[0].sequencing_groups[0].assays[0].id, assays[0]['id']

    @pytest.mark.asyncio
    async def test_query_sample_by_meta(self, graphql_query: GraphQLQueryFunction):
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
        values = (
            await graphql_query(
                q, {'project': self.project_name, 'meta': {'thisKey': 'value'}}
            )
        )['data']

        assert values
        assert len(values['project']['participants'][0]['samples']) == 1

        values2 = await graphql_query(
            q, {'project': self.project_name, 'meta': {'thisKeyDoesNotExistEver': '-1'}}
        )

        assert values2
        assert len(values2['data']['project']['participants'][0]['samples']) == 0

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_sg_analyses_query(self, graphql_query: GraphQLQueryFunction):
        """Example graphql query of analyses from sequencing-group"""
        p = await self.player.upsert_participant(_get_single_participant_upsert())
        assert p.samples is not None
        assert p.samples[0].sequencing_groups is not None
        assert p.samples[0].sequencing_groups[0].id is not None
        sg_id = p.samples[0].sequencing_groups[0].id

        await self.alayer.create_analysis(
            AnalysisInternal(
                sequencing_group_ids=[sg_id],
                type='cram',
                status=AnalysisStatus.COMPLETED,
                meta={},
                output='test://some-output',
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

        resp = await graphql_query(
            q,
            {'sg_id': sequencing_group_id_format(sg_id), 'project': self.project_name},
        )
        resp = resp['data']
        assert 'sequencingGroups' in resp
        assert len(resp['sequencingGroups']) == 1
        assert 'analyses' in resp['sequencingGroups'][0]
        assert len(resp['sequencingGroups'][0]['analyses']) == 1
        analyses = resp['sequencingGroups'][0]['analyses']
        assert 'id' in analyses[0]
        assert 'meta' in analyses[0]
        assert 'output' in analyses[0]

    @pytest.mark.asyncio
    async def test_project_analyses_query_with_meta(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Tests filtering analyses with the meta field when querying through a project"""

        test_meta_key = 'test_meta_key'
        test_meta_val = 'test_meta_value'
        a_id = await self.alayer.create_analysis(
            AnalysisInternal(
                status=AnalysisStatus.COMPLETED,
                type='cram',
                meta={test_meta_key: test_meta_val},
            )
        )

        q = f"""
query MyQuery($project: String!) {{
    project(name: $project) {{
        analyses(meta: {{{test_meta_key}: {{eq: "{test_meta_val}"}}}}) {{
            id
            meta
        }}
    }}
}}
        """
        # Use double curly braces {{ escape to a single { string literal.
        resp = await graphql_query(
            q,
            {
                'project': self.project_name,
                'meta_key': test_meta_key,
                'meta_value': test_meta_val,
            },
        )
        resp = resp['data']
        assert 'project' in resp
        assert 'analyses' in resp['project']
        assert len(resp['project']['analyses']) == 1

        analysis_resp = resp['project']['analyses'][0]
        assert 'id' in analysis_resp
        assert 'meta' in analysis_resp
        assert a_id == analysis_resp['id']
        assert analysis_resp['meta'] == {test_meta_key: test_meta_val}

    @pytest.mark.asyncio
    async def test_participant_phenotypes(self, graphql_query: GraphQLQueryFunction):
        """
        Test getting participant phentypes in graphql
        """
        # insert participant
        p = await self.player.upsert_participant(
            ParticipantUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Demeter'}, meta={}, samples=[]
            )
        )
        assert p.id is not None

        phenotypes = {'phenotype1': 'value1', 'phenotype2': {'number': 123}}
        # insert participant_phenotypes
        await self.player.insert_participant_phenotypes({p.id: phenotypes})

        q = """
query MyQuery($pid: Int!) {
  participant(id: $pid) {
    phenotypes
  }
}"""

        resp = (await graphql_query(q, {'pid': p.id}))['data']

        assert 'participant' in resp
        assert 'phenotypes' in resp['participant']
        assert phenotypes == resp['participant']['phenotypes']

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_family_participants(self, graphql_query: GraphQLQueryFunction):
        """Test inserting + querying family participants from different directions"""
        family_eid = 'family1'

        rows = [
            [family_eid, 'individual1', 'paternal1', 'maternal1', 'm', '1', 'note1'],
            [family_eid, 'paternal1', None, None, 'm', '0', 'note2'],
            [family_eid, 'maternal1', None, None, 'f', '1', 'note3'],
        ]

        await self.flayer.import_pedigree(None, rows, create_missing_participants=True)

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

        resp = (await graphql_query(q, {'project': self.project_name}))['data']
        assert resp is not None

        family_simple_obj = {'family': {'externalId': family_eid}}

        participants = resp['project']['participants']
        families = resp['project']['families']

        participants_by_eid = {p['externalId']: p for p in participants}
        assert len(participants) == 3

        assert {
            'externalId': 'individual1',
            'families': [{'externalId': family_eid}],
            'familyParticipants': [
                {'affected': 1, 'notes': 'note1', **family_simple_obj}
            ],
        } == participants_by_eid['individual1']

        assert len(participants_by_eid['individual1']['families']) == 1

        assert len(families) == 1
        assert family_eid == families[0]['externalId']

        sorted_fps = sorted(
            families[0]['familyParticipants'],
            key=lambda x: x['participant']['externalId'],
        )
        assert sorted_fps == [
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
        ]

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_query_family_by_meta(self, graphql_query: GraphQLQueryFunction):
        """Test querying families by meta field"""
        # Create two families with different meta
        fid1 = await self.flayer.create_family(
            external_ids={PRIMARY_EXTERNAL_ORG: 'family_with_meta'},
            description='Test family 1',
            coded_phenotype='phenotype1',
            meta={'study': 'study_a', 'priority': 'high'},
        )
        fid2 = await self.flayer.create_family(
            external_ids={PRIMARY_EXTERNAL_ORG: 'family_other_meta'},
            description='Test family 2',
            coded_phenotype='phenotype2',
            meta={'study': 'study_b', 'priority': 'low'},
        )

        # Query for families with specific meta
        q = """
        query MyQuery($project: String!, $meta: JSON!) {
            project(name: $project) {
                families(meta: $meta) {
                    id
                    externalId
                    meta
                }
            }
        }"""
        # Filter by study=study_a - should return only family 1
        values = (
            await graphql_query(
                q, {'project': self.project_name, 'meta': {'study': 'study_a'}}
            )
        )['data']
        assert values

        families = values['project']['families']
        assert len(families) == 1
        assert fid1 == families[0]['id']
        assert families[0]['externalId'] == 'family_with_meta'
        assert families[0]['meta']['study'] == 'study_a'

        # Filter by priority=low - should return only family 2
        values2 = (
            await graphql_query(
                q, {'project': self.project_name, 'meta': {'priority': 'low'}}
            )
        )['data']
        assert values2

        families2 = values2['project']['families']
        assert len(families2) == 1
        assert fid2 == families2[0]['id']

        # Filter by non-existent meta - should return empty
        values3 = (
            await graphql_query(
                q, {'project': self.project_name, 'meta': {'nonexistent': 'value'}}
            )
        )['data']
        assert values3

        assert len(values3['project']['families']) == 0

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_get_project_name_from_analysis(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test getting project name from analysis"""
        p = await self.player.upsert_participant(_get_single_participant_upsert())
        assert p.samples is not None
        assert p.samples[0].sequencing_groups is not None
        assert p.samples[0].sequencing_groups[0].id is not None
        sg_id = p.samples[0].sequencing_groups[0].id

        await self.alayer.create_analysis(
            AnalysisInternal(
                sequencing_group_ids=[sg_id],
                type='cram',
                status=AnalysisStatus.COMPLETED,
                meta={},
                output='test://some-output',
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

        resp = (await graphql_query(q, {'sg_id': sequencing_group_id_format(sg_id)}))[
            'data'
        ]
        assert 'sequencingGroups' in resp
        project_name = resp['sequencingGroups'][0]['analyses'][0]['project']['name']
        assert self.project_name == project_name
