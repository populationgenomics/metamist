from typing import Any

import pytest

from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers.participant import ParticipantLayer
from db.python.tables.participant import ParticipantFilter
from models.base import PRIMARY_EXTERNAL_ORG
from models.models.assay import AssayUpsertInternal
from models.models.participant import ParticipantUpsertInternal
from models.models.sample import SampleUpsertInternal
from models.models.sequencing_group import SequencingGroupUpsertInternal
from test.conftest import GraphQLQueryFunction


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


class TestParticipant:
    """Test participant related functionality"""

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_query_by_ids(self, connection_with_project: Connection):
        """Test query"""

        player = ParticipantLayer(connection_with_project)

        p = await player.upsert_participant(get_participant_to_insert())

        ps = await player.query(ParticipantFilter(id=GenericFilter(eq=p.id)))

        assert len(ps) == 1
        assert ps[0].id == p.id

        ps = await player.query(ParticipantFilter(id=GenericFilter(in_=[-1])))
        assert len(ps) == 0

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_query_by_exids(self, connection_with_project: Connection):
        """Test query"""

        p = get_participant_to_insert()
        p.external_ids = {
            PRIMARY_EXTERNAL_ORG: 'P01',
            'external_org': 'ex01',
        }

        player = ParticipantLayer(connection_with_project)

        p = await player.upsert_participant(p)

        ps = await player.query(ParticipantFilter(external_id=GenericFilter(eq='P01')))

        assert len(ps) == 1
        assert ps[0].id == p.id

        ps = await player.query(
            ParticipantFilter(external_id=GenericFilter(in_=['ex01']))
        )
        assert len(ps) == 1
        assert ps[0].id == p.id

        ps = await player.query(
            ParticipantFilter(external_id=GenericFilter(in_=['ex02']))
        )
        assert len(ps) == 0

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_graphql_query_by_id(
        self, connection_with_project: Connection, graphql_query: GraphQLQueryFunction
    ):
        """Test query by id using graphql"""
        player = ParticipantLayer(connection_with_project)

        p = await player.upsert_participant(get_participant_to_insert())

        q = """
            query TestGraphqlQueryById($projectName: String!, $pid: Int!) {
                project(name: $projectName) {
                    participants(id: { in_: [$pid] }) {
                        id
                    }
                }
            }
        """
        resp = await graphql_query(
            q, {'projectName': connection_with_project.project, 'pid': p.id}
        )
        assert resp is not None

        assert len(resp['project']['participants']) == 1

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_query_by_sample(self, connection_with_project: Connection):
        """Test query"""

        player = ParticipantLayer(connection_with_project)

        p = await player.upsert_participant(get_participant_to_insert())

        ps = await player.query(
            ParticipantFilter(
                sample=ParticipantFilter.ParticipantSampleFilter(
                    external_id=GenericFilter(in_=['S01'])
                )
            )
        )

        assert len(ps) == 1
        assert ps[0].id == p.id

        ps = await player.query(
            ParticipantFilter(
                sample=ParticipantFilter.ParticipantSampleFilter(
                    external_id=GenericFilter(in_=['S01-NOT_PRESENT'])
                )
            )
        )
        assert len(ps) == 0

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_query_with_offset(self, connection_with_project: Connection):
        """Test query providing an offset and a limit"""

        project_id = connection_with_project.project_id

        player = ParticipantLayer(connection_with_project)

        p1 = await player.upsert_participant(get_participant_to_insert('1'))
        p2 = await player.upsert_participant(get_participant_to_insert('2'))

        participants = await player.query(
            ParticipantFilter(project=GenericFilter(eq=project_id)),
            limit=1,
        )

        assert len(participants) == 1
        assert participants[0].id == p1.id

        participants = await player.query(
            ParticipantFilter(project=GenericFilter(eq=project_id)),
            limit=1,
            skip=1,
        )

        assert len(participants) == 1
        assert participants[0].id == p2.id

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_upsert_participant_with_phenotypes(
        self, connection_with_project: Connection, graphql_query: GraphQLQueryFunction
    ):
        """Test upserting participant with phenotypes"""

        phenotypes: dict[str, Any] = {
            'phenotype1': 'value1',
            'phenotype2': {'number': 123},
        }
        player = ParticipantLayer(connection_with_project)

        p = await player.upsert_participant(
            ParticipantUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Demeter'},
                meta={},
                samples=[],
                phenotypes=phenotypes,
            )
        )

        q = """
        query GetParticipant($pid: Int!) {
            participant(id: $pid) {
                id
                phenotypes
            }
        }"""

        resp = await graphql_query(q, {'pid': p.id})

        resp_participant = resp['participant']

        assert resp_participant['id'] == p.id

        assert resp_participant['phenotypes'] == phenotypes

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_upsert_participant_with_phenotypes_twice(
        self, connection_with_project: Connection, graphql_query: GraphQLQueryFunction
    ):
        """Test upserting and then updating participant with phenotypes"""

        phenotypes1: dict[str, Any] = {
            'phenotype1': 'value1',
            'phenotype2': {'number': 123},
        }

        phenotypes2: dict[str, Any] = {
            'phenotype1': 'value2',
            'phenotype2': {'number': 345},
            'phenotype3': {'number': 678},
        }

        player = ParticipantLayer(connection_with_project)

        p1 = await player.upsert_participant(
            ParticipantUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Demeter'},
                meta={},
                samples=[],
                phenotypes=phenotypes1,
            )
        )

        p2 = await player.upsert_participant(
            ParticipantUpsertInternal(
                id=p1.id,
                meta={},
                samples=[],
                phenotypes=phenotypes2,
            )
        )

        # ensure second upsert didn't create a new participant
        assert p1.id == p2.id

        q = """
        query GetParticipant($pid: Int!) {
            participant(id: $pid) {
                id
                phenotypes
            }
        }"""

        resp = await graphql_query(q, {'pid': p1.id})

        resp_participant = resp['participant']

        assert resp_participant['id'] == p2.id

        assert resp_participant['phenotypes'] == phenotypes2

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_graphql_upsert_participant_with_phenotypes(
        self, connection_with_project: Connection, graphql_query: GraphQLQueryFunction
    ):
        """Test upserting and then updating participant with phenotypes, via graphql"""
        project_name = connection_with_project.project

        phenotypes1: dict[str, Any] = {
            'phenotype1': 'value1',
            'phenotype2': {'number': 123},
        }

        phenotypes2: dict[str, Any] = {
            'phenotype1': 'value2',
            'phenotype2': {'number': 345},
            'phenotype3': {'number': 678},
        }

        mutation = """
        mutation ParticipantPhenotype($participants:[ParticipantUpsertInput!]!, $project: String!) {
            participant {
                upsertParticipants(project: $project, participants:$participants) {
                    id
                    phenotypes
                }
            }
        }
        """

        p1_resp = await graphql_query(
            mutation,
            {
                'project': project_name,
                'participants': [
                    {
                        'externalIds': {PRIMARY_EXTERNAL_ORG: 'Demeter'},
                        'phenotypes': phenotypes1,
                    }
                ],
            },
        )

        p1 = p1_resp['participant']['upsertParticipants'][0]

        p1_resp = await graphql_query(
            mutation,
            {
                'project': project_name,
                'participants': [
                    {
                        'id': p1['id'],
                        'phenotypes': phenotypes2,
                    }
                ],
            },
        )

        p2 = p1_resp['participant']['upsertParticipants'][0]

        # ensure second upsert didn't create a new participant
        assert p1['id'] == p2['id']

        q = """
        query GetParticipant($pid: Int!) {
            participant(id: $pid) {
                id
                phenotypes
            }
        }"""

        resp = await graphql_query(q, {'pid': p1['id']})

        resp_participant = resp['participant']

        assert resp_participant['id'] == p2['id']

        assert resp_participant['phenotypes'] == phenotypes2
