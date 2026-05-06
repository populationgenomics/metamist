from unittest.mock import patch

import pytest

from metamist.parser.generic_parser import (
    PRIMARY_EXTERNAL_ORG,
    GenericParser,
    ParsedSample,
)

from db.python.connect import Connection
from db.python.layers.sample import SampleLayer
from models.models.sample import SampleUpsertInternal
from models.utils.sample_id_format import sample_id_transform_to_raw
from test.conftest import GraphQLQueryFunction, make_graphql_query_mock


class GenericParserForTest(GenericParser):
    """
    There's an abstract method on the GenericParser, so just implement a dummy
    """

    async def get_assays_from_group(self, *_, **__) -> list:
        """Dummy method to implement abstract method"""
        return []

    def get_primary_sample_id(self, *_, **__) -> str:
        """Dummy method to implement abstract method"""
        return ''

    def get_primary_participant_id(self, *_, **__) -> str | None:
        """Dummy method to implement abstract method"""
        return ''

    def has_participants(self, *_, **__) -> bool:
        """Dummy method to implement abstract method"""
        return True


class TestGenericParser:
    """Test generic parser specific methods"""

    @pytest.mark.asyncio
    @patch('metamist.parser.generic_parser.query_async')
    async def test_nested_samples_misc(
        self,
        mock_graphql_query,
        connection_with_project: Connection,
        graphql_query: GraphQLQueryFunction,
    ):
        """
        Test a bunch of things related to nested samples
        They're all sort of sequential, so just do them all in one test
        """
        assert connection_with_project.project is not None
        mock_graphql_query.side_effect = make_graphql_query_mock(graphql_query)

        slayer = SampleLayer(connection_with_project)
        u_sample = await slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'EX01'},
                type='blood',
                meta={},
                nested_samples=[
                    # intentionally don't have EX01.1
                    # SampleUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'EX01.1'),
                    SampleUpsertInternal(
                        external_ids={PRIMARY_EXTERNAL_ORG: 'EX01.2'},
                        type='blood',
                        meta={},
                    )
                ],
            )
        )
        assert u_sample.nested_samples is not None

        u_child2 = u_sample.nested_samples[0]

        p = GenericParserForTest(
            path_prefix=None,
            search_paths=[],
            project=connection_with_project.project.name,
        )

        def _make_parsed_sample(exid):
            return ParsedSample(
                participant=None,
                rows=[],
                internal_sid=None,
                external_sids={PRIMARY_EXTERNAL_ORG: exid},
                sample_type='blood',
                meta={},
            )

        nested_sample = _make_parsed_sample('EX01')
        child_1 = _make_parsed_sample('EX01.1')
        child_2 = _make_parsed_sample('EX01.2')
        nested_sample.samples.extend([child_1, child_2])

        # Test 1: test the matching works for all samples including nested
        await p.match_sample_ids([nested_sample])

        assert nested_sample.internal_sid is not None
        assert u_sample.id == sample_id_transform_to_raw(nested_sample.internal_sid)
        assert child_1.internal_sid is None
        assert child_2.internal_sid is not None
        assert u_child2.id == sample_id_transform_to_raw(child_2.internal_sid)

        # Test 2: test the summary generated is correctly counting for nested samples
        summary = p.prepare_summary([], [nested_sample], [], [])
        assert summary.samples.insert == 1
        assert summary.samples.update == 2

        # Test 3: test the to_sm method works for nested samples
        transport_model = nested_sample.to_sm()
        assert transport_model.id == nested_sample.internal_sid
        assert transport_model.nested_samples[0].id == child_1.internal_sid
        assert transport_model.nested_samples[1].id == child_2.internal_sid
