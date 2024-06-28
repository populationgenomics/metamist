from test.testbase import DbIsolatedTest, run_as_sync
from unittest.mock import patch

from db.python.layers.sample import SampleLayer
from metamist.parser.generic_parser import (
    PRIMARY_EXTERNAL_ORG,
    GenericParser,
    ParsedSample,
)
from models.models.sample import SampleUpsertInternal
from models.utils.sample_id_format import sample_id_transform_to_raw


class TestGenericParser(DbIsolatedTest):

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    async def test_sample_matching_with_nested_samples(self, mock_graphql_query):

        mock_graphql_query.side_effect = self.run_graphql_query_async

        slayer = SampleLayer(self.connection)
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

        u_child2 = u_sample.nested_samples[0]

        p = GenericParser(path_prefix=None, search_paths=[], project=self.project_name)

        def _make_parsed_sample(exid):
            return ParsedSample(
                participant=None,
                rows=[],
                internal_sid=None,
                external_sid=exid,
                sample_type='blood',
                meta={},
            )

        nested_sample = _make_parsed_sample('EX01')
        child_1 = _make_parsed_sample('EX01.1')
        child_2 = _make_parsed_sample('EX01.2')
        nested_sample.samples.extend([child_1, child_2])

        await p.match_sample_ids([nested_sample])

        self.assertEqual(
            u_sample.id, sample_id_transform_to_raw(nested_sample.internal_sid)
        )
        self.assertIsNone(child_1.internal_sid)
        self.assertEqual(u_child2.id, sample_id_transform_to_raw(child_2.internal_sid))

        summary = p.prepare_summary([], [nested_sample], [], [])
        self.assertEqual(1, summary.samples.insert)
        self.assertEqual(2, summary.samples.update)

        transport_model = nested_sample.to_sm()
        self.assertEqual(transport_model.id, nested_sample.internal_sid)
        self.assertEqual(transport_model.nested_samples[0].id, child_1.internal_sid)
        self.assertEqual(transport_model.nested_samples[1].id, child_2.internal_sid)
