from test.testbase import DbIsolatedTest, run_as_sync
from metamist.parser.sample_json_parser import SampleJsonParser


class TestSampleJsonParser(DbIsolatedTest):
    """Test the SampleJsonParser"""

    @run_as_sync
    async def test_sample_parser_empty_json(self):
        """
        Test empty json
        """

        empty_record = {}

        parser = SampleJsonParser(
            project=self.project_name
        )

        # TODO
        # check the output of parse fun
        # for time being check for NotImplementedError Exception
        try:
            await parser.parse(
                empty_record, dry_run=True
            )
            result = False
        except NotImplementedError:
            result = True

        self.assertTrue(result, msg='Parse did not cause the wanted error.')
