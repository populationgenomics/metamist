from test.testbase import DbIsolatedTest, run_as_sync
import pytest

from metamist.parser.sample_json_parser import SampleJsonParser


class TestSampleJsonParser(DbIsolatedTest):
    """Test the SampleJsonParser"""

    @run_as_sync
    async def test_empty_json(self):
        """
        Test empty json
        """

        empty_record = {}

        parser = SampleJsonParser(
            project=self.project_name
        )

        # TODO
        # check the output of parse fun
        # for time being check for Exception

        with pytest.raises(NotImplementedError):
            await parser.parse(
                empty_record, dry_run=True
            )
