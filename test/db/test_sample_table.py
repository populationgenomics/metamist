# pylint: disable=missing-class-docstring,missing-function-docstring
import unittest

from models.models.sample import Sample
from db.python.sample import SampleTable


class TestSampleTable(unittest.TestCase):
    def test_get_01(self):
        external_sample_id = 'CPG_T001'
        sample_table = SampleTable.from_project('sm_dev', None)

        row = sample_table.get_single_by_external_id(external_sample_id)
        self.assertIsInstance(row, Sample)
        self.assertEqual(row.external_id, external_sample_id)
