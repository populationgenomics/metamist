import unittest

from metamist.models import AssayUpsert


class TestMetamistGeneratedLibrary(unittest.TestCase):
    """
    Test the generated library, mostly instantiating models to check we don't regress.
    """

    def test_create_assay_with_id(self):
        """Just test instantiating the model"""
        _ = AssayUpsert(id=1, external_ids={'blah': 'blah'})

    def test_create_assay_no_id(self):
        """Test instantiating the model without an id"""
        _ = AssayUpsert(id=None, external_ids={'blah': 'blah'})
