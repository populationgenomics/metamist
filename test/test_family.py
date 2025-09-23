from test.testbase import DbIsolatedTest, run_as_sync

from fastapi import UploadFile
from api.routes import family
from tempfile import TemporaryFile


class TestPedigree(DbIsolatedTest):
    """Family testing methods"""

    @run_as_sync
    async def test_import_families_empty_file(self):
        """
        Test importing families from a file where the file is empty.
        """
        with TemporaryFile(prefix='test', suffix='.tsv') as f:
            emptyTestFile = UploadFile(f)

            # Test has_header = true
            response = await family.import_families(
                emptyTestFile,
                has_header=True,
                delimiter='\t',
                connection=self.connection,
            )
            assert response == (
                {
                    'success': False,
                    'message': 'A header was expected but file is empty.',
                },
                400,
            )

            # Test no op when has_header = false
            response = await family.import_families(
                emptyTestFile,
                has_header=False,
                delimiter='\t',
                connection=self.connection,
            )
            assert response == ({}, 204)
