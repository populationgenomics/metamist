from tempfile import TemporaryFile

from fastapi import HTTPException, UploadFile

from api.routes import family
from test.testbase import DbIsolatedTest, run_as_sync


class TestFamilyImportEndpoint(DbIsolatedTest):
    """Family testing methods"""

    @run_as_sync
    async def test_import_families_empty_file(self):
        """
        Test importing families from a file where the file is empty.
        """
        with TemporaryFile(prefix='test', suffix='.tsv') as f:
            emptyTestFile = UploadFile(f)

            # Test has_header = true
            with self.assertRaises(HTTPException) as context:
                _ = await family.import_families(
                    emptyTestFile,
                    has_header=True,
                    delimiter='\t',
                    connection=self.connection,
                )
            self.assertEqual(context.exception.status_code, 400)
            self.assertIn(
                'A header was expected but file is empty.', context.exception.detail
            )

            # Test no op when has_header = false
            response = await family.import_families(
                emptyTestFile,
                has_header=False,
                delimiter='\t',
                connection=self.connection,
            )
            self.assertDictEqual(
                response,
                {
                    'success': True,
                    'warnings': ['Submitted file was empty'],
                },
            )

    @run_as_sync
    async def test_import_families_header_no_content(self):
        """
        Test importing families from a file with a header but no data.
        """
        with TemporaryFile(mode='wb+', prefix='test', suffix='.tsv') as f:
            f.write(b'Some\ttest\theader\twithout\tdata\n')
            f.seek(0)
            headerOnlyFile = UploadFile(f)

            # Test has_header = true
            response = await family.import_families(
                headerOnlyFile,
                has_header=True,
                delimiter='\t',
                connection=self.connection,
            )
            self.assertDictEqual(
                response,
                {
                    'success': True,
                    'warnings': ['Submitted file contained a header with no data'],
                },
            )

    @run_as_sync
    async def test_import_families_valid_data(self):
        """
        Test importing families with valid file contents.
        """
        data = [
            ['familyid', 'description', 'phenotype'],
            ['Smith', 'Blacksmiths', 'burnt'],
            ['Jones', 'From Wales', 'sings well'],
            ['Taylor', 'Post Norman', 'sews'],
        ]
        fileContent = '\n'.join(['\t'.join(row) for row in data]).encode(
            encoding='utf-8-sig'
        )

        with TemporaryFile(mode='wb+', prefix='test', suffix='.tsv') as f:
            f.write(fileContent)
            f.seek(0)
            testFile = UploadFile(f)

            # Test has_header = true
            response = await family.import_families(
                testFile,
                has_header=True,
                delimiter='\t',
                connection=self.connection,
            )
            self.assertDictEqual(response, {'success': True})

            f.seek(0)
            # Test has_header = false
            response = await family.import_families(
                testFile,
                has_header=False,
                delimiter='\t',
                connection=self.connection,
            )
            self.assertDictEqual(response, {'success': True})
