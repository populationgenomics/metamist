from tempfile import TemporaryFile

from fastapi import HTTPException, UploadFile

from api.routes import family
from db.python.filters import GenericFilter
from db.python.layers.family import FamilyLayer
from models.models import PRIMARY_EXTERNAL_ORG
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

    @run_as_sync
    async def test_import_families_valid_data_and_meta(self):
        """
        Test importing families with valid file contents.
        """
        data = [
            ['familyid', 'description', 'phenotype', 'meta'],
            ['Smith', 'Blacksmiths', 'burnt', '{"key1": "value1"}'],
            ['Jones', 'From Wales', 'sings well', ''],
            ['Taylor', 'Post Norman', 'sews', '{"key3": "value3"}'],
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

            # get family and verify that meta was imported correctly
            family_layer = FamilyLayer(self.connection)

            family_list = await family_layer.query(
                family.FamilyFilter(
                    external_id=GenericFilter(eq='Smith'),
                    project=GenericFilter(eq=self.project_id),
                )
            )
            self.assertEqual(len(family_list), 1)
            fam = family_list[0]
            self.assertEqual(fam.meta, {'key1': 'value1'})

    @run_as_sync
    async def test_create_family_with_meta(self):
        """
        Test creating a family with meta
        """
        family_layer = FamilyLayer(self.connection)

        # Create a family with meta
        family_id = await family_layer.create_family(
            external_ids={PRIMARY_EXTERNAL_ORG: 'test-family'},
            description='Test family',
            coded_phenotype='test-phenotype',
            meta={'key1': 'value1', 'nested': {'key2': 'value2'}},
        )

        self.assertIsNotNone(family_id)

        # Query the family and verify meta
        created_family = await family_layer.get_family_by_internal_id(family_id)

        self.assertEqual(created_family.id, family_id)
        self.assertEqual(
            created_family.external_ids[PRIMARY_EXTERNAL_ORG], 'test-family'
        )
        self.assertEqual(created_family.description, 'Test family')
        self.assertEqual(created_family.coded_phenotype, 'test-phenotype')
        self.assertEqual(created_family.meta['key1'], 'value1')
        self.assertEqual(created_family.meta['nested']['key2'], 'value2')

    @run_as_sync
    async def test_update_family_meta(self):
        """
        Test updating a family's meta data.
        """
        family_layer = FamilyLayer(self.connection)

        # Create a family with initial meta
        family_id = await family_layer.create_family(
            external_ids={PRIMARY_EXTERNAL_ORG: 'update-meta-family'},
            description='Family for meta update test',
            coded_phenotype=None,
            meta={'initial_key': 'initial_value'},
        )

        # Update the family's meta (should merge)
        await family_layer.update_family(
            id_=family_id,
            meta={'new_key': 'new_value', 'initial_key': 'updated_value'},
        )

        # Query and verify the updated meta
        updated_family = await family_layer.get_family_by_internal_id(family_id)

        self.assertEqual(updated_family.meta['initial_key'], 'updated_value')
        self.assertEqual(updated_family.meta['new_key'], 'new_value')

    @run_as_sync
    async def test_create_family_without_meta(self):
        """
        Test creating a family without meta data defaults to empty dict.
        """
        family_layer = FamilyLayer(self.connection)

        # Create a family without meta
        family_id = await family_layer.create_family(
            external_ids={PRIMARY_EXTERNAL_ORG: 'no-meta-family'},
            description='Family without meta',
            coded_phenotype=None,
        )

        # Query and verify meta is empty dict
        created_family = await family_layer.get_family_by_internal_id(family_id)

        self.assertEqual(created_family.meta, {})
