from tempfile import TemporaryFile

import pytest
from fastapi import HTTPException, UploadFile

from api.routes import family
from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers.family import FamilyLayer
from models.models import PRIMARY_EXTERNAL_ORG


class TestFamilyImportEndpoint:
    """Family testing methods"""

    @pytest.mark.asyncio
    async def test_import_families_empty_file(
        self, connection_with_project: Connection
    ):
        """
        Test importing families from a file where the file is empty.
        """
        with TemporaryFile(prefix='test', suffix='.tsv') as f:
            emptyTestFile = UploadFile(f)  # noqa: N806

            # Test has_header = true
            with pytest.raises(HTTPException) as context:
                _ = await family.import_families(
                    emptyTestFile,
                    has_header=True,
                    delimiter='\t',
                    connection=connection_with_project,
                )
            assert context.value.status_code == 400
            assert 'A header was expected but file is empty.' in context.value.detail

            # Test no op when has_header = false
            response = await family.import_families(
                emptyTestFile,
                has_header=False,
                delimiter='\t',
                connection=connection_with_project,
            )
            assert response == {
                'success': True,
                'warnings': ['Submitted file was empty'],
            }

    @pytest.mark.asyncio
    async def test_import_families_header_no_content(
        self, connection_with_project: Connection
    ):
        """
        Test importing families from a file with a header but no data.
        """
        with TemporaryFile(mode='wb+', prefix='test', suffix='.tsv') as f:
            f.write(b'Some\ttest\theader\twithout\tdata\n')
            f.seek(0)
            headerOnlyFile = UploadFile(f)  # noqa: N806

            # Test has_header = true
            response = await family.import_families(
                headerOnlyFile,
                has_header=True,
                delimiter='\t',
                connection=connection_with_project,
            )
            assert response == {
                'success': True,
                'warnings': ['Submitted file contained a header with no data'],
            }

    @pytest.mark.asyncio
    async def test_import_families_valid_data(
        self, connection_with_project: Connection
    ):
        """
        Test importing families with valid file contents.
        """
        data = [
            ['familyid', 'description', 'phenotype'],
            ['Smith', 'Blacksmiths', 'burnt'],
            ['Jones', 'From Wales', 'sings well'],
            ['Taylor', 'Post Norman', 'sews'],
        ]
        fileContent = '\n'.join(['\t'.join(row) for row in data]).encode(  # noqa: N806
            encoding='utf-8-sig'
        )

        with TemporaryFile(mode='wb+', prefix='test', suffix='.tsv') as f:
            f.write(fileContent)
            f.seek(0)
            testFile = UploadFile(f)  # noqa: N806

            # Test has_header = true
            response = await family.import_families(
                testFile,
                has_header=True,
                delimiter='\t',
                connection=connection_with_project,
            )
            assert response == {'success': True}

            f.seek(0)
            # Test has_header = false
            response = await family.import_families(
                testFile,
                has_header=False,
                delimiter='\t',
                connection=connection_with_project,
            )
            assert response == {'success': True}

    @pytest.mark.asyncio
    async def test_import_families_valid_data_and_meta(
        self, connection_with_project: Connection
    ):
        """
        Test importing families with valid file contents.
        """
        data = [
            ['familyid', 'description', 'phenotype', 'meta'],
            ['Smith', 'Blacksmiths', 'burnt', '{"key1": "value1"}'],
            ['Jones', 'From Wales', 'sings well', ''],
            ['Taylor', 'Post Norman', 'sews', '{"key3": "value3"}'],
        ]
        fileContent = '\n'.join(['\t'.join(row) for row in data]).encode(  # noqa: N806
            encoding='utf-8-sig'
        )

        with TemporaryFile(mode='wb+', prefix='test', suffix='.tsv') as f:
            f.write(fileContent)
            f.seek(0)
            testFile = UploadFile(f)  # noqa: N806

            # Test has_header = true
            response = await family.import_families(
                testFile,
                has_header=True,
                delimiter='\t',
                connection=connection_with_project,
            )
            assert response == {'success': True}

            f.seek(0)
            # Test has_header = false
            response = await family.import_families(
                testFile,
                has_header=False,
                delimiter='\t',
                connection=connection_with_project,
            )
            assert response == {'success': True}

            # get family and verify that meta was imported correctly
            family_layer = FamilyLayer(connection_with_project)

            family_list = await family_layer.query(
                family.FamilyFilter(
                    external_id=GenericFilter(eq='Smith'),
                    project=GenericFilter(eq=connection_with_project.project_id),
                )
            )
            assert len(family_list) == 1
            fam = family_list[0]
            assert fam.meta == {'key1': 'value1'}

    @pytest.mark.asyncio
    async def test_import_families_fails_on_invalid_meta(
        self, connection_with_project: Connection
    ):
        """
        Test importing families with valid file contents.
        """
        data = [
            ['familyid', 'description', 'phenotype', 'meta'],
            ['Smith', 'Blacksmiths', 'burnt', '{"key1": "...'],
        ]
        fileContent = '\n'.join(['\t'.join(row) for row in data]).encode(  # noqa: N806
            encoding='utf-8-sig'
        )

        with TemporaryFile(mode='wb+', prefix='test', suffix='.tsv') as f:
            f.write(fileContent)
            f.seek(0)
            testFile = UploadFile(f)  # noqa: N806

            # expect to raise ValueError due to invalid JSON in meta
            with pytest.raises(ValueError):
                await family.import_families(
                    testFile,
                    has_header=True,
                    delimiter='\t',
                    connection=connection_with_project,
                )

    @pytest.mark.asyncio
    async def test_create_family_with_meta(self, connection_with_project: Connection):
        """
        Test creating a family with meta
        """
        family_layer = FamilyLayer(connection_with_project)

        # Create a family with meta
        family_id = await family_layer.create_family(
            external_ids={PRIMARY_EXTERNAL_ORG: 'test-family'},
            description='Test family',
            coded_phenotype='test-phenotype',
            meta={'key1': 'value1', 'nested': {'key2': 'value2'}},
        )

        assert family_id is not None

        # Query the family and verify meta
        created_family = await family_layer.get_family_by_internal_id(family_id)

        assert created_family.id == family_id
        assert created_family.external_ids[PRIMARY_EXTERNAL_ORG] == 'test-family'
        assert created_family.description == 'Test family'
        assert created_family.coded_phenotype == 'test-phenotype'
        assert created_family.meta['key1'] == 'value1'
        assert created_family.meta['nested']['key2'] == 'value2'

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_update_family_meta(self, connection_with_project: Connection):
        """
        Test updating a family's meta data.
        """
        family_layer = FamilyLayer(connection_with_project)

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

        assert updated_family.meta['initial_key'] == 'updated_value'
        assert updated_family.meta['new_key'] == 'new_value'

    @pytest.mark.asyncio
    async def test_create_family_without_meta(
        self, connection_with_project: Connection
    ):
        """
        Test creating a family without meta data defaults to empty dict.
        """
        family_layer = FamilyLayer(connection_with_project)

        # Create a family without meta
        family_id = await family_layer.create_family(
            external_ids={PRIMARY_EXTERNAL_ORG: 'no-meta-family'},
            description='Family without meta',
            coded_phenotype=None,
        )

        # Query and verify meta is empty dict
        created_family = await family_layer.get_family_by_internal_id(family_id)

        assert created_family.meta == {}
