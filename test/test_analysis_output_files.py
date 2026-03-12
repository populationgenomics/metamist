import os
from collections.abc import Generator
from typing import Any

import pytest
from google.auth.credentials import AnonymousCredentials
from google.cloud.storage import Client
from testcontainers.core.container import DockerContainer

from db.python.connect import Connection
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.sample import SampleLayer
from db.python.tables.output_file import OutputFileTable
from db.python.tables.project import ProjectPermissionsTable
from models.enums import AnalysisStatus
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AssayUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from models.models.analysis import AnalysisInternal


def custom_get_gcs_client():
    """Create the custom client instance with the desired configuration"""
    return Client(
        credentials=AnonymousCredentials(),
        project='test',
        client_options={'api_endpoint': 'http://localhost:4443'},
    )


@pytest.fixture(autouse=True)
def fake_gcs(monkeypatch) -> Generator[DockerContainer]:
    """Provides a mocked Google Cloud storage bucket for testing"""
    monkeypatch.setattr(
        'models.models.output_file.get_gcs_client', custom_get_gcs_client
    )

    absolute_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')  # noqa: PTH100, PTH118, PTH120
    gcs = (
        DockerContainer('fsouza/fake-gcs-server')
        .with_bind_ports(4443, 4443)
        .with_volume_mapping(
            absolute_path,
            '/data',
        )
        .with_command('-scheme http')
    )
    gcs.start()

    yield gcs

    gcs.stop()


@pytest.fixture
async def fake_sequencing_group(connection_with_project: Connection) -> int:
    """
    Create a sequencing group for testing analysis output files.
    Required because analyses are associated with sequencing groups.
    """
    sample_layer = SampleLayer(connection_with_project)

    sample = await sample_layer.upsert_sample(
        SampleUpsertInternal(
            project=connection_with_project.project_id,
            external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
            type='blood',
            meta={'meta': 'meta ;)'},
            active=True,
            sequencing_groups=[
                SequencingGroupUpsertInternal(
                    type='genome',
                    technology='short-read',
                    platform='illumina',
                    meta={},
                    sample_id=None,
                    assays=[
                        AssayUpsertInternal(
                            type='sequencing',
                            meta={
                                'sequencing_type': 'genome',
                                'sequencing_technology': 'short-read',
                                'sequencing_platform': 'illumina',
                            },
                        )
                    ],
                ),
            ],
        )
    )

    assert sample.sequencing_groups
    return sample.sequencing_groups[0].id


def check_outputs_fields(outputs: dict, output_file_data: dict):
    """Check the fields of the output file"""
    if outputs:
        assert outputs['path'] == output_file_data['path']
        assert outputs['basename'] == output_file_data['basename']
        assert outputs['dirname'] == output_file_data['dirname']
        assert outputs['nameroot'] == output_file_data['nameroot']
        assert outputs['nameext'] == output_file_data['nameext']
        assert outputs['file_checksum'] == output_file_data['file_checksum']
        assert outputs['size'] == output_file_data['size']
        assert outputs['valid'] == output_file_data['valid']
        assert len(outputs['secondary_files']) == len(
            output_file_data['secondary_files']
        )


class TestOutputFiles:
    """Test sample class"""

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_output_str(
        self, connection_with_project: Connection, fake_sequencing_group: int
    ):
        """
        Test how the output(s) behave when you create an analysis by passing in
        just the `output` field
        """
        analysis_layer = AnalysisLayer(connection_with_project)

        output_path = 'FAKE://this_file_doesnt_exist.txt'

        # Create the analysis first
        analysis_id = await analysis_layer.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[fake_sequencing_group],
                meta={'sequencing_type': 'genome', 'size': 1024},
                output=output_path,
            )
        )

        # Query the analysis object
        analysis = await analysis_layer.get_analysis_by_id(analysis_id)
        assert analysis
        assert analysis.output == output_path
        assert analysis.outputs == output_path

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_gs_output_path(
        self, connection_with_project: Connection, fake_sequencing_group: int
    ):
        """
        Test how the output(s) behave when you create an analysis by passing in
        just the `output` field
        """
        analysis_layer = AnalysisLayer(connection_with_project)

        output_path = 'gs://fakegcs/file1.txt'
        output_file_data = {
            'path': 'gs://fakegcs/file1.txt',
            'basename': 'file1.txt',
            'dirname': 'gs://fakegcs/',
            'nameroot': 'file1',
            'nameext': '.txt',
            'file_checksum': 'DG+fhg==',
            'size': 19,
            'valid': True,
            'secondary_files': {},
        }
        # Create the analysis first
        analysis_id = await analysis_layer.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[fake_sequencing_group],
                meta={'sequencing_type': 'genome', 'size': 1024},
                output=output_path,
            )
        )

        # Query the analysis object
        analysis = await analysis_layer.get_analysis_by_id(analysis_id)
        assert analysis

        assert isinstance(analysis.output, str)
        assert isinstance(analysis.outputs, dict)

        assert analysis.output == output_path
        check_outputs_fields(analysis.outputs, output_file_data)

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_create_with_str_on_outputs(
        self, connection_with_project: Connection, fake_sequencing_group: int
    ):
        """
        This should test creating an Analysis by passing a string to the outputs field.
        The test should fail as we don't want to be passing string to this field.
        """
        analysis_layer = AnalysisLayer(connection_with_project)

        output_path = 'gs://fakegcs/file1.txt'
        output_file_data = {
            'path': 'gs://fakegcs/file1.txt',
            'basename': 'file1.txt',
            'dirname': 'gs://fakegcs/',
            'nameroot': 'file1',
            'nameext': '.txt',
            'file_checksum': 'DG+fhg==',
            'size': 19,
            'valid': True,
            'secondary_files': {},
        }

        analysis_id = await analysis_layer.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[fake_sequencing_group],
                meta={'sequencing_type': 'genome', 'size': 1024},
                outputs=output_path,
            )
        )

        analysis = await analysis_layer.get_analysis_by_id(analysis_id)
        assert analysis

        assert isinstance(analysis.output, str)
        assert isinstance(analysis.outputs, dict)

        assert analysis.output == output_path
        check_outputs_fields(analysis.outputs, output_file_data)

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_dict_with_outputs(
        self, connection_with_project: Connection, fake_sequencing_group: int
    ):
        """Should test creating Analysis object with a dictionary as the outputs field"""
        analysis_layer = AnalysisLayer(connection_with_project)

        outputs = {
            'cram': {
                'basename': 'gs://fakegcs/file2.cram',
                'secondary_files': {
                    'meta': {'basename': 'gs://fakegcs/file2.cram.meta'},
                    'ext': {'basename': 'gs://fakegcs/file2.cram.ext'},
                },
            },
        }

        output_file_data = {
            'cram': {
                'path': 'gs://fakegcs/file2.cram',
                'basename': 'file2.cram',
                'dirname': 'gs://fakegcs/',
                'nameroot': 'file2',
                'nameext': '.cram',
                'file_checksum': 'sl7SXw==',
                'size': 20,
                'meta': None,
                'valid': True,
                'secondary_files': {
                    'meta': {
                        'path': 'gs://fakegcs/file2.cram.meta',
                        'basename': 'file2.cram.meta',
                        'dirname': 'gs://fakegcs/',
                        'nameroot': 'file2.cram',
                        'nameext': '.meta',
                        'file_checksum': 'af/YSw==',
                        'size': 17,
                        'meta': None,
                        'valid': True,
                        'secondary_files': {},
                    },
                    'ext': {
                        'path': 'gs://fakegcs/file2.cram.ext',
                        'basename': 'file2.cram.ext',
                        'dirname': 'gs://fakegcs/',
                        'nameroot': 'file2.cram',
                        'nameext': '.ext',
                        'file_checksum': 'gb1EbA==',
                        'size': 21,
                        'meta': None,
                        'valid': True,
                        'secondary_files': {},
                    },
                },
            }
        }

        analysis_id = await analysis_layer.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[fake_sequencing_group],
                meta={'sequencing_type': 'genome', 'size': 1024},
                outputs=outputs,
            )
        )

        analysis = await analysis_layer.get_analysis_by_id(analysis_id)
        assert analysis

        assert isinstance(analysis.output, str)
        assert isinstance(analysis.outputs, dict)

        assert analysis.output == ''
        assert 'cram' in analysis.outputs
        assert 'secondary_files' in analysis.outputs['cram']
        assert 'meta' in analysis.outputs['cram']['secondary_files']
        assert 'ext' in analysis.outputs['cram']['secondary_files']

        # Check for each field against the output file data and also check each secondary file
        check_outputs_fields(analysis.outputs['cram'], output_file_data['cram'])
        check_outputs_fields(
            analysis.outputs['cram']['secondary_files']['meta'],
            output_file_data['cram']['secondary_files']['meta'],  # type: ignore [index]
        )
        check_outputs_fields(
            analysis.outputs['cram']['secondary_files']['ext'],
            output_file_data['cram']['secondary_files']['ext'],  # type: ignore [index]
        )

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_outputs_contains_protocol(
        self, connection_with_project: Connection, fake_sequencing_group: int
    ):
        """Tests validation of the outputs field so that file paths contain a protocol prefix"""
        analysis_layer = AnalysisLayer(connection_with_project)

        outputs_valid: dict[str, Any] = {
            'cram': {
                'filtered': {
                    'basename': 'gs://fakegcs/file2.cram',
                    'secondary_files': {
                        'meta': {'basename': 'gs://fakegcs/file2.cram.meta'},
                        'ext': {'basename': 'gs://fakegcs/file2.cram.ext'},
                    },
                },
            },
            'vcf': {
                'basename': 'gs://fakegcs/file3.vcf',
                'secondary_files': {
                    'meta': {'basename': 'gs://fakegcs/file3.vcf.meta'},
                    'ext': {'basename': 'gs://fakegcs/file3.vcf.ext'},
                },
            },
        }

        analysis_id = None
        # Check the base case of the validator passing a correctly formatted outputs field.
        try:
            analysis_id = await analysis_layer.create_analysis(
                AnalysisInternal(
                    type='cram',
                    status=AnalysisStatus.COMPLETED,
                    sequencing_group_ids=[fake_sequencing_group],
                    meta={'sequencing_type': 'genome', 'size': 1024},
                    outputs=outputs_valid,
                )
            )
        except ValueError as e:
            pytest.fail(f'Received ValueError: {e}')

        # Setup the invalid outputs.
        outputs_invalid = outputs_valid.copy()
        outputs_invalid['cram']['filtered']['secondary_files']['ext']['basename'] = (
            '://fakegcs/file2.cram.ext'
        )

        # Check the case of a file path being incorrectly formatted.
        with pytest.raises(ValueError):
            await analysis_layer.create_analysis(
                AnalysisInternal(
                    type='cram',
                    status=AnalysisStatus.COMPLETED,
                    sequencing_group_ids=[fake_sequencing_group],
                    meta={'sequencing_type': 'genome', 'size': 1024},
                    outputs=outputs_invalid,
                )
            )

        # Check the case of updating an Analysis with a file path being incorrectly formatted.
        with pytest.raises(ValueError):
            await analysis_layer.update_analysis(
                analysis_id=analysis_id, outputs=outputs_invalid
            )

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    @pytest.mark.project_name('project-test')
    async def test_project_deletion(
        self, connection_with_project: Connection, fake_sequencing_group: int
    ):
        """Test ProjectPermissionsTable.delete_project_data's effect on analysis outputs and files"""
        analysis_layer = AnalysisLayer(connection_with_project)

        outputs = {
            'cram': {
                'basename': 'gs://fakegcs/file3.cram',
                'secondary_files': {
                    'meta': {'basename': 'gs://fakegcs/file3.cram.meta'},
                    'ext': {'basename': 'gs://fakegcs/file3.cram.ext'},
                },
            },
        }

        await analysis_layer.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[fake_sequencing_group],
                meta={'sequencing_type': 'genome', 'size': 1024},
                outputs=outputs,
            )
        )

        # Helper to count rows in a table
        async def row_count(table: str) -> int:
            cur = await connection_with_project.pg_connection.execute(
                t'SELECT COUNT(*) as count FROM {table:i}'
            )
            row = await cur.fetchone()
            return row['count'] if row else 0

        assert (await row_count('analysis_outputs')) == 3
        assert (await row_count('output_file')) == 3

        proj_permission_table = ProjectPermissionsTable(connection_with_project)
        project = connection_with_project.project
        assert await proj_permission_table.delete_project_data(project)

        assert (await row_count('analysis_outputs')) == 0
        assert (await row_count('output_file')) == 0

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    @pytest.mark.project_name('project-test')
    async def test_ignore_duplicate_files(
        self, connection_with_project: Connection, fake_sequencing_group: int
    ):
        """Test that duplicate output files are ignored (not inserterd) when adding to an analysis"""
        analysis_layer = AnalysisLayer(connection_with_project)
        output_file_table = OutputFileTable(connection_with_project)

        outputs = {
            'cram': {
                'basename': 'gs://fakegcs/file3.cram',
                'secondary_files': {
                    'meta': {'basename': 'gs://fakegcs/file3.cram.meta'},
                    'ext': {'basename': 'gs://fakegcs/file3.cram.ext'},
                },
            },
        }

        # Create an analysis to add the baseline output files to the dictionary
        analysis_id = await analysis_layer.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[fake_sequencing_group],
                meta={'sequencing_type': 'genome', 'size': 1024},
                outputs=outputs,
            )
        )

        # Helper to view rows in a table
        async def all_rows(table: str) -> list[dict[str, Any]]:
            cur = await connection_with_project.pg_connection.execute(
                t'SELECT * FROM {table:i}'
            )
            rows = await cur.fetchall()
            return rows

        # Get all the output files in the database
        baseline_outputs = await all_rows('analysis_outputs')
        # The sys_period will have changed, but we aren't concerned with that
        for row in baseline_outputs:
            del row['sys_period']

        # Add the same files to the analysis to test that duplicates are ignored (not inserted)
        await output_file_table.create_or_update_analysis_output_files_from_output(
            analysis_id=analysis_id, json_dict=outputs
        )

        outputs_after_dupe = await all_rows('analysis_outputs')
        # The sys_period will have changed, but we aren't concerned with that
        for row in outputs_after_dupe:
            del row['sys_period']

        assert baseline_outputs == outputs_after_dupe

        # Add a different file to the analysis to test that a new file appears in the database
        outputs['basename'] = 'gs://fakegcs/file2.cram'
        await output_file_table.create_or_update_analysis_output_files_from_output(
            analysis_id=analysis_id, json_dict=outputs
        )

        outputs_after_new_file = await all_rows('analysis_outputs')
        # The sys_period will have changed, but we aren't concerned with that
        for row in outputs_after_new_file:
            del row['sys_period']

        assert len(baseline_outputs) != len(outputs_after_new_file)
