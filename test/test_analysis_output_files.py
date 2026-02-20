import os
from collections.abc import Generator
from typing import Any

import pytest
from testcontainers.core.container import DockerContainer

from db.python.connect import Connection
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.sample import SampleLayer
from db.python.tables.project import ProjectPermissionsTable
from models.enums import AnalysisStatus
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AssayUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from models.models.analysis import AnalysisInternal


@pytest.fixture(autouse=True)
def fake_gcs() -> Generator[DockerContainer]:
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
        except ValueError:
            self.fail()

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

        assert (await self.row_count('analysis_outputs')) == 3
        assert (await self.row_count('output_file')) == 3

        pttable = ProjectPermissionsTable(self.connection)
        project = self.project_id_map[self.project_id]
        assert await pttable.delete_project_data(project)

        assert (await self.row_count('analysis_outputs')) == 0
        assert (await self.row_count('output_file')) == 0
