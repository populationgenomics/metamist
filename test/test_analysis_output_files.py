# pylint: disable=invalid-overridden-method
import os
from test.testbase import DbIsolatedTest, run_as_sync

from testcontainers.core.container import DockerContainer

from db.python.layers.analysis import AnalysisLayer
from db.python.layers.assay import AssayLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from models.enums import AnalysisStatus
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AssayUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from models.models.analysis import AnalysisInternal


class TestOutputFiles(DbIsolatedTest):
    """Test sample class"""

    # pylint: disable=too-many-instance-attributes

    @run_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()

        absolute_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
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
        self.gcs = gcs

        self.sl = SampleLayer(self.connection)
        self.sgl = SequencingGroupLayer(self.connection)
        self.asl = AssayLayer(self.connection)
        self.al = AnalysisLayer(self.connection)

        sample = await self.sl.upsert_sample(
            SampleUpsertInternal(
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
                    SequencingGroupUpsertInternal(
                        type='exome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        sample_id=None,
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'sequencing_type': 'exome',
                                    'sequencing_technology': 'short-read',
                                    'sequencing_platform': 'illumina',
                                },
                            )
                        ],
                    ),
                ],
            )
        )
        self.sample_id = sample.id
        assert sample.sequencing_groups
        self.genome_sequencing_group_id = sample.sequencing_groups[0].id
        self.exome_sequencing_group_id = sample.sequencing_groups[self.project_id].id

    def tearDown(self) -> None:
        if self.gcs:
            self.gcs.stop()
        return super().tearDown()

    def check_outputs_fields(self, outputs: dict, output_file_data: dict):
        """Check the fields of the output file"""
        if outputs:
            self.assertEqual(outputs['path'], output_file_data['path'])
            self.assertEqual(outputs['basename'], output_file_data['basename'])
            self.assertEqual(outputs['dirname'], output_file_data['dirname'])
            self.assertEqual(outputs['nameroot'], output_file_data['nameroot'])
            self.assertEqual(outputs['nameext'], output_file_data['nameext'])
            self.assertEqual(
                outputs['file_checksum'], output_file_data['file_checksum']
            )
            self.assertEqual(outputs['size'], output_file_data['size'])
            self.assertEqual(outputs['valid'], output_file_data['valid'])
            self.assertEqual(
                len(outputs['secondary_files']),
                len(output_file_data['secondary_files']),
            )

    @run_as_sync
    async def test_output_str(self):
        """Test how the output(s) behave when you create an analysis by passing in
        just the `output` field
        """

        output_path = 'FAKE://this_file_doesnt_exist.txt'

        # Create the analysis first
        analysis_id = await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
                output=output_path,
            )
        )

        # Query the analysis object
        analysis = await self.al.get_analysis_by_id(analysis_id)
        assert analysis
        self.assertEqual(analysis.output, output_path)
        self.assertEqual(analysis.outputs, output_path)

    @run_as_sync
    async def test_gs_output_path(self):
        """Test how the output(s) behave when you create an analysis by passing in
        just the `output` field
        """
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
        analysis_id = await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
                output=output_path,
            )
        )

        # Query the analysis object
        analysis = await self.al.get_analysis_by_id(analysis_id)
        assert analysis

        assert isinstance(analysis.output, str)
        assert isinstance(analysis.outputs, dict)

        self.assertEqual(analysis.output, output_path)
        self.check_outputs_fields(analysis.outputs, output_file_data)

    @run_as_sync
    async def test_create_with_str_on_outputs(self):
        """This should test creating an Analysis by passing a string to the outputs field.
        The test should fail as we don't want to be passing string to this field.
        """

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

        analysis_id = await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
                outputs=output_path,
            )
        )

        analysis = await self.al.get_analysis_by_id(analysis_id)
        assert analysis

        assert isinstance(analysis.output, str)
        assert isinstance(analysis.outputs, dict)

        self.assertEqual(analysis.output, output_path)
        self.check_outputs_fields(analysis.outputs, output_file_data)

    @run_as_sync
    async def test_dict_with_outputs(self):
        """Should test creating Analysis object with a dictionary as the outputs field"""

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
                'parent_id': None,
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

        analysis_id = await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
                outputs=outputs,
            )
        )

        analysis = await self.al.get_analysis_by_id(analysis_id)
        assert analysis

        assert isinstance(analysis.output, str)
        assert isinstance(analysis.outputs, dict)

        self.assertEqual(analysis.output, '')
        self.assertIn('cram', analysis.outputs)
        self.assertIn('secondary_files', analysis.outputs['cram'])
        self.assertIn('meta', analysis.outputs['cram']['secondary_files'])
        self.assertIn('ext', analysis.outputs['cram']['secondary_files'])

        # Check for each field against the output file data and also check each secondary file
        self.check_outputs_fields(analysis.outputs['cram'], output_file_data['cram'])
        self.check_outputs_fields(
            analysis.outputs['cram']['secondary_files']['meta'],
            output_file_data['cram']['secondary_files']['meta'],  # type: ignore [index]
        )
        self.check_outputs_fields(
            analysis.outputs['cram']['secondary_files']['ext'],
            output_file_data['cram']['secondary_files']['ext'],  # type: ignore [index]
        )
