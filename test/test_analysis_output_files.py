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
