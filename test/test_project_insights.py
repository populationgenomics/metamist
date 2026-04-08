from datetime import datetime, timezone

from db.python.layers import (
    AnalysisLayer,
    AssayLayer,
    FamilyLayer,
    ParticipantLayer,
    ProjectInsightsLayer,
    SampleLayer,
)
from models.enums import AnalysisStatus
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AnalysisInternal,
    AssayUpsertInternal,
    ParticipantUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from test.testbase import DbIsolatedTest, run_as_sync


default_assay_meta = {
    'sequencing_type': 'genome',
    'sequencing_technology': 'short-read',
    'sequencing_platform': 'illumina',
}


def make_sample(
    ext_id: str,
    sg_type: str = 'genome',
    sg_technology: str = 'short-read',
    sg_platform: str = 'illumina',
    sample_type: str = 'blood',
) -> SampleUpsertInternal:
    """Build a sample with one sequencing group and one assay."""
    return SampleUpsertInternal(
        external_ids={PRIMARY_EXTERNAL_ORG: ext_id},
        meta={},
        type=sample_type,
        sequencing_groups=[
            SequencingGroupUpsertInternal(
                type=sg_type,
                technology=sg_technology,
                platform=sg_platform,
                assays=[
                    AssayUpsertInternal(
                        type='sequencing',
                        meta={
                            'sequencing_type': sg_type,
                            'sequencing_technology': sg_technology,
                            'sequencing_platform': sg_platform,
                        },
                    ),
                ],
            )
        ],
    )


def make_participant(
    ext_id: str,
    samples: list[SampleUpsertInternal],
) -> ParticipantUpsertInternal:
    """Build a participant upsert with the given samples."""
    return ParticipantUpsertInternal(
        external_ids={PRIMARY_EXTERNAL_ORG: ext_id},
        meta={},
        samples=samples,
    )


def get_test_participant():
    """Do it like this to avoid an upsert writing the test value"""
    return ParticipantUpsertInternal(
        external_ids={PRIMARY_EXTERNAL_ORG: 'Demeter'},
        meta={},
        samples=[
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'sample_id001'},
                meta={},
                type='blood',
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'reads': [
                                        {
                                            'basename': 'sample_id001.filename-R1.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id001.filename-R1.fastq.gz',
                                            'size': 111,
                                        },
                                        {
                                            'basename': 'sample_id001.filename-R2.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id001.filename-R2.fastq.gz',
                                            'size': 111,
                                        },
                                    ],
                                    'reads_type': 'fastq',
                                    'batch': 'M001',
                                    **default_assay_meta,
                                },
                            ),
                        ],
                    )
                ],
            )
        ],
    )


class TestProjectInsights(DbIsolatedTest):
    """Test project insights class containing project insights endpoints"""

    maxDiff = None

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()
        self.partl = ParticipantLayer(self.connection)
        self.pil = ProjectInsightsLayer(self.connection)
        self.sampl = SampleLayer(self.connection)
        self.seql = AssayLayer(self.connection)
        self.al = AnalysisLayer(self.connection)
        self.fl = FamilyLayer(self.connection)

    # --- Helper to create a family with participants and SGs ---

    async def create_family_with_participants(
        self,
        family_ext_id: str,
        members: list[dict],
    ) -> dict[str, int]:
        """
        Create participants with samples first, then import pedigree to link them.
        members: list of dicts with keys: ext_id, paternal_id, maternal_id, sex, affected
        Returns dict mapping participant ext_id -> sequencing_group internal ID.
        """
        # 1. Create participants with samples/SGs first
        sg_ids = {}
        for m in members:
            sample_ext = f'sample_{m["ext_id"]}'
            sg_type = m.get('sg_type', 'genome')
            sg_tech = m.get('sg_technology', 'short-read')
            sg_platform = m.get('sg_platform', 'illumina')
            p = await self.partl.upsert_participant(
                make_participant(
                    m['ext_id'],
                    [
                        make_sample(
                            sample_ext,
                            sg_type=sg_type,
                            sg_technology=sg_tech,
                            sg_platform=sg_platform,
                        )
                    ],
                )
            )
            sg_ids[m['ext_id']] = p.samples[0].sequencing_groups[0].id

        # 2. Import pedigree to create family and link participants
        rows = []
        for m in members:
            rows.append(
                [
                    family_ext_id,
                    m['ext_id'],
                    m.get('paternal_id', ''),
                    m.get('maternal_id', ''),
                    str(m.get('sex', '0')),
                    str(m.get('affected', '0')),
                ]
            )
        await self.fl.import_pedigree(
            header=None, rows=rows, create_missing_participants=False
        )

        return sg_ids

    async def create_analysis_with_output(
        self,
        analysis: AnalysisInternal,
        output: str,
    ) -> int:
        """
        Create an analysis and set the output column directly via SQL.
        This avoids the GCS client interaction that happens when passing
        output through the normal AnalysisInternal path.
        """
        analysis_id = await self.al.create_analysis(analysis)
        await self.connection.connection.execute(
            'UPDATE analysis SET output = :output WHERE id = :id',
            {'output': output, 'id': analysis_id},
        )
        return analysis_id

    # --- Existing tests (kept as-is) ---

    @run_as_sync
    async def test_project_insights_summary(self):
        """Test getting the summaries for all available projects"""

        await self.partl.upsert_participant(get_test_participant())

        result = await self.pil.get_project_insights_summary(
            project_names=[self.project_name], sequencing_types=['genome', 'exome']
        )

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(row.project, self.project_id)
        self.assertEqual(row.dataset, self.project_name)
        self.assertEqual(row.sequencing_type, 'genome')
        self.assertEqual(row.sequencing_technology, 'short-read')
        self.assertEqual(row.total_families, 0)
        self.assertEqual(row.total_participants, 1)
        self.assertEqual(row.total_samples, 1)
        self.assertEqual(row.total_sequencing_groups, 1)
        self.assertEqual(row.total_crams, 0)
        self.assertIsNone(row.latest_annotate_dataset)
        self.assertIsNone(row.latest_snv_es_index)
        self.assertIsNone(row.latest_sv_es_index)
        self.assertEqual(row.family_ids, [])
        self.assertEqual(len(row.participant_ids), 1)
        self.assertEqual(len(row.sample_ids), 1)

    @run_as_sync
    async def test_project_insights_details(self):
        """Test getting the details for all available projects"""

        await self.partl.upsert_participant(get_test_participant())

        # There's not enough data set up to usefully verify the result
        _ = await self.pil.get_project_insights_details(
            project_names=[self.project_name], sequencing_types=['genome', 'exome']
        )

    # --- Phase 2: Summary with rich data ---

    @run_as_sync
    async def test_summary_with_families_and_crams(self):
        """Summary with a family of 3, CRAMs for 2 of 3 SGs"""
        sg_ids = await self.create_family_with_participants(
            'FAM01',
            [
                {'ext_id': 'FATHER', 'sex': '1', 'affected': '1'},
                {'ext_id': 'MOTHER', 'sex': '2', 'affected': '1'},
                {
                    'ext_id': 'PROBAND',
                    'paternal_id': 'FATHER',
                    'maternal_id': 'MOTHER',
                    'sex': '1',
                    'affected': '2',
                },
            ],
        )

        # Create CRAMs for father and proband only
        for ext_id in ['FATHER', 'PROBAND']:
            await self.al.create_analysis(
                AnalysisInternal(
                    type='cram',
                    status=AnalysisStatus.COMPLETED,
                    sequencing_group_ids=[sg_ids[ext_id]],
                    meta={'sequencing_type': 'genome'},
                )
            )

        result = await self.pil.get_project_insights_summary(
            project_names=[self.project_name], sequencing_types=['genome']
        )

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(row.total_families, 1)
        self.assertEqual(row.total_participants, 3)
        self.assertEqual(row.total_samples, 3)
        self.assertEqual(row.total_sequencing_groups, 3)
        self.assertEqual(row.total_crams, 2)
        self.assertEqual(len(row.family_ids), 1)
        self.assertEqual(len(row.participant_ids), 3)
        self.assertEqual(len(row.sample_ids), 3)

    @run_as_sync
    async def test_summary_with_annotate_dataset_and_es_indices(self):
        """Summary correctly picks the latest analyses and counts SGs"""
        # Create 2 participants with genome/short-read SGs (no family needed for summary)
        p1 = await self.partl.upsert_participant(
            make_participant('P1', [make_sample('sample_p1')])
        )
        p2 = await self.partl.upsert_participant(
            make_participant('P2', [make_sample('sample_p2')])
        )
        sg1 = p1.samples[0].sequencing_groups[0].id
        sg2 = p2.samples[0].sequencing_groups[0].id

        # Older AnnotateDataset covering only sg1
        await self.al.create_analysis(
            AnalysisInternal(
                type='CUSTOM',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg1],
                meta={'stage': 'AnnotateDataset', 'sequencing_type': 'genome'},
                timestamp_completed=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )
        )
        # Newer AnnotateDataset covering sg1 + sg2
        await self.al.create_analysis(
            AnalysisInternal(
                type='CUSTOM',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg1, sg2],
                meta={'stage': 'AnnotateDataset', 'sequencing_type': 'genome'},
                timestamp_completed=datetime(2024, 6, 1, tzinfo=timezone.utc),
            )
        )

        # Older SNV ES-index covering only sg1
        await self.al.create_analysis(
            AnalysisInternal(
                type='es-index',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg1],
                meta={'stage': 'MtToEs', 'sequencing_type': 'genome'},
                timestamp_completed=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )
        )
        # Newer SNV ES-index covering sg1 + sg2
        await self.al.create_analysis(
            AnalysisInternal(
                type='es-index',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg1, sg2],
                meta={'stage': 'MtToEs', 'sequencing_type': 'genome'},
                timestamp_completed=datetime(2024, 6, 1, tzinfo=timezone.utc),
            )
        )

        # SV ES-index covering sg1 only
        await self.al.create_analysis(
            AnalysisInternal(
                type='es-index',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg1],
                meta={'stage': 'MtToEsSv', 'sequencing_type': 'genome'},
                timestamp_completed=datetime(2024, 6, 1, tzinfo=timezone.utc),
            )
        )

        result = await self.pil.get_project_insights_summary(
            project_names=[self.project_name], sequencing_types=['genome']
        )

        self.assertEqual(len(result), 1)
        row = result[0]

        # Latest AnnotateDataset should be the newer one with 2 SGs
        self.assertIsNotNone(row.latest_annotate_dataset)
        self.assertEqual(row.latest_annotate_dataset.sg_count, 2)

        # Latest SNV ES-index should be the newer one with 2 SGs
        self.assertIsNotNone(row.latest_snv_es_index)
        self.assertEqual(row.latest_snv_es_index.sg_count, 2)

        # SV ES-index has only 1 SG
        self.assertIsNotNone(row.latest_sv_es_index)
        self.assertEqual(row.latest_sv_es_index.sg_count, 1)

    @run_as_sync
    async def test_summary_non_short_read_excludes_analyses(self):
        """Non-short-read technology should have counts but no analysis stats"""
        p = await self.partl.upsert_participant(
            make_participant(
                'P1', [make_sample('sample_p1', sg_technology='long-read')]
            )
        )
        sg_id = p.samples[0].sequencing_groups[0].id

        # Create analyses that would match for short-read
        await self.al.create_analysis(
            AnalysisInternal(
                type='CUSTOM',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg_id],
                meta={'stage': 'AnnotateDataset', 'sequencing_type': 'genome'},
            )
        )
        await self.al.create_analysis(
            AnalysisInternal(
                type='es-index',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg_id],
                meta={'stage': 'MtToEs', 'sequencing_type': 'genome'},
            )
        )

        result = await self.pil.get_project_insights_summary(
            project_names=[self.project_name], sequencing_types=['genome']
        )

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(row.sequencing_technology, 'long-read')
        self.assertEqual(row.total_participants, 1)
        self.assertEqual(row.total_sequencing_groups, 1)
        # All analysis fields should be None for non-short-read
        self.assertIsNone(row.latest_annotate_dataset)
        self.assertIsNone(row.latest_snv_es_index)
        self.assertIsNone(row.latest_sv_es_index)

    @run_as_sync
    async def test_summary_exome_sv_uses_cnv_stage(self):
        """Exome SV ES-index should use MtToEsCNV stage from SV_INDEX_SEQ_TYPE_STAGE_MAP"""
        p = await self.partl.upsert_participant(
            make_participant('P1', [make_sample('sample_p1', sg_type='exome')])
        )
        sg_id = p.samples[0].sequencing_groups[0].id

        # Create exome-specific SV/gCNV ES-index
        await self.al.create_analysis(
            AnalysisInternal(
                type='es-index',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg_id],
                meta={'stage': 'MtToEsCNV', 'sequencing_type': 'exome'},
            )
        )

        result = await self.pil.get_project_insights_summary(
            project_names=[self.project_name], sequencing_types=['exome']
        )

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(row.sequencing_type, 'exome')
        self.assertIsNotNone(row.latest_sv_es_index)
        self.assertEqual(row.latest_sv_es_index.sg_count, 1)

    @run_as_sync
    async def test_summary_multiple_seq_types_returns_separate_rows(self):
        """One participant with genome and exome SGs returns 2 summary rows"""
        await self.partl.upsert_participant(
            make_participant(
                'P1',
                [
                    make_sample('sample_genome', sg_type='genome'),
                    make_sample('sample_exome', sg_type='exome'),
                ],
            )
        )

        result = await self.pil.get_project_insights_summary(
            project_names=[self.project_name], sequencing_types=['genome', 'exome']
        )

        self.assertEqual(len(result), 2)
        types = {r.sequencing_type for r in result}
        self.assertEqual(types, {'genome', 'exome'})
        for row in result:
            self.assertEqual(row.total_participants, 1)
            self.assertEqual(row.total_samples, 1)
            self.assertEqual(row.total_sequencing_groups, 1)

    @run_as_sync
    async def test_summary_multiple_families_counted_correctly(self):
        """Two families with participants should return total_families=2"""
        await self.create_family_with_participants(
            'FAM01',
            [{'ext_id': 'FAM1_P1', 'sex': '1', 'affected': '1'}],
        )
        await self.create_family_with_participants(
            'FAM02',
            [{'ext_id': 'FAM2_P1', 'sex': '2', 'affected': '1'}],
        )

        result = await self.pil.get_project_insights_summary(
            project_names=[self.project_name], sequencing_types=['genome']
        )

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(row.total_families, 2)
        self.assertEqual(row.total_participants, 2)

    # --- Phase 3: Details with assertions ---

    @run_as_sync
    async def test_details_basic_with_family_and_cram(self):
        """Details returns per-SG rows with CRAM data for SGs in families"""
        sg_ids = await self.create_family_with_participants(
            'FAM01',
            [
                {'ext_id': 'FATHER', 'sex': '1', 'affected': '1'},
                {'ext_id': 'MOTHER', 'sex': '2', 'affected': '1'},
            ],
        )

        # Create CRAM for FATHER's SG only
        await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg_ids['FATHER']],
                meta={'sequencing_type': 'genome'},
            )
        )

        result = await self.pil.get_project_insights_details(
            project_names=[self.project_name], sequencing_types=['genome']
        )

        self.assertEqual(len(result), 2)

        father_row = [r for r in result if r.participant_ext_id == 'FATHER'][0]
        self.assertEqual(father_row.family_ext_id, 'FAM01')
        self.assertEqual(father_row.sequencing_type, 'genome')
        self.assertEqual(father_row.sequencing_technology, 'short-read')
        self.assertIsNotNone(father_row.cram['id'])
        self.assertIsNotNone(father_row.cram['timestamp_completed'])

        mother_row = [r for r in result if r.participant_ext_id == 'MOTHER'][0]
        self.assertIsNone(mother_row.cram['id'])

    @run_as_sync
    async def test_details_analysis_boolean_flags(self):
        """Boolean in_latest_* flags correctly reflect per-SG analysis membership"""
        sg_ids = await self.create_family_with_participants(
            'FAM01',
            [
                {'ext_id': 'P1', 'sex': '1', 'affected': '1'},
                {'ext_id': 'P2', 'sex': '2', 'affected': '1'},
            ],
        )
        sg1 = sg_ids['P1']
        sg2 = sg_ids['P2']

        # AnnotateDataset includes only SG1
        await self.al.create_analysis(
            AnalysisInternal(
                type='CUSTOM',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg1],
                meta={'stage': 'AnnotateDataset', 'sequencing_type': 'genome'},
            )
        )
        # SNV ES-index includes both
        await self.al.create_analysis(
            AnalysisInternal(
                type='es-index',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg1, sg2],
                meta={'stage': 'MtToEs', 'sequencing_type': 'genome'},
            )
        )
        # SV ES-index includes only SG2
        await self.al.create_analysis(
            AnalysisInternal(
                type='es-index',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg2],
                meta={'stage': 'MtToEsSv', 'sequencing_type': 'genome'},
            )
        )

        result = await self.pil.get_project_insights_details(
            project_names=[self.project_name], sequencing_types=['genome']
        )

        self.assertEqual(len(result), 2)

        p1_row = [r for r in result if r.participant_ext_id == 'P1'][0]
        self.assertTrue(p1_row.in_latest_annotate_dataset)
        self.assertTrue(p1_row.in_latest_snv_es_index)
        self.assertFalse(p1_row.in_latest_sv_es_index)

        p2_row = [r for r in result if r.participant_ext_id == 'P2'][0]
        self.assertFalse(p2_row.in_latest_annotate_dataset)
        self.assertTrue(p2_row.in_latest_snv_es_index)
        self.assertTrue(p2_row.in_latest_sv_es_index)

    @run_as_sync
    async def test_details_excludes_participants_without_families(self):
        """Details only returns SGs belonging to participants in families"""
        # Participant in a family
        await self.create_family_with_participants(
            'FAM01',
            [{'ext_id': 'IN_FAMILY', 'sex': '1', 'affected': '1'}],
        )

        # Participant NOT in a family
        await self.partl.upsert_participant(
            make_participant('NO_FAMILY', [make_sample('sample_no_family')])
        )

        result = await self.pil.get_project_insights_details(
            project_names=[self.project_name], sequencing_types=['genome']
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].participant_ext_id, 'IN_FAMILY')

    # --- Phase 4: Web reports ---

    @run_as_sync
    async def test_details_stripy_report_with_outliers(self):
        """Stripy web report includes URL, outlier detection, and outlier loci"""
        sg_ids = await self.create_family_with_participants(
            'FAM01',
            [{'ext_id': 'P1', 'sex': '1', 'affected': '2'}],
        )
        sg_id = sg_ids['P1']

        await self.create_analysis_with_output(
            AnalysisInternal(
                type='web',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg_id],
                meta={
                    'stage': 'Stripy',
                    'outliers_detected': True,
                    'outlier_loci': ['ATXN1', 'HTT'],
                },
            ),
            output='gs://cpg-test-main-web/stripy/report.html',
        )

        result = await self.pil.get_project_insights_details(
            project_names=[self.project_name], sequencing_types=['genome']
        )

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertIn('stripy', row.web_reports)
        stripy = row.web_reports['stripy']
        self.assertIn('main-web', stripy['url'])
        self.assertIn('/stripy/', stripy['url'])
        self.assertTrue(stripy['outliers_detected'])
        self.assertEqual(stripy['outlier_loci'], ['ATXN1', 'HTT'])
        self.assertIsNotNone(stripy['timestamp_completed'])

    @run_as_sync
    async def test_details_mito_report(self):
        """MitoReport web report includes correct URL pattern"""
        sg_ids = await self.create_family_with_participants(
            'FAM01',
            [{'ext_id': 'P1', 'sex': '1', 'affected': '2'}],
        )
        sg_id = sg_ids['P1']

        await self.create_analysis_with_output(
            AnalysisInternal(
                type='web',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg_id],
                meta={'stage': 'MitoReport'},
            ),
            output='gs://cpg-test-main-web/mito/report.html',
        )

        result = await self.pil.get_project_insights_details(
            project_names=[self.project_name], sequencing_types=['genome']
        )

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertIn('mito', row.web_reports)
        mito = row.web_reports['mito']
        self.assertIn('/mito/mitoreport-', mito['url'])
        self.assertIn('/index.html', mito['url'])
        self.assertIsNotNone(mito['timestamp_completed'])

    @run_as_sync
    async def test_details_test_web_url_vs_main_web_url(self):
        """Report URL base differs based on output path containing main-web vs test-web"""
        sg_ids = await self.create_family_with_participants(
            'FAM01',
            [
                {'ext_id': 'P_MAIN', 'sex': '1', 'affected': '1'},
                {'ext_id': 'P_TEST', 'sex': '2', 'affected': '1'},
            ],
        )

        # Main-web output for P_MAIN's SG
        await self.create_analysis_with_output(
            AnalysisInternal(
                type='web',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg_ids['P_MAIN']],
                meta={'stage': 'Stripy'},
            ),
            output='gs://cpg-project-main-web/stripy/report.html',
        )
        # Test-web output for P_TEST's SG
        await self.create_analysis_with_output(
            AnalysisInternal(
                type='web',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sg_ids['P_TEST']],
                meta={'stage': 'Stripy'},
            ),
            output='gs://cpg-project-test-web/stripy/report.html',
        )

        result = await self.pil.get_project_insights_details(
            project_names=[self.project_name], sequencing_types=['genome']
        )

        main_row = [r for r in result if r.participant_ext_id == 'P_MAIN'][0]
        test_row = [r for r in result if r.participant_ext_id == 'P_TEST'][0]

        self.assertIn('https://main-web', main_row.web_reports['stripy']['url'])
        self.assertIn('https://test-web', test_row.web_reports['stripy']['url'])
