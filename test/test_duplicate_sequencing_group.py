import sys
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from cpg_utils import to_path

from scripts import duplicate_sequencing_group as dsg


SOURCE_SG = 'SG_SOURCE'
NEW_SG = 'SG_NEW'


def make_analysis(
    analysis_type: str = 'cram',
    outputs: dict | str | None = None,
    n_sgs: int = 1,
    stage: str | None = None,
    analysis_id: int = 1,
) -> dict:
    """Builds an analysis as returned by SG_DATA_QUERY"""
    return {
        'id': analysis_id,
        'type': analysis_type,
        'meta': {'stage': stage} if stage else {},
        'outputs': outputs
        if outputs is not None
        else {'path': f'gs://cpg-src-main/cram/{SOURCE_SG}.cram'},
        'sequencingGroups': [{'id': f'SG_{i}'} for i in range(n_sgs)],
    }


def patch_get_batch(batch: MagicMock):
    """Stands in for cpg_utils.hail_batch, which needs hail installed"""
    hail_batch = MagicMock(get_batch=MagicMock(return_value=batch))
    return patch.dict(sys.modules, {'cpg_utils.hail_batch': hail_batch})


def mock_path(path: str, exists: bool = False) -> MagicMock:
    """A mock cloud path that prints as the given path"""
    return MagicMock(**{'__str__.return_value': path, 'exists.return_value': exists})


def fake_storage_client(existing: list[str]) -> MagicMock:
    """A storage client whose buckets contain only the given gs:// paths"""
    client = MagicMock()

    def bucket(bucket_name, user_project=None):  # noqa: ARG001
        names = [
            path.removeprefix(f'gs://{bucket_name}/')
            for path in existing
            if path.startswith(f'gs://{bucket_name}/')
        ]
        fake_bucket = MagicMock()
        fake_bucket.bucket_name = bucket_name
        fake_bucket.names = names
        fake_bucket.blob.side_effect = lambda name: SimpleNamespace(
            exists=lambda: name in names
        )
        return fake_bucket

    client.bucket.side_effect = bucket
    client.list_blobs.side_effect = lambda fake_bucket, prefix: [
        SimpleNamespace(name=name)
        for name in fake_bucket.names
        if name.startswith(prefix)
    ]
    return client


class TestAnalysisSelection(unittest.TestCase):
    """Tests for choosing which analyses are copied"""

    def test_output_path_from_dict(self):
        """The primary path is read from an outputs dict"""
        analysis = make_analysis(outputs={'path': 'gs://a/b.cram', 'basename': 'b'})
        self.assertEqual(dsg.analysis_output_path(analysis), 'gs://a/b.cram')

    def test_output_path_from_string(self):
        """A plain string output is the path"""
        analysis = make_analysis(outputs='gs://a/b.cram')
        self.assertEqual(dsg.analysis_output_path(analysis), 'gs://a/b.cram')

    def test_output_path_missing(self):
        """Outputs without a path give None"""
        analysis = make_analysis(outputs={'nested': {'path': 'gs://a/b'}})
        self.assertIsNone(dsg.analysis_output_path(analysis))

    def test_single_sg_analysis_is_copied(self):
        """A single-SG analysis with a path is copied"""
        self.assertIsNone(dsg.analysis_skip_reason(make_analysis()))

    def test_cohort_analysis_is_skipped(self):
        """Analyses covering several SGs, e.g. index pages, are skipped"""
        reason = dsg.analysis_skip_reason(make_analysis('web', n_sgs=175))
        self.assertIn('175 sequencing groups', reason)

    def test_sv_only_gather_sample_evidence_is_copied(self):
        """Only the GatherSampleEvidence sv analyses are copied"""
        evidence = make_analysis('sv', stage='GatherSampleEvidence')
        annotate = make_analysis('sv', stage='AnnotateVcf')
        self.assertIsNone(dsg.analysis_skip_reason(evidence))
        self.assertIn('AnnotateVcf', dsg.analysis_skip_reason(annotate))

    def test_analysis_without_path_is_skipped(self):
        """Analyses with no primary output path are skipped"""
        analysis = make_analysis(outputs={})
        self.assertEqual(dsg.analysis_skip_reason(analysis), 'no recorded outputs path')

    def test_new_per_sg_types_are_included(self):
        """mito-cram, somalier and exomiser records are recreated"""
        for analysis_type in ['mito-cram', 'somalier', 'exomiser']:
            self.assertIn(analysis_type, dsg.ANALYSIS_TYPES)


class TestGetAnalysesToUpsert(unittest.IsolatedAsyncioTestCase):
    """Tests for building the new analyses and file moves"""

    async def test_maps_paths_and_skips_cohort_analyses(self):
        """Per-SG analyses are remapped, cohort-level ones are left behind"""
        cram = make_analysis(
            outputs={'path': f'gs://cpg-src-main/cram/{SOURCE_SG}.cram'},
            analysis_id=10,
        )
        index_page = make_analysis(
            'web',
            outputs={'path': 'gs://cpg-src-main-web/stripy/src_index.html'},
            n_sgs=175,
            analysis_id=11,
        )
        response = {'project': {'sequencingGroups': [{'analyses': [cram, index_page]}]}}
        with patch.object(dsg, 'query_async', AsyncMock(return_value=response)):
            analyses, moves = await dsg.get_analyses_to_upsert(
                'src', SOURCE_SG, 'dst', NEW_SG
            )

        self.assertEqual(len(analyses), 1)
        self.assertEqual(analyses[0]['output'], f'gs://cpg-dst-main/cram/{NEW_SG}.cram')
        self.assertEqual(analyses[0]['meta']['original_analysis_id'], 10)
        self.assertEqual(analyses[0]['meta']['dataset'], 'dst')
        self.assertEqual(
            [(str(src), str(dst)) for src, dst in moves],
            [
                (
                    f'gs://cpg-src-main/cram/{SOURCE_SG}.cram',
                    f'gs://cpg-dst-main/cram/{NEW_SG}.cram',
                )
            ],
        )


class TestGetSampleUpsert(unittest.IsolatedAsyncioTestCase):
    """Tests for the sample upsert that creates the new SG"""

    def sg_response(self) -> dict:
        """A SG_DATA_QUERY response for the original SG"""
        return {
            'project': {
                'sequencingGroups': [
                    {
                        'id': SOURCE_SG,
                        'type': 'genome',
                        'technology': 'short-read',
                        'platform': 'illumina',
                        'sample': {'type': 'blood'},
                        'assays': [{'id': 1, 'type': 'sequencing', 'meta': {}}],
                        'analyses': [],
                    }
                ]
            }
        }

    async def test_new_sample_copies_type(self):
        """A new sample takes the original sample's type"""
        with patch.object(
            dsg, 'query_async', AsyncMock(return_value=self.sg_response())
        ):
            upsert = await dsg.get_sample_upsert('src', SOURCE_SG, 'EXT-1', 5)
        self.assertEqual(upsert.type, 'blood')
        self.assertEqual(upsert.participant_id, 5)
        self.assertEqual(
            upsert.sequencing_groups[0].meta['original_sequencing_group_id'],
            SOURCE_SG,
        )

    async def test_existing_sample_type_unchanged(self):
        """An existing sample's type is not overwritten"""
        with patch.object(
            dsg, 'query_async', AsyncMock(return_value=self.sg_response())
        ):
            upsert = await dsg.get_sample_upsert(
                'src', SOURCE_SG, 'EXT-1', 5, sample_id='SAMPLE_ID_1'
            )
        self.assertIsNone(upsert.type)

    async def test_missing_sg_raises(self):
        """A missing or archived SG gives a clear error"""
        response: dict = {'project': {'sequencingGroups': []}}
        with (
            patch.object(dsg, 'query_async', AsyncMock(return_value=response)),
            self.assertRaisesRegex(ValueError, 'Only active sequencing groups'),
        ):
            await dsg.get_sample_upsert('src', SOURCE_SG, 'EXT-1')


class TestUnrecordedFiles(unittest.TestCase):
    """Tests for finding files not recorded in analyses"""

    def test_bucket_names(self):
        """Bucket names follow the access level and test datasets"""
        self.assertEqual(
            dsg.get_bucket_names('src', 'dst', 'full'),
            ('cpg-src-main', 'cpg-dst-main'),
        )
        self.assertEqual(
            dsg.get_bucket_names('src', 'dst-test', 'test'),
            ('cpg-src', 'cpg-dst-test'),
        )

    def test_resolve_existing_and_missing_file(self):
        """A plain path resolves to itself only if it exists"""
        client = fake_storage_client(['gs://cpg-src-main/qc/a.txt'])
        self.assertEqual(
            dsg.resolve_source_paths(client, 'gs://cpg-src-main/qc/a.txt', None),
            ['gs://cpg-src-main/qc/a.txt'],
        )
        self.assertEqual(
            dsg.resolve_source_paths(client, 'gs://cpg-src-main/qc/b.txt', None), []
        )

    def test_resolve_folder_in_its_own_bucket(self):
        """Folders are listed in the bucket named in the path, e.g. the -web bucket"""
        client = fake_storage_client(
            [
                'gs://cpg-src-main-web/mito/mitoreport-X/index.html',
                'gs://cpg-src-main-web/mito/mitoreport-X/js/app.js',
                'gs://cpg-src-main-web/mito/mitoreport-Y/index.html',
            ]
        )
        self.assertEqual(
            dsg.resolve_source_paths(
                client, 'gs://cpg-src-main-web/mito/mitoreport-X/', None
            ),
            [
                'gs://cpg-src-main-web/mito/mitoreport-X/index.html',
                'gs://cpg-src-main-web/mito/mitoreport-X/js/app.js',
            ],
        )

    def test_resolve_glob(self):
        """A '*' matches versioned directories"""
        client = fake_storage_client(
            [
                f'gs://cpg-src-main/stripy/1.0/{SOURCE_SG}.log',
                f'gs://cpg-src-main/stripy/1.1/{SOURCE_SG}.log',
                'gs://cpg-src-main/stripy/1.1/SG_OTHER.log',
            ]
        )
        self.assertEqual(
            dsg.resolve_source_paths(
                client, f'gs://cpg-src-main/stripy/*/{SOURCE_SG}.log', None
            ),
            [
                f'gs://cpg-src-main/stripy/1.0/{SOURCE_SG}.log',
                f'gs://cpg-src-main/stripy/1.1/{SOURCE_SG}.log',
            ],
        )

    def test_get_unrecorded_analysis_files(self):
        """Found files are mapped to the new buckets and SG ID"""
        client = fake_storage_client(
            [
                f'gs://cpg-src-main/qc/samtools_stats/{SOURCE_SG}.samtools-stats',
                f'gs://cpg-src-main-web/mito/mitoreport-{SOURCE_SG}/sr.png',
                f'gs://cpg-src-main-web/stripy/1.1/{SOURCE_SG}__default.html',
            ]
        )
        moves = dsg.get_unrecorded_analysis_files(
            'cpg-src-main', 'cpg-dst-main', SOURCE_SG, NEW_SG, None, client
        )
        self.assertCountEqual(
            [(str(src), str(dst)) for src, dst in moves],
            [
                (
                    f'gs://cpg-src-main/qc/samtools_stats/{SOURCE_SG}.samtools-stats',
                    f'gs://cpg-dst-main/qc/samtools_stats/{NEW_SG}.samtools-stats',
                ),
                (
                    f'gs://cpg-src-main-web/mito/mitoreport-{SOURCE_SG}/sr.png',
                    f'gs://cpg-dst-main-web/mito/mitoreport-{NEW_SG}/sr.png',
                ),
                (
                    f'gs://cpg-src-main-web/stripy/1.1/{SOURCE_SG}__default.html',
                    f'gs://cpg-dst-main-web/stripy/1.1/{NEW_SG}__default.html',
                ),
            ],
        )

    def test_dedupe_file_moves_keeps_first(self):
        """Files both recorded and unrecorded are only copied once"""
        recorded = (to_path('gs://b/x.cram'), to_path('gs://c/x.cram'))
        unrecorded = (to_path('gs://b/x.cram'), to_path('gs://d/x.cram'))
        other = (to_path('gs://b/y.txt'), to_path('gs://c/y.txt'))
        self.assertEqual(
            dsg.dedupe_file_moves([recorded, unrecorded, other]), [recorded, other]
        )

    def test_file_is_binary(self):
        """Images are copied as-is, text files are edited"""
        self.assertTrue(dsg.file_is_binary('gs://b/report/sr.PNG'))
        self.assertFalse(dsg.file_is_binary('gs://b/report/index.html'))


class TestReheaderAndUpsert(unittest.IsolatedAsyncioTestCase):
    """Tests for writing the copied files and analysis records"""

    async def test_binary_files_copied_without_batch(self):
        """Binary files are copied directly and an empty batch is not run"""
        old_path = mock_path('gs://cpg-src-main-web/mito/r/sr.png')
        new_path = mock_path('gs://cpg-dst-main-web/mito/r/sr.png')
        batch = MagicMock()
        with patch_get_batch(batch):
            await dsg.reheader_analysis_files_in_batch(
                SOURCE_SG, NEW_SG, [(old_path, new_path)]
            )
        old_path.copy.assert_called_once_with(new_path)
        batch.new_job.assert_not_called()
        batch.run.assert_not_called()

    async def test_upsert_waits_for_analysis_creation(self):
        """Analysis creation is awaited, so failures are raised"""
        api = MagicMock()
        api.create_analysis_async = AsyncMock(side_effect=RuntimeError('API down'))
        analysis = {
            'sg_id': NEW_SG,
            'type': 'cram',
            'output': f'gs://cpg-dst-main/cram/{NEW_SG}.cram',
            'meta': {'dataset': 'dst'},
        }
        with (
            patch.object(dsg, 'AnalysisApi', return_value=api),
            self.assertRaisesRegex(RuntimeError, 'API down'),
        ):
            await dsg.upsert_analyses_records('dst', [analysis], dry_run=False)


class TestValidateIds(unittest.TestCase):
    """Tests for matching the new participant and sample"""

    participants = [
        {
            'id': 7,
            'externalIds': {'': 'PARTICIPANT-1'},
            'samples': [{'id': 'SAMPLE_ID_1', 'externalIds': {'': 'SAMPLE-1'}}],
        }
    ]

    def test_existing_participant_new_sample(self):
        """A known participant without the sample gives no sample ID"""
        self.assertEqual(
            dsg.validate_ids('PARTICIPANT-1', None, 'NEW-SAMPLE', self.participants),
            (7, None),
        )

    def test_existing_participant_and_sample(self):
        """A known participant and sample external ID gives both internal IDs"""
        self.assertEqual(
            dsg.validate_ids('PARTICIPANT-1', None, 'SAMPLE-1', self.participants),
            (7, 'SAMPLE_ID_1'),
        )

    def test_unknown_participant_raises(self):
        """Participants are not created, so an unknown one raises"""
        with self.assertRaises(ValueError):
            dsg.validate_ids('MISSING', None, 'SAMPLE-1', self.participants)


class TestReheaderCommands(unittest.IsolatedAsyncioTestCase):
    """Tests for the commands used to replace the SG ID in each file type"""

    async def run_reheader(self, source: str) -> MagicMock:
        """Runs the reheader for one file and returns the batch job"""
        old_path = mock_path(source)
        new_path = mock_path(source.replace(SOURCE_SG, NEW_SG))
        batch = MagicMock()
        with (
            patch_get_batch(batch),
            patch.object(dsg, 'image_path', side_effect=lambda name: name),
            patch.object(dsg, 'config_retrieve', return_value='driver'),
        ):
            await dsg.reheader_analysis_files_in_batch(
                SOURCE_SG, NEW_SG, [(old_path, new_path)]
            )
        batch.run.assert_called_once()
        return batch.new_job.return_value

    def commands(self, job: MagicMock) -> str:
        """All commands added to a job"""
        return '\n'.join(c.args[0] for c in job.command.call_args_list)

    async def test_cram(self):
        """CRAM read groups are renamed and the index regenerated"""
        job = await self.run_reheader(f'gs://b/cram/{SOURCE_SG}.cram')
        commands = self.commands(job)
        self.assertIn(f'/^@RG/ s/{SOURCE_SG}/{NEW_SG}/g', commands)
        self.assertIn('samtools index', commands)
        job.image.assert_called_once_with('samtools')

    async def test_gvcf(self):
        """VCF sample columns are renamed and the index regenerated"""
        job = await self.run_reheader(f'gs://b/gvcf/{SOURCE_SG}.g.vcf.gz')
        commands = self.commands(job)
        self.assertIn(f'/^#CHROM/ s/{SOURCE_SG}/{NEW_SG}/', commands)
        self.assertIn('tabix', commands)
        job.image.assert_called_once_with('bcftools')

    async def test_sv_evidence(self):
        """SV evidence files have the SG ID column overwritten"""
        job = await self.run_reheader(f'gs://b/sv_evidence/{SOURCE_SG}.sd.txt.gz')
        commands = self.commands(job)
        self.assertIn('-v col=3', commands)
        self.assertIn('tabix -0 -s1 -b2 -e2', commands)

    async def test_generic_gzip(self):
        """Other gzipped files are edited with sed"""
        job = await self.run_reheader(
            f'gs://b/sv_evidence/{SOURCE_SG}.coverage_counts.tsv.gz'
        )
        self.assertIn(f's/{SOURCE_SG}/{NEW_SG}/g', self.commands(job))

    async def test_text(self):
        """Text files are edited with sed on the driver image"""
        job = await self.run_reheader(f'gs://b-web/stripy/{SOURCE_SG}.stripy.html')
        self.assertIn(f's/{SOURCE_SG}/{NEW_SG}/g', self.commands(job))
        job.image.assert_called_once_with('driver')


class TestSomalier(unittest.TestCase):
    """Tests for rewriting the sample ID in binary somalier files"""

    def somalier_bytes(self, sample_id: str) -> bytes:
        """A minimal somalier file: version, ID length, ID, then site counts"""
        encoded = sample_id.encode()
        return b'\x02' + bytes([len(encoded)]) + encoded + b'\x00\x01\x02'

    def test_rewrites_sample_id(self):
        """The ID and its length are replaced and the rest kept"""
        files = {'old': MagicMock(), 'new': MagicMock()}
        files['old'].read_bytes.return_value = self.somalier_bytes(SOURCE_SG)
        with patch.object(dsg, 'to_path', side_effect=files.get):
            dsg.somalier_file_commands('old', 'new', (SOURCE_SG, 'SG_LONGER_ID'))
        files['new'].write_bytes.assert_called_once_with(
            self.somalier_bytes('SG_LONGER_ID')
        )

    def test_wrong_sample_id_raises(self):
        """A file for another sample is not rewritten"""
        files = {'old': MagicMock(), 'new': MagicMock()}
        files['old'].read_bytes.return_value = self.somalier_bytes('SG_OTHER')
        with (
            patch.object(dsg, 'to_path', side_effect=files.get),
            self.assertRaisesRegex(ValueError, 'Unexpected sample ID'),
        ):
            dsg.somalier_file_commands('old', 'new', (SOURCE_SG, NEW_SG))
        files['new'].write_bytes.assert_not_called()


class TestMain(unittest.IsolatedAsyncioTestCase):
    """Tests for the end to end flow, with Metamist and GCS mocked"""

    recorded = [(to_path('gs://cpg-src-main/a'), to_path('gs://cpg-dst-main/a'))]
    unrecorded = [
        (to_path('gs://cpg-src-main/a'), to_path('gs://cpg-dst-main/a')),
        (to_path('gs://cpg-src-main/b'), to_path('gs://cpg-dst-main/b')),
    ]

    async def run_main(self, **kwargs) -> dict[str, MagicMock]:
        """Runs main with everything external mocked, returning the mocks"""
        sample_api = MagicMock()
        sample_api.upsert_samples_async = AsyncMock(
            return_value=[{'sequencing_groups': [{'id': NEW_SG}]}]
        )
        mocks = {
            'get_participant_sample': AsyncMock(return_value=(7, None)),
            'get_sample_upsert': AsyncMock(),
            'SampleApi': MagicMock(return_value=sample_api),
            'get_analyses_to_upsert': AsyncMock(
                return_value=(['analysis'], self.recorded)
            ),
            'get_unrecorded_analysis_files': MagicMock(return_value=self.unrecorded),
            'config_retrieve': MagicMock(side_effect=['full', 'billing']),
            'reheader_analysis_files_in_batch': AsyncMock(),
            'log_file_updates': MagicMock(),
            'upsert_analyses_records': AsyncMock(),
            'SequencingGroupApi': MagicMock(),
        }
        with ExitStack() as stack:
            for name, mock in mocks.items():
                stack.enter_context(patch.object(dsg, name, mock))
            await dsg.main(
                SOURCE_SG,
                'src',
                'dst',
                'PARTICIPANT-1',
                new_participant_id='PARTICIPANT-1',
                **kwargs,
            )
        return mocks

    async def test_main_creates_sg_copies_files_and_registers_analyses(self):
        """A new SG is created, files deduplicated and copied, analyses created"""
        mocks = await self.run_main()

        mocks['get_sample_upsert'].assert_awaited_once_with(
            'src', SOURCE_SG, 'PARTICIPANT-1', 7, None
        )
        mocks['get_unrecorded_analysis_files'].assert_called_once_with(
            'cpg-src-main', 'cpg-dst-main', SOURCE_SG, NEW_SG, billing_project='billing'
        )
        reheader = mocks['reheader_analysis_files_in_batch']
        self.assertEqual(
            reheader.await_args.kwargs['source_paths_new_paths'],
            [self.recorded[0], self.unrecorded[1]],
        )
        mocks['log_file_updates'].assert_not_called()
        mocks['upsert_analyses_records'].assert_awaited_once_with(
            dataset='dst', analyses_to_upsert=['analysis'], dry_run=False
        )
        mocks['SequencingGroupApi'].assert_not_called()

    async def test_dry_run_writes_nothing(self):
        """A dry run logs the plan but creates, copies and archives nothing"""
        mocks = await self.run_main(dry_run=True, deactivate_original_sg=True)

        mocks['SampleApi'].assert_not_called()
        mocks['reheader_analysis_files_in_batch'].assert_not_awaited()
        mocks['SequencingGroupApi'].assert_not_called()
        mocks['get_unrecorded_analysis_files'].assert_called_once_with(
            'cpg-src-main',
            'cpg-dst-main',
            SOURCE_SG,
            dsg.DRY_RUN_SG_ID,
            billing_project='billing',
        )
        mocks['log_file_updates'].assert_called_once_with(
            SOURCE_SG, dsg.DRY_RUN_SG_ID, [self.recorded[0], self.unrecorded[1]]
        )
        mocks['upsert_analyses_records'].assert_awaited_once_with(
            dataset='dst', analyses_to_upsert=['analysis'], dry_run=True
        )


class TestDryRunFileUpdates(unittest.TestCase):
    """Tests for logging the file updates in a dry run"""

    def test_file_update_method(self):
        """Each file type maps to the update the real run would make"""
        self.assertEqual(dsg.file_update_method('gs://b/r/sr.png'), 'binary copy')
        self.assertEqual(
            dsg.file_update_method('gs://b/x.cram.somalier'), 'somalier rewrite'
        )
        self.assertEqual(dsg.file_update_method('gs://b/x.cram'), 'reheader')
        self.assertEqual(dsg.file_update_method('gs://b/x.sd.txt.gz'), 'bgzip edit')
        self.assertEqual(dsg.file_update_method('gs://b/x.html'), 'text edit')

    def test_log_file_updates_writes_nothing(self):
        """Files are sized and somalier headers checked, but nothing is written"""
        cram = (mock_path('gs://b/x.cram'), mock_path('gs://c/x.cram'))
        existing = (mock_path('gs://b/y.html'), mock_path('gs://c/y.html', exists=True))
        somalier = (mock_path('gs://b/x.somalier'), mock_path('gs://c/x.somalier'))
        with (
            patch.object(dsg, 'file_size', return_value=1024**3) as file_size,
            patch.object(dsg, 'somalier_file_commands') as somalier_commands,
            self.assertLogs(dsg.logger, level='INFO') as logs,
        ):
            dsg.log_file_updates(SOURCE_SG, NEW_SG, [cram, existing, somalier])

        self.assertEqual(file_size.call_count, 2)
        somalier_commands.assert_called_once_with(
            'gs://b/x.somalier', 'gs://c/x.somalier', (SOURCE_SG, NEW_SG), dry_run=True
        )
        self.assertIn('Would skip existing file: gs://c/y.html', logs.output[1])
        self.assertIn('2.0 GiB would be copied', logs.output[-1])
        for path in [*cram, *somalier]:
            path.copy.assert_not_called()

    def test_somalier_dry_run_checks_without_writing(self):
        """The somalier header is checked but no file is written"""
        files = {'old': MagicMock(), 'new': MagicMock()}
        encoded = SOURCE_SG.encode()
        files['old'].read_bytes.return_value = (
            b'\x02' + bytes([len(encoded)]) + encoded + b'\x00'
        )
        with patch.object(dsg, 'to_path', side_effect=files.get):
            dsg.somalier_file_commands('old', 'new', (SOURCE_SG, NEW_SG), dry_run=True)
        files['new'].write_bytes.assert_not_called()


if __name__ == '__main__':
    unittest.main()
