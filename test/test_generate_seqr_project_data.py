import asyncio
import random
import re
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from metamist.graphql import configure_sync_client, validate
from metamist.model.analysis import Analysis

from api.graphql.schema import schema  # type: ignore
from test.data.generate_seqr_project_data import (
    NAMES,
    PROJECTS,
    QUERY_ENUMS,
    QUERY_PROJECT_ID,
    QUERY_PROJECT_SGS,
    SEQ_PLATFORMS,
    SEQ_TECHS,
    SEQ_TYPES,
    generate_cohorts,
    generate_cram_analyses,
    generate_pedigree_rows,
    generate_project_pedigree,
    generate_qc_analyses,
    generate_random_id,
    generate_random_number_within_distribution,
    generate_sample_entries,
    generate_seq_platform,
    generate_seq_technology,
    generate_seqr_loader_analyses,
    generate_sequencing_type,
    generate_web_report_analyses,
    main,
    ped_row,
)


class ValidateSeqrGenerateDataQueries(unittest.TestCase):
    """Validate that the GraphQL queries in generate_seqr_project_data are valid against the schema."""

    def test_validate_queries(self):
        client = configure_sync_client(schema=schema.as_str(), auth_token='FAKE')
        validate(QUERY_PROJECT_ID, client=client)
        validate(QUERY_PROJECT_SGS, client=client)
        validate(QUERY_ENUMS, client=client)


class TestPedRow(unittest.TestCase):
    """Phase 1a: Tests for the ped_row class."""

    def test_construction_from_list(self):
        values = ['FAM1', 'IND1', 'PAT1', 'MAT1', 1, 2]
        row = ped_row(values)
        self.assertEqual(row.family_id, 'FAM1')
        self.assertEqual(row.individual_id, 'IND1')
        self.assertEqual(row.paternal_id, 'PAT1')
        self.assertEqual(row.maternal_id, 'MAT1')
        self.assertEqual(row.sex, 1)
        self.assertEqual(row.affected, 2)

    def test_iter_yields_correct_order(self):
        values = ['FAM1', 'IND1', 'PAT1', 'MAT1', 1, 2]
        row = ped_row(values)
        self.assertEqual(list(row), values)

    def test_field_access(self):
        values = ['FAM_X', 'IND_Y', '', '', 0, 1]
        row = ped_row(values)
        self.assertEqual(row.family_id, 'FAM_X')
        self.assertEqual(row.individual_id, 'IND_Y')
        self.assertEqual(row.paternal_id, '')
        self.assertEqual(row.maternal_id, '')
        self.assertEqual(row.sex, 0)
        self.assertEqual(row.affected, 1)


class TestGenerateRandomId(unittest.TestCase):
    """Phase 1b: Tests for generate_random_id."""

    def test_returns_matching_pattern(self):
        random.seed(42)
        used_ids: set[str] = set()
        rid = generate_random_id(used_ids)
        # Pattern: NAME_NNNN where NAME is from NAMES and NNNN is zero-padded 4 digits
        pattern = re.compile(r'^[A-Z]+_\d{4}$')
        self.assertRegex(rid, pattern)
        name_part = rid.split('_')[0]
        self.assertIn(name_part, NAMES)

    def test_adds_to_used_ids(self):
        random.seed(42)
        used_ids: set[str] = set()
        rid = generate_random_id(used_ids)
        self.assertIn(rid, used_ids)

    def test_never_returns_duplicate(self):
        random.seed(42)
        used_ids: set[str] = set()
        ids = [generate_random_id(used_ids) for _ in range(50)]
        self.assertEqual(len(ids), len(set(ids)))


class TestGeneratePedigreeRows(unittest.TestCase):
    """Phase 1c: Tests for generate_pedigree_rows."""

    def test_zero_families_returns_empty(self):
        result = generate_pedigree_rows(num_families=0)
        self.assertEqual(result, [])

    def test_one_family_returns_at_least_one_row(self):
        random.seed(42)
        result = generate_pedigree_rows(num_families=1)
        self.assertGreaterEqual(len(result), 1)

    def test_all_rows_are_ped_row_instances(self):
        random.seed(42)
        result = generate_pedigree_rows(num_families=5)
        for row in result:
            self.assertIsInstance(row, ped_row)

    def test_all_individual_ids_unique(self):
        random.seed(42)
        result = generate_pedigree_rows(num_families=10)
        individual_ids = [row.individual_id for row in result]
        self.assertEqual(len(individual_ids), len(set(individual_ids)))

    def test_singleton_family(self):
        """Force a singleton family (1 individual) and check properties."""
        with patch(
            'test.data.generate_seqr_project_data.random.randint',
            side_effect=lambda a, b: 1 if (a, b) == (1, 5) else random.Random(42).randint(a, b),
        ):
            result = generate_pedigree_rows(num_families=1)
            self.assertEqual(len(result), 1)
            row = result[0]
            self.assertEqual(row.paternal_id, '')
            self.assertEqual(row.maternal_id, '')
            self.assertEqual(row.affected, 2)

    def test_duo_family(self):
        """Force a duo family (2 individuals) and check parent-child relationship."""
        rng = random.Random(42)
        with patch(
            'test.data.generate_seqr_project_data.random.randint',
            side_effect=lambda a, b: 2 if (a, b) == (1, 5) else rng.randint(a, b),
        ), patch(
            'test.data.generate_seqr_project_data.random.choice',
            side_effect=rng.choice,
        ), patch(
            'test.data.generate_seqr_project_data.random.choices',
            side_effect=lambda pop, weights=None, k=1: rng.choices(pop, weights=weights, k=k),
        ):
            result = generate_pedigree_rows(num_families=1)
            self.assertEqual(len(result), 2)
            parent = result[0]
            child = result[1]
            # Parent has no parents
            self.assertEqual(parent.paternal_id, '')
            self.assertEqual(parent.maternal_id, '')
            # Child references parent based on parent sex
            if parent.sex == 1:
                self.assertEqual(child.paternal_id, parent.individual_id)
                self.assertEqual(child.maternal_id, '')
            else:
                self.assertEqual(child.paternal_id, '')
                self.assertEqual(child.maternal_id, parent.individual_id)
            self.assertEqual(child.affected, 2)

    def test_trio_plus_family(self):
        """Force a trio+ family (3-5 individuals) and check founders."""
        rng = random.Random(42)
        with patch(
            'test.data.generate_seqr_project_data.random.randint',
            side_effect=lambda a, b: 4 if (a, b) == (1, 5) else rng.randint(a, b),
        ), patch(
            'test.data.generate_seqr_project_data.random.choice',
            side_effect=rng.choice,
        ), patch(
            'test.data.generate_seqr_project_data.random.choices',
            side_effect=lambda pop, weights=None, k=1: rng.choices(pop, weights=weights, k=k),
        ):
            result = generate_pedigree_rows(num_families=1)
            self.assertEqual(len(result), 4)
            # First two are founders
            founder1 = result[0]
            founder2 = result[1]
            self.assertEqual(founder1.sex, 1)
            self.assertEqual(founder2.sex, 2)
            self.assertEqual(founder1.paternal_id, '')
            self.assertEqual(founder1.maternal_id, '')
            self.assertEqual(founder2.paternal_id, '')
            self.assertEqual(founder2.maternal_id, '')
            # Children reference founders
            for child in result[2:]:
                if child.paternal_id:
                    self.assertEqual(
                        child.paternal_id, founder1.individual_id
                    )
                if child.maternal_id:
                    self.assertEqual(
                        child.maternal_id, founder2.individual_id
                    )


class TestSequencingGenerators(unittest.TestCase):
    """Phase 1d-g: Tests for sequencing type/platform/technology generators."""

    # Phase 1d: generate_sequencing_type
    def test_generate_sequencing_type_returns_list(self):
        random.seed(42)
        dist = {1: 0.8, 2: 0.15, 3: 0.05}
        result = generate_sequencing_type(dist, SEQ_TYPES)
        self.assertIsInstance(result, list)

    def test_generate_sequencing_type_length_from_distribution(self):
        random.seed(42)
        dist = {1: 0.8, 2: 0.15, 3: 0.05}
        result = generate_sequencing_type(dist, SEQ_TYPES)
        self.assertIn(len(result), dist.keys())

    def test_generate_sequencing_type_values_from_seq_types(self):
        random.seed(42)
        dist = {1: 0.8, 2: 0.15, 3: 0.05}
        result = generate_sequencing_type(dist, SEQ_TYPES)
        for val in result:
            self.assertIn(val, SEQ_TYPES)

    # Phase 1e: generate_seq_platform
    def test_long_read_always_pacbio(self):
        for seed in range(20):
            random.seed(seed)
            result = generate_seq_platform(SEQ_PLATFORMS, 'long-read')
            self.assertEqual(result, 'pacbio')

    def test_short_read_returns_platform(self):
        random.seed(42)
        result = generate_seq_platform(SEQ_PLATFORMS, 'short-read')
        self.assertIn(result, SEQ_PLATFORMS)

    # Phase 1f: generate_seq_technology
    def test_genome_returns_short_or_long_read(self):
        results = set()
        for seed in range(200):
            random.seed(seed)
            results.add(generate_seq_technology(SEQ_TECHS, 'genome'))
        self.assertTrue(results.issubset({'short-read', 'long-read'}))

    def test_exome_always_short_read(self):
        for seed in range(20):
            random.seed(seed)
            result = generate_seq_technology(SEQ_TECHS, 'exome')
            self.assertEqual(result, 'short-read')

    def test_transcriptome_always_short_read(self):
        for seed in range(20):
            random.seed(seed)
            result = generate_seq_technology(SEQ_TECHS, 'transcriptome')
            self.assertEqual(result, 'short-read')

    # Phase 1g: generate_random_number_within_distribution
    def test_returns_key_from_distribution(self):
        random.seed(42)
        dist = {1: 0.5, 2: 0.3, 3: 0.2}
        result = generate_random_number_within_distribution(dist)
        self.assertIn(result, dist.keys())

    def test_respects_weighting(self):
        random.seed(42)
        dist = {10: 0.99, 20: 0.01}
        results = [
            generate_random_number_within_distribution(dist) for _ in range(100)
        ]
        # With 99% weight, 10 should appear far more often
        self.assertGreater(results.count(10), results.count(20))


class TestGenerateCramAnalyses(unittest.TestCase):
    """Phase 3a: Tests for generate_cram_analyses."""

    def _make_sg(self, sg_id, sg_type, technology, platform='illumina'):
        return {
            'id': sg_id,
            'type': sg_type,
            'technology': technology,
            'platform': platform,
        }

    def test_cram_analyses_appended(self):
        sgs = [
            self._make_sg('SG001', 'genome', 'short-read'),
            self._make_sg('SG002', 'exome', 'short-read'),
            self._make_sg('SG003', 'transcriptome', 'short-read'),
            self._make_sg('SG004', 'genome', 'long-read', 'pacbio'),
        ]
        mock_response = {'project': {'sequencingGroups': sgs}}

        analyses: list[Analysis] = []

        with patch(
            'test.data.generate_seqr_project_data.query_async',
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            # Force all SGs to be selected as aligned
            random.seed(42)
            result = asyncio.run(
                generate_cram_analyses('TESTPROJ', 1, analyses)
            )

        # All SGs returned as aligned (random.sample with k >= len/2)
        self.assertGreater(len(result), 0)
        self.assertGreater(len(analyses), 0)

        # Analyses should correspond to the aligned (returned) SGs
        aligned_sg_ids = {sg['id'] for sg in result}
        for a in analyses:
            self.assertEqual(a.type, 'cram')
            self.assertEqual(str(a.status), 'completed')
            self.assertTrue(
                set(a.sequencing_group_ids).issubset(aligned_sg_ids),
                f'Analysis SG IDs {a.sequencing_group_ids} not in aligned SGs {aligned_sg_ids}',
            )

    def _run_cram_with_all_aligned(self, sgs):
        """Helper: run generate_cram_analyses forcing all SGs to be aligned."""
        mock_response = {'project': {'sequencingGroups': sgs}}
        analyses: list[Analysis] = []

        with patch(
            'test.data.generate_seqr_project_data.query_async',
            new_callable=AsyncMock,
            return_value=mock_response,
        ), patch(
            'test.data.generate_seqr_project_data.random.sample',
            side_effect=lambda population, k: population,  # noqa: ARG005
        ):
            random.seed(42)
            result = asyncio.run(
                generate_cram_analyses('PROJ', 1, analyses)
            )
        return analyses, result

    def test_cram_path_short_read_genome(self):
        sgs = [self._make_sg('SG001', 'genome', 'short-read')]
        analyses, _ = self._run_cram_with_all_aligned(sgs)
        self.assertEqual(len(analyses), 1)
        self.assertEqual(analyses[0].output, 'FAKE://PROJ/cram/SG001.cram')

    def test_cram_path_short_read_exome(self):
        sgs = [self._make_sg('SG001', 'exome', 'short-read')]
        analyses, _ = self._run_cram_with_all_aligned(sgs)
        self.assertEqual(len(analyses), 1)
        self.assertEqual(
            analyses[0].output, 'FAKE://PROJ/exome/cram/SG001.cram'
        )

    def test_cram_path_short_read_transcriptome(self):
        sgs = [self._make_sg('SG001', 'transcriptome', 'short-read')]
        analyses, _ = self._run_cram_with_all_aligned(sgs)
        self.assertEqual(len(analyses), 1)
        self.assertEqual(
            analyses[0].output, 'FAKE://PROJ/transcriptome/cram/SG001.cram'
        )

    def test_cram_path_long_read(self):
        sgs = [self._make_sg('SG001', 'genome', 'long-read', 'pacbio')]
        analyses, _ = self._run_cram_with_all_aligned(sgs)
        self.assertEqual(len(analyses), 1)
        self.assertEqual(
            analyses[0].output, 'FAKE://PROJ/long_read/SG001.cram'
        )

    def test_cram_path_unknown_technology_fallback(self):
        sgs = [self._make_sg('SG001', 'genome', 'unknown-tech')]
        analyses, _ = self._run_cram_with_all_aligned(sgs)
        self.assertEqual(len(analyses), 1)
        self.assertEqual(
            analyses[0].output, 'FAKE://PROJ/crams/SG001.cram'
        )

    def test_cram_meta_fields(self):
        sgs = [self._make_sg('SG001', 'genome', 'short-read', 'illumina')]
        analyses, _ = self._run_cram_with_all_aligned(sgs)
        meta = analyses[0].meta
        self.assertEqual(meta['sequencing_type'], 'genome')
        self.assertEqual(meta['sequencing_technology'], 'short-read')
        self.assertEqual(meta['sequencing_platform'], 'illumina')
        self.assertIn('size', meta)

    def test_returns_subset_of_sgs(self):
        sgs = [
            self._make_sg(f'SG{i:03}', 'genome', 'short-read')
            for i in range(10)
        ]
        mock_response = {'project': {'sequencingGroups': sgs}}
        analyses: list[Analysis] = []

        with patch(
            'test.data.generate_seqr_project_data.query_async',
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            random.seed(42)
            result = asyncio.run(
                generate_cram_analyses('PROJ', 1, analyses)
            )

        # Returns between len/2 and len SGs
        self.assertGreaterEqual(len(result), len(sgs) // 2)
        self.assertLessEqual(len(result), len(sgs))
        # All returned SGs should be from the original list
        result_ids = {sg['id'] for sg in result}
        original_ids = {sg['id'] for sg in sgs}
        self.assertTrue(result_ids.issubset(original_ids))


class TestGenerateQcAnalyses(unittest.TestCase):
    """Phase 3b: Tests for generate_qc_analyses."""

    def test_long_read_skipped(self):
        cohort_ids = {
            ('genome', 'long-read'): 'COH001',
        }
        analyses: list[Analysis] = []
        asyncio.run(
            generate_qc_analyses('PROJ', cohort_ids, analyses)
        )
        self.assertEqual(len(analyses), 0)

    def test_exome_produces_qc_and_web(self):
        cohort_ids = {
            ('exome', 'short-read'): 'COH001',
        }
        analyses: list[Analysis] = []
        asyncio.run(
            generate_qc_analyses('PROJ', cohort_ids, analyses)
        )
        self.assertEqual(len(analyses), 2)
        types = [(a.type, a.meta.get('stage')) for a in analyses]
        self.assertIn(('qc', 'CramMultiQC'), types)
        self.assertIn(('web', 'SomalierPedigree'), types)

    def test_genome_produces_qc_and_web(self):
        cohort_ids = {
            ('genome', 'short-read'): 'COH001',
        }
        analyses: list[Analysis] = []
        asyncio.run(
            generate_qc_analyses('PROJ', cohort_ids, analyses)
        )
        self.assertEqual(len(analyses), 2)
        types = [(a.type, a.meta.get('stage')) for a in analyses]
        self.assertIn(('qc', 'CramMultiQC'), types)
        self.assertIn(('web', 'SomalierPedigree'), types)

    def test_transcriptome_skipped(self):
        cohort_ids = {
            ('transcriptome', 'short-read'): 'COH001',
        }
        analyses: list[Analysis] = []
        asyncio.run(
            generate_qc_analyses('PROJ', cohort_ids, analyses)
        )
        self.assertEqual(len(analyses), 0)

    def test_mixed_cohorts(self):
        cohort_ids = {
            ('genome', 'short-read'): 'COH001',
            ('exome', 'short-read'): 'COH002',
            ('genome', 'long-read'): 'COH003',
            ('transcriptome', 'short-read'): 'COH004',
        }
        analyses: list[Analysis] = []
        asyncio.run(
            generate_qc_analyses('PROJ', cohort_ids, analyses)
        )
        # genome short-read: 2, exome short-read: 2, long-read: 0, transcriptome: 0
        self.assertEqual(len(analyses), 4)


class TestGenerateSeqrLoaderAnalyses(unittest.TestCase):
    """Phase 3c: Tests for generate_seqr_loader_analyses."""

    def test_long_read_skipped(self):
        cohort_ids = {
            ('genome', 'long-read'): 'COH001',
        }
        analyses: list[Analysis] = []
        asyncio.run(
            generate_seqr_loader_analyses('PROJ', cohort_ids, analyses)
        )
        self.assertEqual(len(analyses), 0)

    def test_genome_produces_matrixtable_and_es_index_and_sv(self):
        cohort_ids = {
            ('genome', 'short-read'): 'COH001',
        }
        analyses: list[Analysis] = []
        asyncio.run(
            generate_seqr_loader_analyses('PROJ', cohort_ids, analyses)
        )
        # matrixtable (AnnotateDataset) + es-index (MtToEs) + es-index (MtToEsSv)
        self.assertEqual(len(analyses), 3)
        types_stages = [(a.type, a.meta.get('stage')) for a in analyses]
        self.assertIn(('matrixtable', 'AnnotateDataset'), types_stages)
        self.assertIn(('es-index', 'MtToEs'), types_stages)
        self.assertIn(('es-index', 'MtToEsSv'), types_stages)

    def test_exome_produces_matrixtable_and_es_index_and_gcnv(self):
        cohort_ids = {
            ('exome', 'short-read'): 'COH001',
        }
        analyses: list[Analysis] = []
        asyncio.run(
            generate_seqr_loader_analyses('PROJ', cohort_ids, analyses)
        )
        # matrixtable (AnnotateDataset) + es-index (MtToEs) + es-index (MtToEsCNV)
        self.assertEqual(len(analyses), 3)
        types_stages = [(a.type, a.meta.get('stage')) for a in analyses]
        self.assertIn(('matrixtable', 'AnnotateDataset'), types_stages)
        self.assertIn(('es-index', 'MtToEs'), types_stages)
        self.assertIn(('es-index', 'MtToEsCNV'), types_stages)

    def test_transcriptome_skipped(self):
        cohort_ids = {
            ('transcriptome', 'short-read'): 'COH001',
        }
        analyses: list[Analysis] = []
        asyncio.run(
            generate_seqr_loader_analyses('PROJ', cohort_ids, analyses)
        )
        self.assertEqual(len(analyses), 0)


class TestGenerateWebReportAnalyses(unittest.TestCase):
    """Phase 3d: Tests for generate_web_report_analyses."""

    def _make_sg(self, sg_id, sg_type='genome', technology='short-read', platform='illumina'):
        return {
            'id': sg_id,
            'type': sg_type,
            'technology': technology,
            'platform': platform,
        }

    def test_each_sg_gets_stripy_and_mito(self):
        random.seed(42)
        sgs = [self._make_sg('SG001'), self._make_sg('SG002')]
        analyses: list[Analysis] = []
        asyncio.run(
            generate_web_report_analyses('PROJ', 1, sgs, analyses)
        )
        # 2 SGs * 2 analyses each + 1 STRipy index = 5
        self.assertEqual(len(analyses), 5)

        # Check per-SG analyses
        for sg in sgs:
            sg_analyses = [
                a
                for a in analyses
                if a.sequencing_group_ids == [sg['id']]
            ]
            stages = [a.meta.get('stage') for a in sg_analyses]
            self.assertIn('Stripy', stages)
            self.assertIn('MitoReport', stages)

    def test_stripy_index_references_all_sg_ids(self):
        random.seed(42)
        sgs = [self._make_sg('SG001'), self._make_sg('SG002'), self._make_sg('SG003')]
        analyses: list[Analysis] = []
        asyncio.run(
            generate_web_report_analyses('PROJ', 1, sgs, analyses)
        )
        # Last analysis is the STRipy index
        index_analysis = analyses[-1]
        self.assertEqual(index_analysis.meta.get('stage'), 'MakeIndexPage')
        expected_ids = [sg['id'] for sg in sgs]
        self.assertEqual(index_analysis.sequencing_group_ids, expected_ids)

    def test_stripy_meta_contains_outlier_keys(self):
        random.seed(42)
        sgs = [self._make_sg('SG001')]
        analyses: list[Analysis] = []
        asyncio.run(
            generate_web_report_analyses('PROJ', 1, sgs, analyses)
        )
        stripy_analyses = [
            a for a in analyses if a.meta.get('stage') == 'Stripy'
        ]
        self.assertGreater(len(stripy_analyses), 0)
        for a in stripy_analyses:
            self.assertIn('outliers_detected', a.meta)
            self.assertIn('outlier_loci', a.meta)

    def test_analyses_are_web_type_completed(self):
        random.seed(42)
        sgs = [self._make_sg('SG001')]
        analyses: list[Analysis] = []
        asyncio.run(
            generate_web_report_analyses('PROJ', 1, sgs, analyses)
        )
        for a in analyses:
            self.assertEqual(a.type, 'web')
            self.assertEqual(str(a.status), 'completed')


class TestGenerateProjectPedigree(unittest.TestCase):
    """Phase 4a: Tests for generate_project_pedigree."""

    @patch('test.data.generate_seqr_project_data.ParticipantApi')
    @patch('test.data.generate_seqr_project_data.FamilyApi')
    def test_returns_id_map_from_participant_api(self, mock_family_cls, mock_participant_cls):
        random.seed(42)
        expected_map = {'SOLAR_0042': 1, 'LUNAR_0099': 2}

        mock_family_instance = mock_family_cls.return_value
        mock_family_instance.import_pedigree_async = AsyncMock()

        mock_participant_instance = mock_participant_cls.return_value
        mock_participant_instance.get_participant_id_map_by_external_ids_async = AsyncMock(
            return_value=expected_map
        )

        result = asyncio.run(generate_project_pedigree('test-project'))
        self.assertEqual(result, expected_map)

    @patch('test.data.generate_seqr_project_data.ParticipantApi')
    @patch('test.data.generate_seqr_project_data.FamilyApi')
    def test_import_pedigree_called_with_correct_args(self, mock_family_cls, mock_participant_cls):
        random.seed(42)

        mock_family_instance = mock_family_cls.return_value
        mock_family_instance.import_pedigree_async = AsyncMock()

        mock_participant_instance = mock_participant_cls.return_value
        mock_participant_instance.get_participant_id_map_by_external_ids_async = AsyncMock(
            return_value={}
        )

        asyncio.run(generate_project_pedigree('test-project'))

        mock_family_instance.import_pedigree_async.assert_called_once()
        call_kwargs = mock_family_instance.import_pedigree_async.call_args
        self.assertEqual(call_kwargs.kwargs['project'], 'test-project')
        self.assertFalse(call_kwargs.kwargs['has_header'])
        self.assertTrue(call_kwargs.kwargs['create_missing_participants'])

    @patch('test.data.generate_seqr_project_data.ParticipantApi')
    @patch('test.data.generate_seqr_project_data.FamilyApi')
    def test_participant_eids_passed_to_get_id_map(self, mock_family_cls, mock_participant_cls):
        random.seed(42)

        mock_family_instance = mock_family_cls.return_value
        mock_family_instance.import_pedigree_async = AsyncMock()

        mock_participant_instance = mock_participant_cls.return_value
        mock_participant_instance.get_participant_id_map_by_external_ids_async = AsyncMock(
            return_value={}
        )

        asyncio.run(generate_project_pedigree('test-project'))

        call_kwargs = mock_participant_instance.get_participant_id_map_by_external_ids_async.call_args
        self.assertEqual(call_kwargs.kwargs['project'], 'test-project')
        # request_body should be a list of individual IDs (strings)
        request_body = call_kwargs.kwargs['request_body']
        self.assertIsInstance(request_body, list)
        self.assertGreater(len(request_body), 0)
        for eid in request_body:
            self.assertIsInstance(eid, str)


class TestGenerateSampleEntries(unittest.TestCase):
    """Phase 4b: Tests for generate_sample_entries."""

    def test_upsert_called_with_sample_upserts(self):
        random.seed(42)
        participant_id_map = {'PART_01': 1, 'PART_02': 2}
        fake_enums = {'enum': {'sampleType': ['blood', 'saliva']}}
        mock_sapi = AsyncMock()

        asyncio.run(
            generate_sample_entries('test-proj', participant_id_map, fake_enums, mock_sapi)
        )

        mock_sapi.upsert_samples_async.assert_called_once()
        call_args = mock_sapi.upsert_samples_async.call_args
        self.assertEqual(call_args[0][0], 'test-proj')
        samples = call_args[0][1]
        self.assertIsInstance(samples, list)
        self.assertGreater(len(samples), 0)

    def test_each_sample_has_valid_fields(self):
        random.seed(42)
        participant_id_map = {'PART_01': 1, 'PART_02': 2}
        fake_enums = {'enum': {'sampleType': ['blood', 'saliva']}}
        mock_sapi = AsyncMock()

        asyncio.run(
            generate_sample_entries('test-proj', participant_id_map, fake_enums, mock_sapi)
        )

        samples = mock_sapi.upsert_samples_async.call_args[0][1]
        valid_participant_ids = set(participant_id_map.values())
        for sample in samples:
            # Each sample should have external_ids dict
            self.assertIsNotNone(sample.external_ids)
            # Each sample type should be from the enums
            self.assertIn(sample.type, ['blood', 'saliva'])
            # Each sample should reference a valid participant
            self.assertIn(sample.participant_id, valid_participant_ids)

    def test_deterministic_with_seed(self):
        """Same seed should produce the same samples."""
        participant_id_map = {'PART_01': 1}
        fake_enums = {'enum': {'sampleType': ['blood']}}

        mock_sapi_1 = AsyncMock()
        random.seed(99)
        asyncio.run(
            generate_sample_entries('proj', participant_id_map, fake_enums, mock_sapi_1)
        )

        mock_sapi_2 = AsyncMock()
        random.seed(99)
        asyncio.run(
            generate_sample_entries('proj', participant_id_map, fake_enums, mock_sapi_2)
        )

        samples_1 = mock_sapi_1.upsert_samples_async.call_args[0][1]
        samples_2 = mock_sapi_2.upsert_samples_async.call_args[0][1]
        self.assertEqual(len(samples_1), len(samples_2))
        for s1, s2 in zip(samples_1, samples_2, strict=True):
            self.assertEqual(s1.external_ids, s2.external_ids)
            self.assertEqual(s1.type, s2.type)
            self.assertEqual(s1.participant_id, s2.participant_id)


class TestGenerateCohorts(unittest.TestCase):
    """Phase 4c: Tests for generate_cohorts."""

    def _make_mock_cohort_api(self, cohort_id='COH123'):
        """Create a MagicMock cohort API (create_cohort_from_criteria is sync)."""
        mock_cohort_api = MagicMock()
        mock_cohort_api.create_cohort_from_criteria.return_value = {
            'cohort_id': cohort_id
        }
        return mock_cohort_api

    def test_one_cohort_per_type_tech_pair(self):
        sgs = [
            {'id': 'SG1', 'type': 'genome', 'technology': 'short-read'},
            {'id': 'SG2', 'type': 'genome', 'technology': 'short-read'},
            {'id': 'SG3', 'type': 'exome', 'technology': 'short-read'},
            {'id': 'SG4', 'type': 'genome', 'technology': 'long-read'},
        ]
        mock_cohort_api = self._make_mock_cohort_api()

        result = asyncio.run(generate_cohorts('test-proj', sgs, mock_cohort_api))

        # 3 unique (type, tech) pairs
        self.assertEqual(len(result), 3)
        self.assertIn(('genome', 'short-read'), result)
        self.assertIn(('exome', 'short-read'), result)
        self.assertIn(('genome', 'long-read'), result)

    def test_returns_cohort_ids(self):
        sgs = [
            {'id': 'SG1', 'type': 'genome', 'technology': 'short-read'},
        ]
        mock_cohort_api = self._make_mock_cohort_api('COH999')

        result = asyncio.run(generate_cohorts('proj', sgs, mock_cohort_api))

        self.assertEqual(result[('genome', 'short-read')], 'COH999')

    def test_create_cohort_called_per_pair(self):
        sgs = [
            {'id': 'SG1', 'type': 'genome', 'technology': 'short-read'},
            {'id': 'SG2', 'type': 'exome', 'technology': 'short-read'},
        ]
        mock_cohort_api = self._make_mock_cohort_api()

        asyncio.run(generate_cohorts('proj', sgs, mock_cohort_api))

        self.assertEqual(mock_cohort_api.create_cohort_from_criteria.call_count, 2)


class TestMain(unittest.TestCase):
    """Phase 4d: Tests for the main function."""

    @patch('test.data.generate_seqr_project_data.CohortApi')
    @patch('test.data.generate_seqr_project_data.EnumsApi')
    @patch('test.data.generate_seqr_project_data.SampleApi')
    @patch('test.data.generate_seqr_project_data.ProjectApi')
    @patch('test.data.generate_seqr_project_data.AnalysisApi')
    @patch('test.data.generate_seqr_project_data.query_async', new_callable=AsyncMock)
    @patch('test.data.generate_seqr_project_data.generate_project_pedigree', new_callable=AsyncMock)
    @patch('test.data.generate_seqr_project_data.generate_sample_entries', new_callable=AsyncMock)
    @patch('test.data.generate_seqr_project_data.generate_cram_analyses', new_callable=AsyncMock)
    @patch('test.data.generate_seqr_project_data.generate_cohorts', new_callable=AsyncMock)
    @patch('test.data.generate_seqr_project_data.generate_web_report_analyses', new_callable=AsyncMock)
    @patch('test.data.generate_seqr_project_data.generate_seqr_loader_analyses', new_callable=AsyncMock)
    def test_exit_when_no_default_user(
        self,
        _mock_seqr_loader,
        _mock_web_report,
        _mock_cohorts,
        _mock_cram,
        _mock_samples,
        _mock_pedigree,
        mock_query,
        _mock_analysis_cls,
        mock_project_cls,
        _mock_sample_cls,
        mock_enums_cls,
        _mock_cohort_cls,
    ):
        """When SM_LOCALONLY_DEFAULTUSER is not set, main should call sys.exit(1)."""
        mock_papi = mock_project_cls.return_value
        mock_papi.get_my_projects_async = AsyncMock(return_value=[])
        mock_papi.create_project_async = AsyncMock()

        mock_enums_instance = mock_enums_cls.return_value
        mock_enums_instance.post_analysis_types_async = AsyncMock()

        mock_query.return_value = {'enum': {'sampleType': ['blood']}}

        with patch.dict('os.environ', {}, clear=True), \
             self.assertRaises(SystemExit) as cm:
            asyncio.run(main())

        self.assertEqual(cm.exception.code, 1)

    @patch('test.data.generate_seqr_project_data.CohortApi')
    @patch('test.data.generate_seqr_project_data.EnumsApi')
    @patch('test.data.generate_seqr_project_data.SampleApi')
    @patch('test.data.generate_seqr_project_data.ProjectApi')
    @patch('test.data.generate_seqr_project_data.AnalysisApi')
    @patch('test.data.generate_seqr_project_data.query_async', new_callable=AsyncMock)
    @patch('test.data.generate_seqr_project_data.generate_project_pedigree', new_callable=AsyncMock)
    @patch('test.data.generate_seqr_project_data.generate_sample_entries', new_callable=AsyncMock)
    @patch('test.data.generate_seqr_project_data.generate_cram_analyses', new_callable=AsyncMock)
    @patch('test.data.generate_seqr_project_data.generate_cohorts', new_callable=AsyncMock)
    @patch('test.data.generate_seqr_project_data.generate_web_report_analyses', new_callable=AsyncMock)
    @patch('test.data.generate_seqr_project_data.generate_seqr_loader_analyses', new_callable=AsyncMock)
    def test_creates_project_when_not_in_existing(
        self,
        _mock_seqr_loader,
        _mock_web_report,
        mock_cohorts,
        mock_cram,
        _mock_samples,
        mock_pedigree,
        mock_query,
        mock_analysis_cls,
        mock_project_cls,
        _mock_sample_cls,
        mock_enums_cls,
        _mock_cohort_cls,
    ):
        """When project not in existing_projects, create_project_async is called."""
        mock_papi = mock_project_cls.return_value
        mock_papi.get_my_projects_async = AsyncMock(return_value=[])
        mock_papi.create_project_async = AsyncMock()
        mock_papi.update_project_members_async = AsyncMock()
        mock_papi.update_project_async = AsyncMock()

        mock_enums_instance = mock_enums_cls.return_value
        mock_enums_instance.post_analysis_types_async = AsyncMock()

        mock_query.return_value = {'enum': {'sampleType': ['blood']}, 'project': {'id': 1}}
        mock_pedigree.return_value = {}
        mock_cram.return_value = []
        mock_cohorts.return_value = {}

        mock_aapi = mock_analysis_cls.return_value
        mock_aapi.create_analysis_async = AsyncMock()

        with patch.dict('os.environ', {'SM_LOCALONLY_DEFAULTUSER': 'testuser@example.com'}):
            asyncio.run(main())

        # create_project_async should have been called for each project in PROJECTS
        self.assertEqual(
            mock_papi.create_project_async.call_count, len(PROJECTS)
        )


class TestEdgeCases(unittest.TestCase):
    """Phase 5: Edge case tests."""

    def test_large_num_families_exercises_all_branches(self):
        """generate_pedigree_rows with large num_families exercises all family-size branches."""
        random.seed(42)
        result = generate_pedigree_rows(num_families=20)
        # Should produce many rows across multiple families
        self.assertGreater(len(result), 20)
        # All individual IDs should still be unique
        individual_ids = [row.individual_id for row in result]
        self.assertEqual(len(individual_ids), len(set(individual_ids)))

        # Check that we see different family sizes (singleton, duo, trio+)
        family_ids = {}
        for row in result:
            family_ids.setdefault(row.family_id, []).append(row)
        family_sizes = {len(members) for members in family_ids.values()}
        # With 20 families and seed 42, we should see at least 2 different sizes
        self.assertGreaterEqual(len(family_sizes), 2)

    def test_generate_seq_technology_fallback_for_unknown_type(self):
        """When type doesn't match genome/exome/transcriptome, falls back to rna tech."""
        random.seed(42)
        result = generate_seq_technology(
            ['rna-seq', 'short-read'], 'something_else'
        )
        self.assertIn('rna', result)

    def test_generate_web_report_analyses_empty_sgs(self):
        """
        generate_web_report_analyses with empty SG list still appends a STRipy index.

        This documents the current behavior: the per-SG loop body is
        skipped, but the STRipy index analysis (line 668-683) is always
        appended with an empty sequencing_group_ids list.
        """
        random.seed(42)
        analyses: list[Analysis] = []
        asyncio.run(
            generate_web_report_analyses('PROJ', 1, [], analyses)
        )
        # Only the STRipy index entry is created
        self.assertEqual(len(analyses), 1)
        self.assertEqual(analyses[0].meta.get('stage'), 'MakeIndexPage')
        self.assertEqual(analyses[0].sequencing_group_ids, [])
