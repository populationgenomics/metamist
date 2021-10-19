import string
import unittest
import random

from sample_metadata import (
    exceptions,
    NewSequence,
    AnalysisModel,
    NewSample,
    SampleApi,
    AnalysisApi,
    SequenceApi,
    SampleUpdateModel,
)

PROJ = 'dev'
sapi = SampleApi()
aapi = AnalysisApi()
seqapi = SequenceApi()


class TestSmApiBasic(unittest.TestCase):
    """
    Test basic API commands: add and get samples, analyses, sequences
    """

    def test_sm_api_basic(self):
        """Run test"""
        sample_by_spgid, _, _, _ = _list_db_contents()

        rand_suf = ''.join(random.choices(string.ascii_uppercase, k=3))
        sample_id = f'Test_{rand_suf}'
        if sample_id not in sample_by_spgid:
            sample_id = sapi.create_new_sample(
                PROJ,
                NewSample(
                    external_id=sample_id, type='blood', meta={'other-meta': 'value'}
                ),
            )
            print(f'Inserted sample with ID {sample_id}')
        else:
            sample_id = sample_by_spgid[0]['id']
            print(f'Pulled existing sample with ID {sample_id}')
        print()

        for t in ['gvcf', 'cram']:
            analysis_id = aapi.create_new_analysis(
                PROJ,
                AnalysisModel(
                    sample_ids=[sample_id],
                    type=t,
                    output='gs://output-path',
                    status='completed',
                ),
            )
            print(f'Inserted {t.upper()} analysis with ID: {analysis_id}')
        print()

        seq_id = seqapi.create_new_sequence(
            NewSequence(
                sample_id=sample_id,
                status='uploaded',
                type='wgs',
                meta={},
            )
        )
        print(f'Inserted sequence with ID: {seq_id}')
        print()

        (
            sample_by_spgid,
            seq_ids_by_cpgid,
            cram_analyses_by_cpgid,
            gvcf_analyses_by_cpgid,
        ) = _list_db_contents()
        self.assertIn(sample_id, sample_by_spgid)
        self.assertIn(sample_id, seq_ids_by_cpgid)
        self.assertIn(sample_id, cram_analyses_by_cpgid)
        self.assertIn(sample_id, gvcf_analyses_by_cpgid)

        sapi.update_sample(sample_id, SampleUpdateModel(active=False))


def _list_db_contents():
    samples = sapi.get_samples(
        body_get_samples_by_criteria_api_v1_sample_post={
            'project_ids': [PROJ],
            'active': True,
        }
    )
    sample_by_spgid = {s['id']: s for s in samples}
    cpgids = [s['id'] for s in samples]
    seq_ids_by_cpgid = seqapi.get_sequence_ids_from_sample_ids(
        request_body=cpgids,
    )
    cram_analyses = aapi.get_latest_analysis_for_samples_and_type(
        project=PROJ,
        analysis_type='cram',
        request_body=cpgids,
    )
    gvcf_analyses = aapi.get_latest_analysis_for_samples_and_type(
        project=PROJ,
        analysis_type='gvcf',
        request_body=cpgids,
    )
    cram_analyses_by_cpgid = {a['sample_ids'][0]: a for a in cram_analyses}
    gvcf_analyses_by_cpgid = {a['sample_ids'][0]: a for a in gvcf_analyses}
    print(f'Found {len(samples)} "active" samples')
    for s in samples:
        print(f'Found sample {s["id"]}: {s}')
        if s['id'] in seq_ids_by_cpgid:
            print(f'Sequencing entry IDs: {seq_ids_by_cpgid[s["id"]]}')
        else:
            print(f'No sequencing entries found for sample')

        if s['id'] in cram_analyses_by_cpgid:
            print(f'CRAM analysis: {cram_analyses_by_cpgid[s["id"]]}')
        else:
            print(f'No CRAM analysis entries found for sample')

        if s['id'] in gvcf_analyses_by_cpgid:
            print(f'GVCF analysis: {gvcf_analyses_by_cpgid[s["id"]]}')
        else:
            print(f'No GVCF analysis entries found for sample')
        print()

    try:
        jc_analysis = aapi.get_latest_complete_analysis_for_type(
            project=PROJ,
            analysis_type='joint-calling',
        )
    except exceptions.ApiException:
        print(f'Not joint-calling analysis found in the project')
    else:
        print(f'Joint-calling analysis: {jc_analysis}')
    print()
    return (
        sample_by_spgid,
        seq_ids_by_cpgid,
        cram_analyses_by_cpgid,
        gvcf_analyses_by_cpgid,
    )
