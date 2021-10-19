from sample_metadata.api import SampleApi, AnalysisApi, SequenceApi
from sample_metadata.models.new_sample import NewSample
from sample_metadata.models.analysis_model import AnalysisModel
from sample_metadata.models.new_sequence import NewSequence


PROJ = 'test_output_project'
sapi = SampleApi()
aapi = AnalysisApi()
seqapi = SequenceApi()


def _list_db_contents():
    samples = sapi.get_samples(
        body_get_samples_by_criteria_api_v1_sample_post={
            'project_ids': [PROJ],
            'active': True,
        }
    )
    cpgids = [s['id'] for s in samples]
    seq_ids_by_cpgid = seqapi.get_sequence_ids_from_sample_ids(
        project=PROJ,
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
    jc_analysis = aapi.get_latest_complete_analysis_for_type(
        project=PROJ,
        analysis_type='joint_calling',
    )
    cram_analyses_by_cpgid = {a['sample_ids'][0]: a for a in cram_analyses}
    gvcf_analyses_by_cpgid = {a['sample_ids'][0]: a for a in gvcf_analyses}
    for s in samples:
        print(f'Found sample {s["id"]}: {s}')
        print(f'Sequencing entries: {s["id"]}: {seq_ids_by_cpgid[s["id"]]}')
        print(f'Analyses for sample {s["id"]}:')
        print(f'  CRAM: {cram_analyses_by_cpgid[s["id"]]}')
        print(f'  GVCF: {gvcf_analyses_by_cpgid[s["id"]]}')
    print(f'Joint-calling analysis: {jc_analysis}')
    print()
    return samples


def run_test():
    """Run test"""
    existing_samples = _list_db_contents()

    if not existing_samples:
        sample_id = sapi.create_new_sample(
            PROJ,
            NewSample(external_id='Test', type='blood', meta={'other-meta': 'value'}),
        )
        print(f'Inserted sample with ID: {sample_id}')
    else:
        sample_id = existing_samples[0]['id']

    analysis_id = aapi.create_new_analysis(
        PROJ,
        AnalysisModel(
            sample_ids=[sample_id],
            type='gvcf',
            output='gs://output-path',
            status='completed',
        ),
    )
    print(f'Inserted analysis with ID: {analysis_id}')

    seq_id = seqapi.create_new_sequence(
        NewSequence(sample_id=sample_id, status='uploaded', type='WGS')
    )
    print(f'Inserted sequence with ID: {seq_id}')
    print()
    _list_db_contents()


if __name__ == '__main__':
    run_test()
