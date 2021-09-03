from sample_metadata.api import SampleApi, AnalysisApi
from sample_metadata.models.new_sample import NewSample
from sample_metadata.models.analysis_model import AnalysisModel


PROJ = 'test_output_project'


sapi = SampleApi()
aapi = AnalysisApi()


def _print_samples():
    samples = sapi.get_samples(
        body_get_samples_by_criteria_api_v1_sample_post={
            'project_ids': [PROJ],
            'active': True,
        }
    )
    for s in samples:
        print(f'Found sample {s["id"]}: {s}')
        print(f'Analyses for sample {s["id"]}:')
        for t in ['cram', 'gvcf']:
            a = aapi.get_latest_analysis_for_samples_and_type(
                project=PROJ,
                analysis_type=t,
                request_body=[s['id']],
            )
            print(f'   Type: {t}: {a}')
        t = 'joint-calling'
        a = aapi.get_latest_complete_analysis_for_type(
            project=PROJ,
            analysis_type=t,
        )
        print(f'   Type: {t}: {a}')
    print()
    return samples


existing_samples = _print_samples()

if not existing_samples:
    sample_id = sapi.create_new_sample(
        PROJ, NewSample(external_id='Test', type='blood', meta={'other-meta': 'value'})
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
    )    
)
print(f'Inserted analysis with ID: {analysis_id}')
print()

_print_samples()
