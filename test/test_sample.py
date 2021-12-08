from sample_metadata.apis import SampleApi, AnalysisApi
from sample_metadata.models import NewSample, AnalysisModel


PROJ = 'test_project'


def run_test():
    """Run test"""
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

    _print_samples()
    print()

    new_sample = NewSample(
        external_id='Test', type='blood', meta={'other-meta': 'value'}
    )
    sample_id = sapi.create_new_sample(PROJ, new_sample)
    print(f'Inserted sample with ID: {sample_id}')

    analysis = AnalysisModel(
        sample_ids=[sample_id],
        type='gvcf',
        output='gs://output-path',
        status='completed',
    )
    analysis_id = aapi.create_new_analysis(PROJ, analysis)
    print(f'Inserted analysis with ID: {analysis_id}')

    print()
    _print_samples()


if __name__ == '__main__':
    run_test()
