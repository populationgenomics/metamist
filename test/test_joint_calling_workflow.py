from typing import List

from sample_metadata.apis import SampleApi, AnalysisApi
from sample_metadata.models import (
    NewSample,
    AnalysisModel,
    AnalysisUpdateModel,
    SampleType,
    AnalysisType,
    AnalysisStatus,
)


INPUT_PROJECT = 'test_input_project'
OUTPUT_PROJECT = 'test_output_project'


aapi = AnalysisApi()
sapi = SampleApi()


def _add_samples(test_run_id: str, project: str):
    """
    Add 3 samples: one with fastq input, one with CRAM input, one with GVCF input.
    :param test_run_id: to suffix sample names for uniqueness
    """
    s1 = NewSample(
        external_id=f'NA12878-from-fq-{test_run_id}',
        type=SampleType('blood'),
        meta={
            'reads': [
                [
                    'gs://cpg-seqr-test/batches/NA12878-trio-tiny/NA12878_L001_R1.fq',
                    'gs://cpg-seqr-test/batches/NA12878-trio-tiny/NA12878_L002_R1.fq',
                ],
                [
                    'gs://cpg-seqr-test/batches/NA12878-trio-tiny/NA12878_L001_R2.fq',
                    'gs://cpg-seqr-test/batches/NA12878-trio-tiny/NA12878_L002_R2.fq',
                ],
            ]
        },
    )
    s2 = NewSample(
        external_id=f'NA12878-from-cram-{test_run_id}',
        type=SampleType('blood'),
        meta={'reads': 'gs://cpg-seqr-test/batches/NA12878-trio-tiny/NA12878.cram'},
    )
    s3 = NewSample(
        external_id=f'NA12878-from-gvcf-{test_run_id}',
        type=SampleType('blood'),
        meta={'reads': 'gs://cpg-seqr-test/batches/NA12878-trio/NA12878.g.vcf.gz'},
    )
    sample_ids = [sapi.create_new_sample(project, s) for s in (s1, s2, s3)]
    print(f'Added samples {", ".join(sample_ids)}')
    return sample_ids


def _submit_analyses(samples: List, output_project: str, a_type: str):
    """
    Add or update analyses. Iterate over completed analyses,
    and submit next-step analyses
    """

    if a_type in ['gvcf', 'cram']:
        for s in samples:
            if a_type == 'gvcf':
                cram_analysis = aapi.get_latest_analysis_for_samples_and_type(
                    project=output_project,
                    analysis_type=AnalysisType('cram'),
                    request_body=[s['id']],
                )
                assert cram_analysis, s

            # completed_analysis = latest_by_type_and_sids.get(('cram', (s['id'],))),
            am = AnalysisModel(
                type=AnalysisType(a_type),
                output=f'result.{a_type}',
                status=AnalysisStatus('queued'),
                sample_ids=[s['id']],
            )
            aapi.create_new_analysis(project=output_project, analysis_model=am)

    elif a_type == 'joint-calling':
        for s in samples:
            gvcf_analysis = aapi.get_latest_analysis_for_samples_and_type(
                project=output_project,
                analysis_type=AnalysisType('gvcf'),
                request_body=[s['id']],
            )
            assert gvcf_analysis, s

        am = AnalysisModel(
            sample_ids=[s['id'] for s in samples],
            type=AnalysisType('joint-calling'),
            output='joint-called.vcf',
            status=AnalysisStatus('queued'),
        )
        aapi.create_new_analysis(project=output_project, analysis_model=am)


def _update_analyses(proj: str, new_status: str):
    """
    Update existing analyses status
    """
    analyses = aapi.get_incomplete_analyses(project=proj)
    print(f'Current analyses:')
    for a in analyses:
        print(a)

    if analyses:
        for a in analyses:
            print(f'Setting analysis {a} to {new_status}')
            aum = AnalysisUpdateModel(status=AnalysisStatus(new_status))
            aapi.update_analysis_status(
                analysis_id=a['id'],
                analysis_update_model=aum,
            )


def test_simulate_joint_calling_pipeline(
    input_project: str,
    output_project: str,
):
    """
    Simulates events of the joint-calling workflow
    """
    samples = sapi.get_samples(
        body_get_samples_by_criteria_api_v1_sample_post={
            'project_ids': [input_project],
            'active': True,
        }
    )

    print('Add/update analyses, reads -> cram')
    _submit_analyses(samples, output_project, a_type='cram')
    print()
    _update_analyses(output_project, new_status='in-progress')
    print()
    _update_analyses(output_project, new_status='completed')
    print()

    print('Add/update analyses, cram -> gvcf')
    _submit_analyses(samples, output_project, a_type='gvcf')
    print()
    _update_analyses(output_project, new_status='in-progress')
    print()
    _update_analyses(output_project, new_status='completed')
    print()

    print('Add/update analyses, gvcf -> joint_calling')
    _submit_analyses(samples, output_project, a_type='joint-calling')
    print()
    _update_analyses(output_project, new_status='in-progress')
    print()
    _update_analyses(output_project, new_status='completed')
    print()

    # Checking that after all calls, a 'completed' 'joint-calling' analysis must exist
    # that includes all initally added samples
    analysis = aapi.get_latest_complete_analysis_for_type(
        project=output_project, analysis_type=AnalysisType('joint-calling')
    )
    assert analysis['type'] == 'joint-calling'
    assert set([s['id'] for s in samples]).issubset(set(analysis['sample_ids']))


if __name__ == '__main__':
    test_simulate_joint_calling_pipeline(
        input_project=INPUT_PROJECT,
        output_project=OUTPUT_PROJECT,
    )
