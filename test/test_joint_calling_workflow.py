import os
import string
import random
import sys
from collections import defaultdict

from models.models.sample import sample_id_format
from sample_metadata import AnalysisUpdateModel
from sample_metadata.api import SampleApi, AnalysisApi
from sample_metadata.models.new_sample import NewSample
from sample_metadata.models.analysis_model import AnalysisModel


PROJ = os.environ.get('SM_DEV_DB_PROJECT', 'sm_dev')


def _jc_pipeline_add_samples(test_run_id: str):
    """
    Add 3 samples: one with fastq input, one with CRAM input, one with GVCF input.
    :param test_run_id: to suffix sample names for uniqueness
    """
    sapi = SampleApi()
    s1 = NewSample(
        external_id=f'NA12878-from-fq-{test_run_id}',
        type='blood',
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
        type='blood',
        meta={'reads': 'gs://cpg-seqr-test/batches/NA12878-trio-tiny/NA12878.cram'},
    )
    s3 = NewSample(
        external_id=f'NA12878-from-gvcf-{test_run_id}',
        type='blood',
        meta={'reads': 'gs://cpg-seqr-test/batches/NA12878-trio/NA12878.g.vcf.gz'},
    )
    sample_ids = [sapi.create_new_sample(PROJ, s) for s in (s1, s2, s3)]
    print(f'Added samples {", ".join(sample_ids)}')
    return sample_ids


def _jc_pipeline_submit_analyses():
    """
    Add or update analyses. Iterate over completed analyses,
    and submit next-step analyses
    """

    # If there are incomplete analyses, throw an error
    aapi = AnalysisApi()
    analyses = aapi.get_incomplete_analyses(project=PROJ)
    if analyses:
        print(f'ERROR: found incomplete or queued analysis: {analyses}')
        sys.exit()

    # TODO: for samples with finished joint-calling, latest-completed will return joint-calling.
    # so on reruns, pipelien will redo haploytype calling. We want to avoid that.

    # Get the list of latest complete analyses
    latest_complete_analyses = aapi.get_latest_complete_analyses(project=PROJ)
    print(f'Latest complete analyses: {latest_complete_analyses}')
    latest_by_type_and_sids = defaultdict(list)
    for a in latest_complete_analyses:
        a_s_ids = sample_id_format(a['sample_ids'])
        latest_by_type_and_sids[(a['type'], tuple(set(a_s_ids)))].append(a)

    # Iterate over samples, check latest complete analyses, and add next-step analyses
    sapi = SampleApi()
    samples = sapi.get_all_samples(project=PROJ)

    if latest_by_type_and_sids.get(
        ('joint-calling', tuple(set(s.id for s in samples)))
    ):
        print(f'All samples went through joint-calling, nothing to submit')
        return

    latest_complete_gvcf_analyses = aapi.get_latest_complete_analyses_by_type(
        project=PROJ, analysis_type='gvcf'
    )
    sids_with_gvcf = set(
        sample_id_format(a['sample_ids'])[0] for a in latest_complete_gvcf_analyses
    )
    new_sids_with_gvcf = set(s.id for s in samples) - sids_with_gvcf
    if not new_sids_with_gvcf:
        print('All samples went through variant calling, so can submit joint-calling')
        analysis = AnalysisModel(
            sample_ids=[s.id for s in samples],
            type='joint-calling',
            output='gs://my-bucket/joint-calling/joint-called.g.vcf.gz',
            status='queued',
        )
        print(f'Queueing {analysis.type}')
        aapi.create_new_analysis(project=PROJ, analysis_model=analysis)
        return

    for s in [s for s in samples if s.id in new_sids_with_gvcf]:
        print(f'Sample {s.id}')

        if latest_by_type_and_sids.get(('gvcf', (s.id,))):
            print('  Sample has a complete gvcf analysis')

        elif latest_by_type_and_sids.get(('cram', (s.id,))):
            print(f'  Sample has a complete CRAM analysis, queueing variant calling')
            analysis = AnalysisModel(
                sample_ids=[s.id],
                type='gvcf',
                output=f'gs://my-bucket/variant-calling/{s.id}.g.vcf.gz',
                status='queued',
            )
            aapi.create_new_analysis(project=PROJ, analysis_model=analysis)

        else:
            print(
                f'  Sample doesn not have any analysis yet, trying to get "reads" '
                'metadata to submit alignment'
            )
            reads_data = s.meta.get('reads')
            if not reads_data:
                print(f'  ERROR: no "reads" data')
            elif isinstance(reads_data, str):
                if reads_data.endswith('.g.vcf.gz'):
                    analysis = AnalysisModel(
                        sample_ids=[s.id],
                        type='gvcf',
                        output=reads_data,
                        status='completed',
                    )
                    aapi.create_new_analysis(project=PROJ, analysis_model=analysis)
                elif reads_data.endswith('.cram') or reads_data.endswith('.bam'):
                    print(f'  Queueing cram re-alignment analysis')
                    analysis = AnalysisModel(
                        sample_ids=[s.id],
                        type='cram',
                        output=f'gs://my-bucket/realignment/{s.id}.cram',
                        status='queued',
                    )
                    aapi.create_new_analysis(project=PROJ, analysis_model=analysis)
                else:
                    print(f'  ERROR: unrecognised "reads" meta data: {reads_data}')
            elif isinstance(reads_data, list) and len(reads_data) == 2:
                print(f'  Queueing cram alignment analyses')
                analysis = AnalysisModel(
                    sample_ids=[s.id],
                    type='cram',
                    output=f'gs://my-bucket/alignment/{s.id}.cram',
                    status='queued',
                )
                aapi.create_new_analysis(project=PROJ, analysis_model=analysis)
            else:
                print(f'  ERROR: can\'t recognise "reads" data: {reads_data}')


def _jc_pipeline_set_in_progress():
    """
    Update existing queued analyses and set their status to in-progress
    """
    aapi = AnalysisApi()
    analyses = aapi.get_incomplete_analyses(project=PROJ)
    if analyses:
        for a in analyses:
            print(f'Setting analysis {a} to in-progress')
            aum = AnalysisUpdateModel(status='in-progress')
            aapi.update_analysis_status(
                analysis_id=a['id'],
                project=PROJ,
                analysis_update_model=aum,
            )


def _jc_pipeline_set_completed():
    """
    Update existing in-progress analyses and set their status to completed
    """
    aapi = AnalysisApi()
    analyses = aapi.get_incomplete_analyses(project=PROJ)
    if analyses:
        for a in analyses:
            print(f'Setting analysis {a} to completed')
            aum = AnalysisUpdateModel(status='completed')
            aapi.update_analysis_status(
                analysis_id=a['id'],
                project=PROJ,
                analysis_update_model=aum,
            )


def test_simulate_joint_calling_pipeline():
    """
    Simulates events of the joint-calling workflow
    """

    # test_run_id = 'AFYPXR'
    # sample_ids = 'CPG620, CPG638, CPG646'.split(', ')

    # Unique test run ID to avoid clashing with previous test run samples
    test_run_id = os.environ.get(
        'SM_DV_TEST_RUN_ID',
        ''.join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(6)
        ),
    )
    print(f'Test run ID: {test_run_id}')

    print(f'Populate samples for test run {test_run_id}')
    sample_ids = _jc_pipeline_add_samples(test_run_id)
    print()

    print('Add/update analyses, reads -> cram')
    _jc_pipeline_submit_analyses()
    print()
    _jc_pipeline_set_in_progress()
    print()
    _jc_pipeline_set_completed()
    print()

    print('Add/update analyses, cram -> gvcf')
    _jc_pipeline_submit_analyses()
    print()
    _jc_pipeline_set_in_progress()
    print()
    _jc_pipeline_set_completed()
    print()

    print('Add/update analyses, gvcf -> joint-calling')
    _jc_pipeline_submit_analyses()
    print()
    _jc_pipeline_set_in_progress()
    print()
    _jc_pipeline_set_completed()
    print()

    # Checking that after all calls, a 'completed' 'joint-calling' analysis must exist
    # for the initally added samples
    aapi = AnalysisApi()
    analyses = aapi.get_latest_complete_analyses(project=PROJ)
    assert any(
        a['type'] == 'joint-calling'
        and set(sample_ids) & set(sample_id_format(a['sample_ids'])) == set(sample_ids)
        for a in analyses
    ), [
        (a['type'], set(sample_id_format(a['sample_ids'])), set(sample_ids))
        for a in analyses
    ]


if __name__ == '__main__':
    test_simulate_joint_calling_pipeline()
