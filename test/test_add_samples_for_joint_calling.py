"""
Adds samples for a subsequent test_joint_calling_workflow.py test

Requires either write access to test_input_project,
or the server run with SM_ALLOWALLACCESS=1
"""

import random
import string

from sample_metadata.apis import SampleApi
from sample_metadata.models import NewSample, SampleType

INPUT_PROJECT = 'test_input_project'


sapi = SampleApi()


def _add_samples(run_id: str, project: str):
    """
    Add 3 samples: one with fastq input, one with CRAM input, one with GVCF input.
    :param run_id: to suffix sample names for uniqueness
    """
    s1 = NewSample(
        external_id=f'NA12878-from-fq-{run_id}',
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
        external_id=f'NA12878-from-cram-{run_id}',
        type=SampleType('blood'),
        meta={'reads': 'gs://cpg-seqr-test/batches/NA12878-trio-tiny/NA12878.cram'},
    )
    s3 = NewSample(
        external_id=f'NA12878-from-gvcf-{run_id}',
        type=SampleType('blood'),
        meta={'reads': 'gs://cpg-seqr-test/batches/NA12878-trio/NA12878.g.vcf.gz'},
    )
    sample_ids = [sapi.create_new_sample(project, s) for s in (s1, s2, s3)]
    print(f'Added samples {", ".join(sample_ids)}')
    return sample_ids


if __name__ == '__main__':
    # Unique test run ID to avoid clashing with previous test run samples
    TEST_RUN_ID = ''.join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(6)
    )
    print(f'Test run ID: {TEST_RUN_ID}')
    print(f'Populate samples for test run {TEST_RUN_ID}, input project {INPUT_PROJECT}')
    _add_samples(TEST_RUN_ID, INPUT_PROJECT)
