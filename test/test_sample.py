import os
from sample_metadata.apis import SampleApi, AnalysisApi
from sample_metadata.models.new_sample import NewSample
from sample_metadata.models.analysis_model import AnalysisModel


PROJ = os.environ.get('SM_DEV_DB_PROJECT', 'sm_dev')


sapi = SampleApi()
aapi = AnalysisApi()

new_sample = NewSample(external_id='Test', type='blood', meta={'other-meta': 'value'})
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
