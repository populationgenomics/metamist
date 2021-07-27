from sample_metadata.api import SampleApi, AnalysisApi
from sample_metadata.models.new_sample import NewSample
from sample_metadata.models.analysis_model import AnalysisModel

sapi = SampleApi()
aapi = AnalysisApi()

new_sample = NewSample(
    external_id='CPG-109', type='blood', meta={'other-meta': 'value'}
)
sample_id = sapi.create_new_sample('dev', new_sample)

analysis = AnalysisModel(
    sample_ids=[sample_id],
    type='gvcf',
    output='gs://output-path',
    status='completed',
)
analysis_id = aapi.create_new_analysis('dev', analysis)

print(f'Inserted sample with ID: {sample_id}, inserted analysis with ID: {analysis_id}')
