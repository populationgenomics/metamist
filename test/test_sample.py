from sample_metadata.apis import SampleApi, AnalysisApi
from sample_metadata.model.new_sample import NewSample
from sample_metadata.model.analysis_model import AnalysisModel
from sample_metadata.models import AnalysisStatus, AnalysisType, SampleType

sapi = SampleApi()
aapi = AnalysisApi()

new_sample = NewSample(
    external_id='CPG0007', type=SampleType('blood'), meta={'other-meta': 'value'}
)
sample_id = sapi.create_new_sample('dev', new_sample)

analysis = AnalysisModel(
    sample_ids=[sample_id],
    type=AnalysisType('gvcf'),
    output='gs://output-path',
    status=AnalysisStatus('completed'),
)
analysis_id = aapi.create_new_analysis('dev', analysis)

print(f'Inserted sample with ID: {sample_id}, inserted analysis with ID: {analysis_id}')
