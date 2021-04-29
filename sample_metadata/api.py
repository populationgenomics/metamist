import json
from models.models.analysis import Analysis
from models.models.sample import Sample
import requests

from models.enums.samplestatus import SampleStatus

class SampleMetadataApi:

    def __init__(self, project: str) -> None:
        pass

    URL_STRUCTURE = "/api/v1/{project}/{module}/{name}"

    def _get_url(self, module, name, query_params=None):
        q = ""
        if query_params:
            q = "?" + "&".join(f'{k}={v}' for k,v in query_params.items())

        return self.URL_STRUCTURE.format(
            project=self.project,
            module=module,
            name=name
        ) + q

    def _post(self, module, name, data=None):
        resp = requests.post(
            self._get_url(module, name),
            json=json.dumps(data)
        )
        resp.raise_for_status()
        return resp.text

    def _get(self, module, name, query_params):
        resp = requests.get(
            self._get_url(module, name, query_params)
        )
        resp.raise_for_status()
        return resp.text


    def get_samples_by_status(self, status: SampleStatus) -> Sample:
        return self._get("sample", "", {status: status})

    def add_analysis(self, analysis: Analysis, internal_sample_ids: List[str]) -> Analysis:
        return self._post(
            "analysis", "",
            {
                "analysis": {
                    **analysis,
                    "sample_ids": internal_sample_ids
                }
            }
        )

