from models.models.sample import Sample

from sample_metadata.api import get_sm


def _get_sample_api(project, method, q=None):
    return get_sm(project, 'sample', method, query_params=q)


def get_sample_by_external_id(project: str, external_id: str) -> Sample:
    """Get sample by external id, returns Sample object"""
    return Sample.from_db(**_get_sample_api(project, external_id))


if __name__ == '__main__':
    sample_obj = get_sample_by_external_id('sm_dev', 'TOB1761')
