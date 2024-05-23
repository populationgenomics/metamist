"""
PBMC samples should have the same participant as their WGS counterparts.
This script adjusts the data within the TOB-WGS project to uphold this.
"""

from api.routes.sample import GetSamplesCriteria
from metamist.apis import SampleApi
from metamist.models import SampleUpsert


def main():
    """Update participant IDs of PBMC Samples"""
    sapi = SampleApi()

    all_samples = sapi.get_samples(
        GetSamplesCriteria(
            project_ids=['tob-wgs'],
            active=True,
        )
    )

    sample_map_by_external_id = {s['external_id']: s for s in all_samples}

    pbmc_samples = [s for s in all_samples if '-PBMC' in s['external_id']]

    for sample in pbmc_samples:
        external_id = sample['external_id']
        non_pbmc_id = external_id.strip('-PBMC')
        non_pbmc_sample = sample_map_by_external_id.get(non_pbmc_id)
        pbmc_sample = sample_map_by_external_id.get(external_id)
        try:
            participant_id = int(non_pbmc_sample['participant_id'])
            internal_id = pbmc_sample['id']
            sample_update = SampleUpsert(id=internal_id, participant_id=participant_id)
            sapi.update_sample(id_=internal_id, sample_upsert=sample_update)
        except TypeError:
            print(f'Skipping {external_id}, could not find.')


if __name__ == '__main__':
    main()
