"""
PBMC samples should have the same participant as their WGS counterparts.
This script adjusts the data within the TOB-WGS project to uphold this.
"""

from sample_metadata.apis import SampleApi
from sample_metadata.models import SampleUpdateModel


def main():
    """Update participant IDs of PBMC Samples"""
    sapi = SampleApi()

    # TODO: Swap test_project with tob-wgs
    all_samples = sapi.get_samples(
        body_get_samples_by_criteria_api_v1_sample_post={
            'project_ids': ['test_project'],
            'active': True,
        }
    )

    all_sample_ids = [
        sample['external_id'] for sample in all_samples if 'external_id' in sample
    ]

    pbmc_ids = list(filter(lambda id: '-PBMC' in id, all_sample_ids))
    pbmc_map = {i: i.strip('-PBMC') for i in pbmc_ids}

    for pbmc_id, s_id in pbmc_map.items():
        participant_id = int(
            next(
                (
                    sample['participant_id']
                    for sample in all_samples
                    if sample['external_id'] == s_id
                ),
                None,
            )
        )

        internal_id = next(
            (
                sample['id']
                for sample in all_samples
                if sample['external_id'] == pbmc_id
            ),
            None,
        )

        if participant_id is None:
            print(f'Skipping {pbmc_id}, could not find.')
        else:
            sample_update = SampleUpdateModel(participant_id=participant_id)
            sapi.update_sample(id_=internal_id, sample_update_model=sample_update)


if __name__ == '__main__':
    main()
