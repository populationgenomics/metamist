"""
Update metadata for hgdp and thousand-genomes datasets:
population labels, sex, pedigree.
"""

import csv
from cpg_utils import to_path

from sample_metadata.model.participant_upsert import ParticipantUpsert
from sample_metadata.model.participant_update_model import ParticipantUpdateModel
from sample_metadata.model.participant_upsert_body import ParticipantUpsertBody
from sample_metadata.apis import SampleApi, ParticipantApi, FamilyApi
from sample_metadata.model.sample_batch_upsert import SampleBatchUpsert
from sample_metadata.model.sample_update_model import SampleUpdateModel

sapi = SampleApi()
papi = ParticipantApi()
fapi = FamilyApi()

for project in ['thousand-genomes', 'hgdp']:
    metamist_samples = sapi.get_samples(
        body_get_samples={
            'project_ids': [project],
        }
    )

    pops_tsv_path = 'samples-pops.tsv'
    meta_by_collaborator_id = {}
    meta_by_illumina_id = {}
    with to_path(pops_tsv_path).open() as fh:
        tsv_file = csv.DictReader(fh, delimiter='\t')
        for entry in tsv_file:
            sid = entry['Sample ID (Collaborator)']
            meta_by_collaborator_id[sid] = entry
            illumina_id = entry['Sample ID (Illumina)']
            if illumina_id != 'NA':
                meta_by_illumina_id[illumina_id] = entry

    meta_by_cpg_id = {}
    for s in metamist_samples:
        ext_id = s['external_id']
        meta = meta_by_illumina_id.get(ext_id) or meta_by_collaborator_id.get(ext_id)
        if not meta:
            print(f'Error: metadata not found for sample {s}')
            continue
        meta_by_cpg_id[s['id']] = meta
        meta_by_cpg_id[s['id']]['existing_ext_id'] = ext_id

    # Pedigree
    if project == 'thousand-genomes' and input('Load pedigree? (y/n): ').lower() == 'y':
        ped_path = '20130606_g1k-cut16.ped'
        with open(ped_path) as f:
            fapi.import_pedigree(
                project, f, has_header=True, create_missing_participants=True
            )

    # Participants
    try:
        pid_map = papi.get_participant_id_map_by_external_ids(
            project=project, request_body=[s['external_id'] for s in metamist_samples]
        )
    except BaseException:
        if input('Participant entries do not exist. Create? (y/n): ').lower() == 'y':
            # Create new
            body = ParticipantUpsertBody(
                participants=[
                    ParticipantUpsert(
                        external_id=meta['Sample ID (Collaborator)'],
                        reported_sex={'male': 1, 'female': 2}.get(
                            meta['Sex'].lower(), 0
                        ),
                        meta={
                            'Superpopulation name': meta['Superpopulation name'],
                            'Population name': meta['Population name'],
                            'Population description': meta['Population description'],
                        },
                        samples=[
                            SampleBatchUpsert(
                                id=cpg_id,
                                sequences=[],
                            ),
                        ],
                    )
                    for cpg_id, meta in meta_by_cpg_id.items()
                ]
            )
            print(papi.batch_upsert_participants(project, body))
    else:
        if input('Participant entries exist. Update? (y/n): ').lower() == 'y':
            # Update existing
            participant_update_models = {
                meta['existing_ext_id']: ParticipantUpdateModel(
                    external_id=meta['Sample ID (Collaborator)'],
                    reported_sex={'male': 1, 'female': 2}.get(meta['Sex'].lower(), 0),
                    meta={
                        'Superpopulation name': meta['Superpopulation name'],
                        'Population name': meta['Population name'],
                        'Population description': meta['Population description'],
                    },
                )
                for cpg_id, meta in meta_by_cpg_id.items()
            }
            print(f'Number of participants to update: {len(participant_update_models)}')
            for i, (ext_id, model) in enumerate(participant_update_models.items()):
                print(f'Participant #{i}: {ext_id}')
                api_response = papi.update_participant(pid_map[ext_id], model)
                print(api_response)

    # Samples
    if input('Update samples? (y/n): ').lower() == 'y':
        sample_update_models = {
            cpg_id: SampleUpdateModel(
                meta={
                    'Population name': None,
                    'Superpopulation name': None,
                    'Population description': None,
                    'Sex': None,
                    'subpop': None,
                    'subcontinental_pop': None,
                    'sex': None,
                    'continental_pop': None,
                },
            )
            for cpg_id, meta in meta_by_cpg_id.items()
        }
        print(f'Number of samples to update: {len(sample_update_models)}')
        for i, (cpg_id, model) in enumerate(sample_update_models.items()):
            print(f'Sample #{i}: {cpg_id}')
            api_response = sapi.update_sample(cpg_id, model)
            print(api_response)
