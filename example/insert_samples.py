from models.enums.sample import SampleType
from models.models.sample import Sample
from db.python.sample import SampleTable


if __name__ == '__main__':
    sample_data = [
        Sample(
            id_=None,
            external_id=f'CPG_T00{i}',
            participant_id=None,
            active=True,
            sample_meta={'FluidX TubeID': f'FD_T00{i}'},
            sample_type=SampleType.saliva,
        )
        for i in range(1, 6)
    ]
    sdb = SampleTable.from_project('sm_dev', None)

    new_ids = []
    with sdb.transaction():
        for sample in sample_data:
            new_ids.append(
                sdb.insert_sample(
                    external_id=sample.external_id,
                    sample_type=sample.sample_type,
                    active=sample.active,
                    participant_id=sample.participant_id,
                    sample_meta=sample.sample_meta,
                    commit=False,
                )
            )
        sdb.commit()

        print('Inserted docs with IDS: ' + ', '.join(str(i) for i in new_ids))
