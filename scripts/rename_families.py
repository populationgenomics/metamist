import json
import logging
import subprocess

import click

from sample_metadata.apis import FamilyApi
from sample_metadata.models import FamilyUpdateModel


logging.basicConfig(level=logging.DEBUG)


@click.command()
@click.option('--project', type=str, required=True)
@click.option('--skip-missing', is_flag=True)
@click.option('--confirm', is_flag=True)
@click.argument('rename_map', type=str, required=True)
def main(project: str, rename_map: str, skip_missing=False, confirm=False):
    """Rename Family IDs"""
    if rename_map.startswith('{'):
        old_to_new_eid = json.loads(rename_map)
    elif rename_map.startswith('gs://'):
        old_to_new_eid = json.loads(
            subprocess.check_output(['gsutil', 'cat', rename_map]).decode().strip()
        )
    else:
        with open(rename_map) as f:
            old_to_new_eid = json.load(f)

    logging.info(f'Updating {len(old_to_new_eid)} families')
    fapi = FamilyApi()
    families = fapi.get_families(project=project)

    eid_to_family_id_map = {f['external_id']: f['id'] for f in families}
    missing_eids = ', '.join(
        old_eid for old_eid in old_to_new_eid if old_eid not in eid_to_family_id_map
    )

    if missing_eids:
        if skip_missing:
            logging.info(f'Skipping family IDs with missing EIDs: {missing_eids}')
        else:
            raise ValueError(
                f'There were external family IDs {missing_eids} NOT found in project: {project}'
            )

    family_id_to_new_eid = {
        eid_to_family_id_map[old_eid]: new_eid
        for old_eid, new_eid in old_to_new_eid.items()
        if old_eid in eid_to_family_id_map
    }

    message = f'Updating {len(family_id_to_new_eid)} families with map: {json.dumps(family_id_to_new_eid)}'
    logging.info(message)
    if confirm:
        if str(input('Continue (y/n)? ')) not in ('y', '1'):
            raise SystemExit('Cancelled')

    for fid, new_eid in family_id_to_new_eid.items():
        fapi.update_family(FamilyUpdateModel(id=fid, external_id=new_eid))
        logging.debug(f'Updated {fid}')

    logging.info(f'Updated {len(family_id_to_new_eid)} families successfully')


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
