"""
Small script to update external family IDs
"""
import json
import click
from sample_metadata.apis import FamilyApi
from sample_metadata.model.family_update_model import FamilyUpdateModel


@click.command()
@click.option(
    '--family-id-json', help='json map (string) of {old_external: new_external}'
)
@click.option('--project')
def main(family_id_json: str, project: str):
    """Rename family external IDs with map {old_external: new_external}"""
    fid_map = json.loads(family_id_json)
    fapi = FamilyApi()
    families = fapi.get_families(project=project)
    internal_fid_map = {f['external_id']: f['id'] for f in families}
    update_map = {
        internal_fid_map[prev_id]: new_id
        for prev_id, new_id in fid_map.items()
        if prev_id in internal_fid_map
    }
    print('Updating with', update_map)
    new_map = list(update_map.items())
    for fid, new_eid in new_map:
        print(f'Updating {fid}: {new_eid}')
        fapi.update_family(FamilyUpdateModel(id=fid, external_id=new_eid))
