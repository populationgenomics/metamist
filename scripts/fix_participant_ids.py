"""
Small script to update external participant IDs
"""
import json
import click
from sample_metadata.apis import ParticipantApi


@click.command()
@click.option(
    '--participant-id-json', help='json map (string) of {old_external: new_external}'
)
@click.option('--project')
def main(participant_id_json: str, project: str):
    """Update participant IDs by external ID with map {old_external: new_external}"""
    pid_map = json.loads(participant_id_json)
    papi = ParticipantApi()
    internal_pid_map = papi.get_participant_id_map_by_external_ids(
        project=project, request_body=list(pid_map.keys()), allow_missing=True
    )
    update_map = {
        internal_pid_map[prev_id]: new_id
        for prev_id, new_id in pid_map.items()
        if prev_id in internal_pid_map
    }

    if click.confirm(f'Updating {len(update_map)} participants: {update_map}\n'):
        new_map = {str(k): str(v) for k, v in update_map.items()}
        papi.update_many_participants(request_body=new_map)
