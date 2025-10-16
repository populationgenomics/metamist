"""
Small script to update external participant IDs
"""

import json

import click

from metamist.apis import ParticipantApi


@click.command()
@click.option(
    '--participant-id-json', help='json map (string) of {old_external: new_external}'
)
@click.option('--project')
@click.option('--force', is_flag=True, help='Do not confirm updates')
def main(participant_id_json: str, project: str, force: bool = False):
    """Update participant IDs by external ID with map {old_external: new_external}"""
    if participant_id_json.startswith('{'):
        pid_map = json.loads(participant_id_json)
    else:
        with open(participant_id_json) as f:
            pid_map = json.load(f)

    papi = ParticipantApi()
    internal_pid_map = papi.get_participant_id_map_by_external_ids(
        project=project, request_body=list(pid_map.keys()), allow_missing=True
    )
    update_map = {
        internal_pid_map[prev_id]: new_id
        for prev_id, new_id in pid_map.items()
        if prev_id in internal_pid_map
    }

    message = f'Updating {len(update_map)} participants: {update_map}'

    if force:
        print(message)
    elif not click.confirm(message, default=False):
        raise click.Abort()

    new_map = {str(k): str(v) for k, v in update_map.items()}
    papi.update_many_participants(request_body=new_map)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
