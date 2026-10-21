import datetime
import json
import logging
import os
import re

import google.auth.exceptions
import google.auth.transport.requests
from cloudpathlib import AnyPath
from google.oauth2 import service_account

from .config import CREDENTIALS_FILENAME, MAP_LOCATION, SEQR_AUDIENCE
from .logging_config import logger


def get_token():
    """Get identity-token for seqr specific service-credentials"""
    credential_filename = CREDENTIALS_FILENAME
    with open(credential_filename, 'r') as f:
        info = json.load(f)
        credentials_content = (info.get('type') == 'service_account') and info or None
        credentials = service_account.IDTokenCredentials.from_service_account_info(
            credentials_content, target_audience=SEQR_AUDIENCE
        )
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        return credentials.token


def parse_participants(
    participants: dict,
) -> tuple[set[str], set[str], dict[str, list[str]]]:
    """
    Parses the participants data from GraphQL / file and returns the data required for syncing

    Returns:
        - set of family external IDs
        - set of participant external IDs
        - dict mapping participant external IDs to internal sequencing group IDs
    """
    family_eids: set[str] = set()
    participant_eids: set[str] = set()
    peid_to_sgid_map: dict[str, list[str]] = {}
    for participant in participants:
        sg_ids = {
            sg['id'] for s in participant['samples'] for sg in s['sequencingGroups']
        }
        if not sg_ids:
            # nothing more required for this participant
            continue

        peid_to_sgid_map[participant['externalId']] = list(sg_ids)
        participant_eids.add(participant['externalId'])
        family_eids |= {f['externalId'] for f in participant['families']}

    participant_eids = {p['externalId'] for p in participants}
    filtered_family_eids = {
        f['externalId'] for p in participants for f in p['families']
    }

    if not participant_eids:
        raise ValueError('No participants to sync?')
    if not filtered_family_eids:
        raise ValueError('No families to sync')

    return filtered_family_eids, participant_eids, peid_to_sgid_map


def get_peid_to_reads_map(
    sg_participants: dict[str, str], sg_crams: dict[str, str]
) -> dict[str, list[str]]:
    """Get a map of participant external IDs to their reads"""
    peid_to_read_map: dict[str, list[str]] = {}
    for sg, read_path in sg_crams.items():
        participant = sg_participants[sg]
        if participant not in peid_to_read_map:
            peid_to_read_map[participant] = []
        peid_to_read_map[participant].append(read_path)
    return peid_to_read_map


def write_sgid_to_peid_map_to_file(
    dataset: str,
    peid_to_sgid_map: dict[str, list[str]] | None = None,
    sgid_to_peid_map: dict[str, str] | None = None,
):
    """Write the sgid - peid map to a file in the MAP_LOCATION directory"""
    if not peid_to_sgid_map and not sgid_to_peid_map:
        raise ValueError('No data to write to file')

    if peid_to_sgid_map:
        rows_to_write = [
            # (ID in ES-Index, External PID in seqr)
            '\t'.join([sg, pid])
            for pid, p_sgids in peid_to_sgid_map.items()
            for sg in p_sgids
        ]
    else:
        rows_to_write = [
            # (ID in ES-Index, External PID in seqr)
            '\t'.join([sg, pid])
            for sg, pid in sgid_to_peid_map.items()
        ]

    filename = f'{dataset}_sgid_peid_map_{datetime.datetime.now().isoformat()}.tsv'
    filename = re.sub(r'[/\\?%*:|\'<>\x7F\x00-\x1F]', '-', filename)

    fn_path = os.path.join(MAP_LOCATION, filename)
    # pylint: disable=no-member
    with AnyPath(fn_path).open('w+') as f:  # type: ignore
        f.write('\n'.join(rows_to_write))
    logger.info(
        f'{dataset} :: Wrote {len(rows_to_write)} sequencing groups to {fn_path}'
    )

    return fn_path


def check_for_missing_sgs(
    dataset: str,
    sequencing_type: str,
    latest_analysis: dict,
    peid_to_sgid_map: dict[str, list[str]],
):
    """Check for missing SGs in the latest ES index and log warnings if any are found"""
    sg_ids = set(latest_analysis['sequencing_group_ids'])
    logger.info(
        f'{dataset}.{sequencing_type} :: Found {len(sg_ids)} SGs in index {latest_analysis["output"]}'
    )
    missing_sg_ids = {
        sg for sgids in peid_to_sgid_map.values() for sg in sgids
    } - sg_ids
    if missing_sg_ids:
        logging.warning(
            f'{dataset}.{sequencing_type} :: Samples missing from index {latest_analysis["output"]}: '
        )
        for peid, sgids in peid_to_sgid_map.items():
            missing_sgids = set(sgids) & missing_sg_ids
            if missing_sgids:
                logging.warning(f'\t{peid} :: {",".join(missing_sgids)}')
