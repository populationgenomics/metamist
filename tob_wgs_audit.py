from collections import defaultdict, namedtuple
import asyncio
import pandas as pd
from metamist.audit.non_async_auditor import GenericAuditor
from metamist.audit.audithelper import AuditHelper
import logging
from typing import Any


class TobWgsAuditor(GenericAuditor):
    def __init__(
        self,
        dataset: str,
        sequencing_types: str,
        file_types: str,
        default_analysis_type='cram',
        default_analysis_status='completed',
    ):
        super().__init__(
            dataset=dataset,
            sequencing_types=sequencing_types,
            file_types=file_types,
            default_analysis_type=default_analysis_type,
            default_analysis_status=default_analysis_status,
        )

    def get_assay_map_from_participants(
        self, participants: list[dict]
    ) -> tuple[dict[str, str], dict[int, str], dict[Any, list[tuple[Any, Any]]]]:
        """
        Input the list of Metamist participant dictionaries from the 'QUERY_PARTICIPANTS_SAMPLES_SGS_ASSAYS' query

        Returns the mappings:
        1. { sg_id       : sample_id }
        2. { assay_id    : sg_id }
        3. { assay_id    : (read_filepath, read_filesize,) }
        """

        sg_sample_id_map = {}
        assay_sg_id_map = {}
        assay_filepaths_filesizes = defaultdict(list)

        sample_sgs = {
            sample['id']: sample['sequencingGroups']
            for participant in participants
            for sample in participant['samples']
        }

        # md5_check_list = []

        for sample_id, sgs in sample_sgs.items():
            for sg in sgs:
                if sg['type'].lower() not in self.sequencing_types:
                    continue

                sg_sample_id_map[sg['id']] = sample_id
                for assay in sg['assays']:
                    reads = assay['meta'].get('reads')
                    print(reads)
                    if not reads:
                        logging.warning(
                            f'{self.dataset} :: SG {sg["id"]} assay {assay["id"]} has no reads field'
                        )
                        continue

                    if assay_sg_id_map.get(assay['id']):
                        raise ValueError(
                            f'{self.dataset} :: Assay {assay["id"]} has multiple SGs: {assay_sg_id_map[assay["id"]]} and {sg["id"]}'
                        )
                    assay_sg_id_map[assay['id']] = sg['id']

                    if isinstance(reads, dict):
                        assay_filepaths_filesizes[assay['id']].append(
                            (
                                reads.get('location'),
                                reads.get('size'),
                            )
                        )
                        continue

                    if not isinstance(reads, list):
                        logging.error(
                            f'{self.dataset} :: Got {type(reads)} reads for SG {sg["id"]}, expected list: list type. Reads may have been deleted after analysis'
                        )
                        continue
                    for read in reads:
                        if not isinstance(read, dict):
                            logging.error(
                                f'{self.dataset} :: Got {type(read)} read for SG {sg["id"]}, expected dict: {read}'
                            )
                            continue

                        assay_filepaths_filesizes[assay['id']].append(
                            (
                                read.get('location'),
                                read.get('size'),
                            )
                        )

                        # here implement md5 hash check
                        location = read.get('location')
                        checksum = read.get('checksum').split(' ')[0].split(':')[1]
                        # md5_check_list.append(self.check_md5_hash(location, checksum))

        return (
            sg_sample_id_map,
            assay_sg_id_map,
            assay_filepaths_filesizes,
            # md5_check_list,
        )


def audit_tob_wgs(
    dataset: str,
    sequencing_types: list[str],
    file_types: list[str],
    default_analysis_type: str,
    default_analysis_status: str,
):
    auditor = TobWgsAuditor(
        dataset=dataset,
        sequencing_types=sequencing_types,
        file_types=file_types,
        default_analysis_type=default_analysis_type,
        default_analysis_status=default_analysis_status,
    )

    participant_data = auditor.get_participant_data_for_dataset()
    sample_internal_external_id_map = auditor.map_internal_to_external_sample_ids(
        participant_data
    )
    (
        sg_sample_id_map,
        assay_sg_id_map,
        assay_filepaths_filesizes,
        # md5_check_list,
    ) = auditor.get_assay_map_from_participants(participant_data)
    return participant_data


audit_tob_wgs(
    dataset='tob-wgs',
    sequencing_types=['genome'],
    file_types=['cram'],
    default_analysis_type='cram',
    default_analysis_status='completed',
)
