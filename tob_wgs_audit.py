from collections import defaultdict, namedtuple
import csv
import asyncio
from datetime import datetime
import pandas as pd
from typing import Dict, Any
from metamist.audit.non_async_auditor import GenericAuditor
from metamist.audit.audithelper import AuditHelper
import logging
from typing import Any
from functools import lru_cache
from os.path import basename, dirname, join
from google.cloud import storage
from scripts.cache_bucket_files import check_gs_exists_path, get_contents_of_gs_path


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

    def check_assay_meta_fields(self, sg_id: str, assay: Dict) -> Dict[str, str]:
        """
        check if 'type' of sg is present (and if it is in the sequencing types list?)
        if reads isnt a dictionary, add the sg to the erroneous assays dictionary
        if reads_type field isn't present and also see what do to with reads_type field
        if the meta field doesn't exist, add the sg to the erroneous assays dictionary
        if assay doesn't have 'gvcf' key, then does it have something else?
        check all file locations and if they exist in the meta field, add the sg to the erroneous assays dictionary
            gvcf locations needs checking
        """
        assay_id = assay['id']
        erroneous_assay_meta_fields = defaultdict(dict)
        erroneous_assay_meta_fields[assay_id]['meta field present'] = (
            '0' if assay.get('meta') is None else '1'
        )
        erroneous_assay_meta_fields[assay_id]['incorrect reads field'] = (
            '1' if isinstance(assay['meta'].get('reads'), dict) else '0'
        )
        reads = assay['meta'].get('reads')
        reads_type = (
            '1'
            if isinstance(reads, dict)
            and assay['meta']['reads_type'] in basename(reads).split('.')
            else '0'
            if isinstance(reads, dict)
            else 'N/A'
        )
        erroneous_assay_meta_fields[assay_id][
            'reads_type matches file type'
        ] = reads_type
        # check gvcf field exists
        gvcf = assay['meta'].get('gvcf')
        gvcf_exists = (
            '1'
            if gvcf and self.check_gcs_file_exists(gvcf['location'])  # gvcf exists
            else '0'  # gvcf does not exist
            if gvcf
            else 'N/A'  # no gvcf field
        )
        erroneous_assay_meta_fields[assay_id]['gvcf file exists'] = gvcf_exists
        # check 'secondary_files' field exists
        # TODO: figure out if there's other fields that should be checked here
        if gvcf:
            secondary_files = assay['meta'].get('gvcf').get('secondaryFiles')
            secondary_files_exists = (
                '1'
                if secondary_files and isinstance(secondary_files, list)
                else '0'
                if secondary_files
                else 'N/A'
            )
            erroneous_assay_meta_fields[assay_id][
                'secondaryFiles field exists'
            ] = secondary_files_exists
        return erroneous_assay_meta_fields

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
        erroneous_assays = defaultdict(dict)
        for sample_id, sgs in sample_sgs.items():
            for sg in sgs:
                if sg['type'].lower() not in self.sequencing_types:
                    continue
                sg_sample_id_map[sg['id']] = sample_id
                erroneous_assay_meta_fields = []
                for assay in sg['assays']:
                    reads = assay['meta'].get('reads')
                    if not reads or not isinstance(reads, dict):
                        erroneous_assay_meta_fields.append(
                            self.check_assay_meta_fields(sg['id'], assay)
                        )
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
                            f'{self.dataset} :: Got {type(reads)} reads for SG {sg["id"]}, expected list type. Reads may have been deleted after analysis'
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
                if erroneous_assay_meta_fields:
                    erroneous_assays[sg['id']] = erroneous_assay_meta_fields

        return (
            sg_sample_id_map,
            assay_sg_id_map,
            assay_filepaths_filesizes,
            # md5_check_list,
            erroneous_assays,
        )

    def map_bone_marrow_sgid_to_assay(
        self, participants: list[dict], sg_cram_paths: dict
    ):
        # get the bone marrow samples from the participant list
        bone_marrow_sg_assay: Dict = {}
        for participant in participants:
            for sample in participant['samples']:
                sgs = sample['sequencingGroups']
                for sg in sgs:
                    if sg['id'] not in sg_cram_paths.keys():
                        continue
                    if sg['id'] in sg_cram_paths.keys():
                        for assay in sg['assays']:
                            meta = assay.get('meta')
                            if not meta:
                                logging.warning(
                                    f'{self.dataset} :: Assay {assay["id"]} has no meta field'
                                )
                                continue
                            if meta.get('Primary study') == 'Pilot/bone marrow':
                                bone_marrow_sg_assay[sg['id']] = assay['id']
        return bone_marrow_sg_assay

    @staticmethod
    def get_most_recent_analyses_by_sg(
        dataset: str, analyses_list: list[dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        """
        Takes a list of completed analyses for a number of sequencing groups and returns the latest
        completed analysis for each sequencing group, creating a 1:1 mapping of SG to analysis.
        """
        most_recent_analysis_by_sg = {}
        most_recent_long_read_analysis_by_sg = {}
        sg_ids_with_no_analyses = []
        for sg_analyses in analyses_list:
            sg_id = sg_analyses['id']
            analyses = sg_analyses['analyses']
            # Filter analyses to consider only those with a .cram extension in the output field
            cram_analyses = [
                analysis
                for analysis in analyses
                if analysis['output'].endswith('.cram')
            ]

            long_read_analyses = [
                analysis
                for analysis in analyses
                if 'ont' in analysis['meta'].get('sequence_type')
            ]

            if not cram_analyses:
                sg_ids_with_no_analyses.append(sg_id)
                logging.warning(f'{dataset} :: SG {sg_id} has no completed analyses')
                continue

            if len(cram_analyses) == 1:
                most_recent_analysis_by_sg[sg_id] = cram_analyses[0]
                continue

            sorted_cram_analyses = sorted(
                cram_analyses,
                key=lambda x: datetime.strptime(
                    x['timestampCompleted'], '%Y-%m-%dT%H:%M:%S'
                ),
            )

            sorted_long_read_analyses = sorted(
                long_read_analyses,
                key=lambda x: datetime.strptime(
                    x['timestampCompleted'], '%Y-%m-%dT%H:%M:%S'
                ),
            )
            most_recent_analysis_by_sg[sg_id] = sorted_cram_analyses[-1]
            if sorted_long_read_analyses:
                most_recent_long_read_analysis_by_sg[sg_id] = sorted_long_read_analyses[
                    -1
                ]
            # log long read analyses
            if len(most_recent_long_read_analysis_by_sg) > 0:
                logging.warning(
                    f'{dataset} :: SG {sg_id} has multiple long read analyses: {most_recent_long_read_analysis_by_sg}'
                )

        return (
            most_recent_analysis_by_sg,
            most_recent_long_read_analysis_by_sg,
            sg_ids_with_no_analyses,
        )

    @lru_cache
    def get_contents_of_gs_path(
        self, gcs_client: storage, filepath: str, bucket_name: str, prefix: str
    ) -> set[str]:
        assert filepath.startswith('gs://')

        # create gsclient and list contents of blob at path
        # need to supply a user project here if requester-pays bucket
        blobs = gcs_client.list_blobs(bucket_name, prefix=prefix)
        blob_set = {blob.name for blob in blobs}
        return blob_set

    def check_gcs_file_exists(self, filepath: str) -> list[bool]:
        """Check if files exist in GCS"""
        gcs_client = self.gcs_client
        filepath_parts = (
            dirname(filepath).removeprefix('gs://').strip('/').split('/', maxsplit=1)
        )
        if len(filepath_parts) == 1:  # files in root of bucket
            bucket_name, prefix = filepath_parts[0], ''
        else:
            bucket_name, prefix = filepath_parts
        return (prefix + '/' + basename(filepath)) in self.get_contents_of_gs_path(
            gcs_client, dirname(filepath), bucket_name, prefix
        )

    def check_analysis_fields(self, analyses_list: list[dict]):
        """
        Find analyses that are out of date/point to files that don't exist
        dictionary of issues with analyses fields:
            erroneous_analyses{
                sg_id : {
                    meta field present : bool,
                    duplicate 'sequencing_type' and 'sequence_type' fields : bool,
                    no project field : bool,
                    file exists : bool,
                    sequence_type matches file type : bool,
                    no analysis type : bool,
                    if multiqc check if files exist (bool) (not checking metrics),
                }
            }
        """
        erroneous_analyses = defaultdict(dict)
        for sg_analyses in analyses_list:
            sg_id = sg_analyses['id']
            erroneous_analysis_meta_fields = []
            for analysis in sg_analyses['analyses']:
                analysis_id = analysis['id']
                analysis_meta_fields = {}
                meta_field_present = '1' if analysis['meta'] else '0'
                analysis_meta_fields[analysis_id] = {
                    'meta field present': meta_field_present
                }

                duplicate_sequence_type = (
                    '1'
                    if 'sequence_type' in analysis['meta']
                    and 'sequencing_type' in analysis['meta']
                    else '0'
                )
                analysis_meta_fields[analysis_id][
                    'duplicate sequence_type'
                ] = duplicate_sequence_type

                if analysis['output']:
                    if analysis['output'].endswith('.mt') or analysis[
                        'output'
                    ].endswith(
                        '.ht'
                    ):  # skip table/matrix-table files
                        continue
                    file_exists = (
                        '1' if self.check_gcs_file_exists(analysis['output']) else '0'
                    )
                    analysis_meta_fields[analysis_id]['file exists'] = file_exists
                    if analysis['type'] and file_exists == '1':
                        if analysis['type'] != analysis['output'].split('.')[-1]:
                            analysis_meta_fields[analysis_id][
                                'mismatched analysis type'
                            ] = '1'
                        else:
                            analysis_meta_fields[analysis_id][
                                'mismatched analysis type'
                            ] = '0'
                    elif analysis['type'] and file_exists == '0':
                        analysis_meta_fields[analysis_id][
                            'mismatched analysis type'
                        ] = 'N/A'
                    analysis_meta_fields[analysis_id]['analysis type present'] = (
                        '1' if analysis['type'] else '0'
                    )
                erroneous_analysis_meta_fields.append(analysis_meta_fields)
            erroneous_analyses[sg_id] = erroneous_analysis_meta_fields

        return erroneous_analyses

    def write_summary_files(
        self, input_dict: Dict[str, list[Dict[int, Dict[str, str]]]], meta_type: str
    ):
        """
        Write summary files for erroneous assays and analyses
        """
        today = datetime.today().strftime('%Y-%m-%d')

        header = set()
        for inner_list in input_dict.values():
            for inner_dict in inner_list:
                for value in inner_dict.values():
                    for k in value.keys():
                        header.add(k)

        header = ['sg_id', f'{meta_type}_id'] + sorted(
            header, key=lambda x: next(iter(x))
        )

        # Write to CSV
        csv_file_path = f'{meta_type}_meta_field_summary_{today}.csv'
        with open(csv_file_path, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=header)
            writer.writeheader()

            for sg_id, inner_list in input_dict.items():
                for inner_dict in inner_list:
                    meta_id = next(iter(inner_dict))
                    row = {
                        'sg_id': sg_id,
                        f'{meta_type}_id': meta_id,
                        **inner_dict[meta_id],
                    }
                    writer.writerow(row)


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
        erroneous_assays,
    ) = auditor.get_assay_map_from_participants(participant_data)

    (
        sg_cram_paths,
        long_read_analyses,
        sg_ids_with_no_analyses,
        cram_analyses,
    ) = auditor.get_analysis_cram_paths_for_dataset_sgs(assay_sg_id_map)
    complete_incomplete_crams = auditor.get_complete_and_incomplete_sgs(
        assay_sg_id_map, sg_cram_paths
    )

    # map sg's with analyses (most recent analyses) to their primary study
    bone_marrow_sg_assay = auditor.map_bone_marrow_sgid_to_assay(
        participant_data, sg_cram_paths
    )

    erroneous_analyses = auditor.check_analysis_fields(cram_analyses)

    auditor.write_summary_files(erroneous_assays, 'assay')
    auditor.write_summary_files(erroneous_analyses, 'analysis')

    return participant_data


audit_tob_wgs(
    dataset='tob-wgs',
    sequencing_types=['genome'],
    file_types=['cram'],
    default_analysis_type='cram',
    default_analysis_status='completed',
)
