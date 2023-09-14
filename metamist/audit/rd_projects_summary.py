#!/usr/bin/env python3

# pylint: disable=unsubscriptable-object,too-many-nested-blocks,too-many-locals,consider-using-with

"""
This script goes through the Rare Disease datasets in Metamist, and finds all families, participants,
samples, and sequencing groups (SGs), as well as the latest completed CRAM per SG, as well as the latest
completed genome and/or exome ES-Index analysis and Joint-Call (DatasetVCF) analysis.
Entries are saved into named tuples which are then output into a CSV for further reporting/analysis.
"""

import logging
import os
import sys
from collections import defaultdict, namedtuple
from datetime import datetime, timezone

import click
from cloudpathlib import AnyPath, GSPath
from metamist.graphql import query, gql
from metamist.audit.audithelper import AuditHelper
from metamist.apis import ProjectApi

proj_api = ProjectApi()
RD_DATASETS = [dataset for dataset in proj_api.get_my_projects() if 'test' not in dataset and 'training' not in dataset]

CSV_FIELDS = [
    'Dataset',
    'Family_ID',
    'Family_ext_ID',
    'Participant_ID',
    'Participant_ext_ID',
    'Sample_ID',
    'Sample_ext_ID',
    'SG_ID',
    'Sequence_Type',
    'Completed_CRAM',
    'CRAM_Path',
    'CRAM_Sequence_Type',
    'CRAM_TimeStamp',
    'In_Latest_ES_Index',
    'ES_Index_ID',
    'ES_Index_Name',
    'ES_Index_TimeStamp',
    'In_Latest_Joint_Call',
    'Joint_Call_ID',
    'Joint_Call_Output',
    'Joint_Call_TimeStamp',
    'Stripy_Report',
    'Mito_Report',
]

ES_INDEXES_QUERY = gql(
    """
        query DatasetData($datasetName: String!) {
            project(name: $datasetName) {
                analyses(status: {eq: COMPLETED}, type: {eq: "ES-INDEX"}) {
                  meta
                  output
                  timestampCompleted
                  id
                }
            }
        }
    """
)

JOINT_CALLS_QUERY = gql(
    """
        query DatasetData($datasetName: String!) {
            project(name: $datasetName) {
                analyses(status: {eq: COMPLETED}, type: {eq: "CUSTOM"}) {
                  meta
                  output
                  timestampCompleted
                  id
                }
            }
        }
    """
)

WEB_REPORTS_QUERY = gql(
    """
        query DatasetData($datasetName: String!) {
            project(name: $datasetName) {
                sequencingGroups {
                    id
                    type
                    analyses(status: {eq: COMPLETED}, type: {eq: "WEB"}) {
                        id
                        meta
                        output
                        timestampCompleted
                    }
                }
            }
        }
    """
)


TODAY = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d')

summary_row = namedtuple('Summary_Row', CSV_FIELDS)


def get_crams(datasets: list[str]):
    """
    Iterates through each dataset, collecting families, participants,
    samples, sample_sequence type, and the latest completed CRAM for each sample
    """
    _query = gql(
        """
            query DatasetData($datasetName: String!) {
                project(name: $datasetName) {
                    families {
                        id
                        externalId
                        participants {
                            externalId
                            id
                            samples {
                                id
                                externalId
                                sequencingGroups {
                                    id
                                    type
                                    analyses(status: {eq: COMPLETED}, type: {eq: "CRAM"}) {
                                        id
                                        meta
                                        output
                                        timestampCompleted
                                    }
                                }
                            }
                        }
                    }
                }
            }
        """
    )

    family_intext_id_map = {}
    participant_intext_id_map = {}
    sample_intext_id_map = {}
    sg_type_map = {}  # Sequencing type map for sequencing group entries

    datasets_crams = []
    for dataset in datasets:
        families = query(_query, {'datasetName': dataset})['project']['families']
        dataset_families = defaultdict(list)

        for family in families:
            family_intext_id_map.update({family['id']: family['externalId']})
            family_participants = defaultdict(list)

            for participant in family['participants']:
                participant_intext_id_map.update(
                    {participant['id']: participant['externalId']}
                )
                participant_samples = defaultdict(list)

                for sample in participant['samples']:
                    sample_intext_id_map.update({sample['id']: sample['externalId']})
                    sample_sgs = defaultdict(list)

                    if not sample['sequencingGroups']:
                        sample_sgs[sample['id']] = None

                    for sg in sample['sequencingGroups']:
                        sg_type_map[sg['id']] = sg['type']
                        sg_crams = {}

                        crams = [
                            cram
                            for cram in sg['analyses']
                            if 'size' in cram['meta']
                            and 'sequencing_type' in cram['meta']
                        ]
                        # crams = [
                        #     cram
                        #     for cram in sg['analyses']
                        #     if 'sequencing_type' not in cram['meta']
                        #     or 'size' not in cram['meta']
                        # ]

                        sg_crams[sg['id']] = get_latest_analysis(crams)
                        sample_sgs[sample['id']].append(sg_crams)

                    participant_samples[participant['id']].append(sample_sgs)

                family_participants[family['id']].append(participant_samples)

            dataset_families[dataset].append(family_participants)

        datasets_crams.append(dataset_families)

    intext_id_maps = {
        'family': family_intext_id_map,
        'participant': participant_intext_id_map,
        'sample': sample_intext_id_map,
        'sg_sequence_type': sg_type_map,
    }

    return datasets_crams, intext_id_maps


def get_web_reports(datasets: list[str]):
    """Find all completed Stripy and MitoReport entries across the list of datasets"""
    sg_web_reports = {}
    for dataset in datasets:
        sgs = query(WEB_REPORTS_QUERY, {'datasetName': dataset})['project'][
            'sequencingGroups'
        ]
        for sg in sgs:
            stripy_report = False
            mito_report = False

            sg_id = sg['id']
            sg['type']
            web_analyses = sg['analyses']
            if not web_analyses:
                sg_web_reports[sg_id] = None
                continue

            stripy_reports = [
                report
                for report in web_analyses
                if 'stripy' in report['meta'].get('stage', 'NA').lower()
            ]
            mito_reports = [
                report
                for report in web_analyses
                if 'mitoreport' in report['meta'].get('stage', 'NA').lower()
            ]

            if stripy_reports:
                stripy_report = True
            if mito_reports:
                mito_report = True

            sg_web_reports[sg_id] = {
                'stripy_report': stripy_report,
                'mito_report': mito_report,
            }

    return sg_web_reports


def query_analyses_for_latest_by_meta_type(
    datasets: list[str], query_str, meta_type: str
):
    """
    Executes a gql query for every dataset in the list, filtering analyses
    based on the 'type' subfield in the 'meta' field. Returns the latest
    analysis for genomes and exomes per dataset.
    """
    dataset_genome_analyses = {}
    dataset_exome_analysis = {}
    for dataset in datasets:
        analyses = query(query_str, {'datasetName': dataset})['project']['analyses']
        if meta_type:
            analyses = [
                analysis
                for analysis in analyses
                if analysis['meta'].get('type') == meta_type
            ]

        (
            latest_genome_analysis,
            latest_exome_analysis,
        ) = get_latest_analysis_by_sequencing_type(analyses)

        dataset_genome_analyses.update({dataset: latest_genome_analysis})
        dataset_exome_analysis.update({dataset: latest_exome_analysis})

    return {'genome': dataset_genome_analyses, 'exome': dataset_exome_analysis}


def get_latest_analysis_by_sequencing_type(analyses: list[dict]):
    """Get the latest genome and exome analysis from the list of analyses"""
    genome_analyses = [
        analysis
        for analysis in analyses
        if analysis['meta']['sequencing_type'] == 'genome'
    ]
    exome_analyses = [
        analysis
        for analysis in analyses
        if analysis['meta']['sequencing_type'] == 'exome'
    ]
    latest_genome_analysis = get_latest_analysis(genome_analyses)
    latest_exome_analysis = get_latest_analysis(exome_analyses)

    return latest_genome_analysis, latest_exome_analysis


def get_latest_analysis(analyses: list[dict]):
    """Sorts completed analyses by timestamp and returns the latest one"""
    if not analyses:
        return {}
    return sorted(
        analyses,
        key=lambda analysis: datetime.strptime(
            analysis['timestampCompleted'], '%Y-%m-%dT%H:%M:%S'
        ),
    )[-1]


def get_analysis_samples_or_sgs(analysis):
    """
    Gets the list of samples OR sequencing_group IDs associated with the analysis
    """
    try:
        return analysis['meta']['samples']
    except KeyError:
        try:
            return analysis['meta']['sequencing_groups']
        except KeyError:
            logging.warning('INCOMPLETE ANALYSIS ENTRY - META FIELD ERROR')
            logging.warning(analysis)
            return []


def create_summary_csv_rows(
    datasets_crams: list[dict],
    dataset_es_indexes: dict[str, dict],
    dataset_joint_calls: dict[str, dict],
    web_reports: dict[str, dict[str, bool]],
    intext_id_maps: dict[str, dict],
):
    """
    Build the rows to go into the summary CSV, based on each sample and its CRAM, the latest
    ES-Index and joint call for the dataset/sequence type.

    Iterate hierarchically through each dataset, family, participant, sample, and sequencing group.
    Use the internal-external ID maps to fill in the various fields.

    """

    # Unpack the dictionaries
    dataset_genome_es_indexes = dataset_es_indexes['genome']
    dataset_genome_joint_calls = dataset_joint_calls['genome']
    dataset_exome_es_indexes = dataset_es_indexes['exome']
    dataset_exome_joint_calls = dataset_joint_calls['exome']

    family_intext_id_map = intext_id_maps['family']
    participant_intext_id_map = intext_id_maps['participant']
    sample_intext_id_map = intext_id_maps['sample']
    sg_type_map = intext_id_maps['sg_sequence_type']

    csv_rows = []
    for dataset_crams in datasets_crams:
        if not dataset_crams:
            continue

        dataset = list(dataset_crams.keys())[0]

        dataset_latest_genome_index = dataset_genome_es_indexes.get(dataset)
        if dataset_latest_genome_index:
            latest_genome_index_sgs = get_analysis_samples_or_sgs(
                dataset_latest_genome_index
            )

        dataset_latest_exome_index = dataset_exome_es_indexes.get(dataset)
        if dataset_latest_exome_index:
            latest_exome_index_sgs = get_analysis_samples_or_sgs(
                dataset_latest_exome_index
            )

        dataset_latest_genome_jc = dataset_genome_joint_calls.get(dataset)
        if dataset_latest_genome_jc:
            latest_genome_jc_sgs = get_analysis_samples_or_sgs(dataset_latest_genome_jc)

        dataset_latest_exome_jc = dataset_exome_joint_calls.get(dataset)
        if dataset_latest_exome_jc:
            latest_exome_jc_sgs = get_analysis_samples_or_sgs(dataset_latest_exome_jc)

        for family in dataset_crams[dataset]:
            if not family:
                continue
            family_id = list(family.keys())[0]
            if not family[family_id]:
                continue
            family_ext_id = family_intext_id_map[family_id]

            for participant in family[family_id]:
                try:
                    participant_id = list(participant.keys())[0]
                except IndexError:
                    continue
                if not participant[participant_id]:
                    continue
                participant_ext_id = participant_intext_id_map[participant_id]

                for sample in participant[participant_id]:
                    if not sample:
                        continue
                    sample_id = list(sample.keys())[0]
                    sample_ext_id = sample_intext_id_map[sample_id]
                    if not sample[sample_id]:
                        continue

                    for sg_crams in sample[sample_id]:
                        sg_id = list(sg_crams.keys())[0]
                        sg_seq_type = sg_type_map[sg_id].upper()

                        completed_cram = False
                        in_latest_index = False
                        in_latest_jc = False

                        if not sg_crams[sg_id]:  # When there are no CRAMs for a SG
                            s = summary_row(
                                dataset,
                                family_id,
                                family_ext_id,
                                participant_id,
                                participant_ext_id,
                                sample_id,
                                sample_ext_id,
                                sg_id,
                                sg_seq_type,
                                completed_cram,
                                '',  # CRAM output
                                '',  # CRAM seq type
                                '',  # CRAM timestamp
                                in_latest_index,
                                '',  # Latest index ID
                                '',  # Latest index name
                                '',  # Latest index timestamp
                                in_latest_jc,
                                '',  # Latest jc ID
                                '',  # Latest jc output
                                '',  # Latest jc timestamp
                                '',  # Stripy Report
                                '',  # Mito Report
                            )
                            csv_rows.append(s)
                            continue

                        completed_cram = True
                        cram_path = sg_crams[sg_id]['output']
                        cram_seq_type = sg_crams[sg_id]['meta']['sequencing_type']
                        cram_timestamp = sg_crams[sg_id]['timestampCompleted']

                        if web_reports.get(sg_id):
                            if web_reports.get(sg_id).get('stripy_report'):
                                if sg_seq_type == 'GENOME':
                                    stripy_link = os.path.join(
                                        'https://main-web.populationgenomics.org.au',
                                        dataset,
                                        'stripy',
                                        f'{sg_id}.stripy.html',
                                    )
                                else:
                                    stripy_link = ''
                            else:
                                stripy_link = ''
                        else:
                            stripy_link = ''

                        if web_reports.get(sg_id):
                            if web_reports.get(sg_id).get('mito_report'):
                                mito_link = os.path.join(
                                    'https://main-web.populationgenomics.org.au',
                                    dataset,
                                    'mito',
                                    f'mitoreport-{sg_id}',
                                    'index.html',
                                )
                            else:
                                mito_link = ''
                        else:
                            mito_link = ''

                        latest_index_id = ''
                        latest_index_output = ''
                        latest_index_timestamp = ''

                        latest_jc_id = ''
                        latest_jc_output = ''
                        latest_jc_timestamp = ''

                        if sg_seq_type == 'GENOME':
                            if (
                                dataset_latest_genome_index
                                and sg_id in latest_genome_index_sgs
                            ):
                                in_latest_index = True
                                latest_index_id = dataset_latest_genome_index['id']
                                latest_index_output = dataset_latest_genome_index[
                                    'output'
                                ]
                                latest_index_timestamp = dataset_latest_genome_index[
                                    'timestampCompleted'
                                ]
                            if (
                                dataset_latest_genome_jc
                                and sg_id in latest_genome_jc_sgs
                            ):
                                in_latest_jc = True
                                latest_jc_id = dataset_latest_genome_jc['id']
                                latest_jc_output = dataset_latest_genome_jc['output']
                                latest_jc_timestamp = dataset_latest_genome_jc[
                                    'timestampCompleted'
                                ]

                        elif sg_seq_type == 'EXOME':
                            if (
                                dataset_latest_exome_index
                                and sg_id in latest_exome_index_sgs
                            ):
                                in_latest_index = True
                                latest_index_id = dataset_latest_exome_index['id']
                                latest_index_output = dataset_latest_exome_index[
                                    'output'
                                ]
                                latest_index_timestamp = dataset_latest_exome_index[
                                    'timestampCompleted'
                                ]
                            if dataset_latest_exome_jc and sg_id in latest_exome_jc_sgs:
                                in_latest_jc = True
                                latest_jc_id = dataset_latest_exome_jc['id']
                                latest_jc_output = dataset_latest_exome_jc['output']
                                latest_jc_timestamp = dataset_latest_exome_jc[
                                    'timestampCompleted'
                                ]

                        s = summary_row(
                            dataset,
                            family_id,
                            family_ext_id,
                            participant_id,
                            participant_ext_id,
                            sample_id,
                            sample_ext_id,
                            sg_id,
                            sg_seq_type,
                            completed_cram,
                            cram_path,
                            cram_seq_type,
                            cram_timestamp,
                            in_latest_index,
                            latest_index_id,
                            latest_index_output,
                            latest_index_timestamp,
                            in_latest_jc,
                            latest_jc_id,
                            latest_jc_output,
                            latest_jc_timestamp,
                            stripy_link,
                            mito_link,
                        )
                        csv_rows.append(s)

    return csv_rows


@click.command()
@click.option('--output-path', '-o', help='Output CSV location')
@click.option('--datasets', '-d', multiple=True, default=RD_DATASETS)
def main(output_path, datasets):
    """
    Performs the audit over the list of specified datasets (default: all rare disease datasets)
    Outputs the results in a CSV with file suffix containing the run data in yyyy-mm-dd format,
    at path specified by the --output-path option.
    """

    # Validate datasets are a subset of all rare disease datasets
    if any(dataset not in RD_DATASETS for dataset in datasets):
        invalid_datasets = list(filter(lambda ds: ds not in RD_DATASETS, datasets))
        logging.warning(f'Dataset(s) outside of scope detected: {invalid_datasets}')
        sys.exit()

    # Validate write access by creating the output csv - GCP or local
    if isinstance(AnyPath(output_path), GSPath):
        output_file = output_path.joinpath(f'RD_Datasets_Summary_{TODAY}.csv')
        output_file.touch()
    else:
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        output_file = os.path.join(output_path, f'RD_Datasets_Summary_{TODAY}.csv')
        open(output_file, 'a').close()

    datasets_crams, intext_id_maps = get_crams(datasets=datasets)
    es_indexes = query_analyses_for_latest_by_meta_type(
        datasets=datasets, query_str=ES_INDEXES_QUERY, meta_type=''
    )
    joint_calls = query_analyses_for_latest_by_meta_type(
        datasets=datasets,
        query_str=JOINT_CALLS_QUERY,
        meta_type='annotated-dataset-callset',
    )
    web_reports = get_web_reports(datasets=datasets)

    summary_rows = create_summary_csv_rows(
        datasets_crams,
        es_indexes,
        joint_calls,
        web_reports,
        intext_id_maps,
    )

    AuditHelper.write_csv_report_to_cloud(summary_rows, output_file, CSV_FIELDS)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
