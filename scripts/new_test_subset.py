#!/usr/bin/env python3
# pylint: disable=unsubscriptable-object,too-many-locals

"""
Versatile script for migrating production data to a test bucket & corresponding
metamist project.

Example Invocation

analysis-runner \
--dataset acute-care --description "populate acute care test subset" --output-dir "acute-care-test" \
--access-level full \
scripts/new_test_subset.py --project acute-care --families 4

This example will populate acute-care-test with the metamist data for 4 families.
"""
import logging
import subprocess
import sys
import random
from argparse import ArgumentParser
from collections import defaultdict, Counter
from itertools import chain

from cpg_utils import to_path
from metamist.apis import (
    AnalysisApi,
    AssayApi,
    SampleApi,
    FamilyApi,
    ParticipantApi,
)
from metamist.graphql import gql, query
from metamist.models import (
    AssayUpsert,
    SampleUpsert,
    Analysis,
    AnalysisStatus,
)


FAMILY_HEADINGS = ['Family ID', 'Description', 'Coded Phenotype', 'Display Name']
PED_HEADINGS = [
    '#Family ID',
    'Individual ID',
    'Paternal ID',
    'Maternal ID',
    'Sex',
    'Affected',
]
PROJECT_DATA = dict[str, dict[str, str]]

analapi = AnalysisApi()
assayapi = AssayApi()
fapi = FamilyApi()
papi = ParticipantApi()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stderr,
)


def get_prod_samples(project: str) -> PROJECT_DATA:
    """
    Get all prod SG IDs for a project using gql
    Return in form
    {
        CPG_id: {
           'sample': externalID,
           'family': familyID
        }
    }

    Args:
        project (str):

    Returns:
        dict
    """
    sample_query = gql(
        """
    query ProjectSampleQuery($project: String!) {
        project(name: $project) {
            sequencingGroups {
                id
                meta
                type
                sample {
                    externalId
                    participant {
                        families {
                            externalId
                        }
                    }
                }
            }
        }
    }"""
    )
    result = query(sample_query, variables={'project': project})
    return {
        sam['id']: {
            'sample': sam['sample']['externalId'],
            'family': sam['sample']['participant']['families'][0]['externalId'],
            'meta': sam['meta'],
        }
        for sam in result['project']['sequencingGroups']
    }


def get_fams_for_sgs(project: str, sg_ids: set[str]) -> set[str]:
    """
    Find the families that a list of samples belong to (Samples or SGID?)
    Args:
        project ():
        sg_ids (set[str]): a list of SequencingGroup IDs

    Returns:
        set of family IDs (str)
    """

    family_full = query(
        gql(
            """query MyQuery($project: String!, $samples: [String!]!) {
                project(name: $project) {
                sequencingGroups(id: {in_: $samples}, activeOnly: {eq: true}) {
                    id
                    sample {
                        participant {
                            families {
                                    externalId
                                    participants {
                                        externalId
                                    }
                                }
                            }
                        }
                    }
                }
            }"""
        ),
        variables={'samples': list(sg_ids), 'project': project},
    )

    # extract the family IDs
    fams = set()
    for sg in family_full['project']['sequencingGroups']:
        for fam in sg['sample']['participant']['families']:
            fams.add(fam['externalId'])

    return fams


def get_random_families(
    project: str, dodge_families: set[str], families_n: int, min_fam_size: int = 0
) -> set[str]:
    """
    Get a random subset of families
    Balance family selection to create an even distribution of family sizes
    Do this using the random.choices function, which allows for weighted random selection

    We pull all families, remove families we already intend to select, and sample from
    the remaining families

    Args:
        project (str): project to query for
        dodge_families (set[str]): we already want these fams, don't include in random selection
        families_n (int): number of new families to include
        min_fam_size (int): currently unused

    Returns:
        set[str] - additional families to select
    """

    # query for families in this project
    ped = query(
        gql(
            """
    query MyQuery($project: String!) {
        project(name: $project) {
            families {
                externalId
                participants {
                    externalId
                }
                }
            }
        }
    """
        ),
        variables={'project': project},
    )

    total_counter = 0
    family_counter = defaultdict(list)
    for family in ped['project']['families']:
        if (
            family['externalId'] in dodge_families
            or len(family['participants']) < min_fam_size
        ):
            continue
        total_counter += 1
        family_counter[len(family['participants'])].append(family['externalId'])

    if total_counter < families_n:
        raise ValueError(
            f'Not enough families to choose from. You explicitly requested '
            f'{len(dodge_families)} families through sample/family ID, and '
            f'Only {total_counter} other families are available, '
            f'{families_n} additional random families were requested.'
        )

    # return every other family
    if total_counter == families_n:
        return set(chain.from_iterable(family_counter.values()))

    # weighted random selection from remaining families
    weights = [len(family_counter[k]) for k in sorted(family_counter.keys())]
    family_size_choices = dict(
        Counter(
            random.choices(sorted(family_counter.keys()), weights=weights, k=families_n)
        )
    )
    return_families = set()
    for k, v in family_size_choices.items():
        return_families.add(random.sample(family_counter[k], v))

    return return_families


def main(
    project: str,
    samples_n: int | None = None,
    families_n: int | None = None,
    skip_ped: bool = True,
    additional_families: list[str] = None,
    additional_samples: list[str] = None,
    noninteractive: bool = False,
    clear_out_test: bool = False,
):
    """
    Who runs the world? MAIN()

    Args:
        project (str): name of prod project
        samples_n (int): number of samples to include
        families_n (int): number of families to include
        skip_ped (bool): skip pedigree/family information during transfer
        additional_families (list[set]): specific families to include
        additional_samples (list[set]): specific samples to include
        noninteractive (bool): skip interactive confirmation
        clear_out_test (bool): inactivate all samples in the test project
    """

    # get all prod samples for the project
    # object in form [{externalId: 'sample1', id: 'X-id'}, ...]
    sample_set = set(additional_samples) if additional_samples else set()
    family_set = set(additional_families) if additional_families else set()

    # get all prod samples for the project
    # {CPG_id: {'sample': externalID, 'family': familyID}}
    metamist_main_content = get_prod_samples(project)

    logging.info(f'Found {len(metamist_main_content)} samples')
    if (samples_n and samples_n >= len(metamist_main_content)) and not noninteractive:
        if (
            input(
                f'Requesting {samples_n} samples which is >= '
                f'than the number of available samples ({len(metamist_main_content)}). '
                f'The test project will be a copy of the production project. '
                f'Please confirm (y): '
            )
            != 'y'
        ):
            raise SystemExit()

    random.seed(42)  # for reproducibility
    if families_n:
        # if there are additional samples specified, find the corresponding families
        # these specifically requested families & samples are copied over in addition
        # to the random selection families_n number of families
        if sample_set:
            family_set |= get_fams_for_sgs(project, sample_set)

        # family_set is ones we definitely want to include
        # we need to make sure we don't include them in the random selection
        family_set |= get_random_families(project, family_set, families_n)

        # now go get the corresponding participant/SG IDs
        sample_set = pull_samples_from_families(family_set, metamist_main_content)

    elif samples_n:
        # if there are additional samples specified, find the corresponding families
        # these specifically requested families & samples are copied over in addition
        # to the random selection families_n number of families
        if family_set:
            # we already have this data in query result
            sample_set |= pull_samples_from_families(family_set, metamist_main_content)

        all_sgids = set(metamist_main_content.keys())

        # top up the selected SGIDs with random selections
        # resulting SG IDs we want to copy into the test project
        sample_set |= set(
            random.sample(all_sgids - sample_set, samples_n - len(sample_set))
        )

    # maybe we only want to copy specific samples or families...
    else:
        if sample_set:
            sample_set |= pull_samples_from_families(family_set, metamist_main_content)

    logging.info(f'Subset to {len(sample_set)} samples')

    # Populating test project
    target_project = project + '-test'
    logging.info('Checking any existing test samples in the target test project')
    metamist_test_content = get_prod_samples(target_project)
    keep, remove = process_existing_test_samples(
        metamist_test_content, sample_set, metamist_main_content, clear_out_test
    )

    if remove and clear_out_test:
        logging.info(f'Removing test samples: {remove}')
        # TODO do removal

    # don't bother actioning these guys
    new_sample_map = {}
    for s in keep:
        prod_ext = metamist_main_content[s]['sample']
        test_s = [
            k for k, v in metamist_test_content.items() if v['sample'] == prod_ext
        ]
        if test_s:
            new_sample_map[s] = test_s
        else:
            logging.error(f'Could not find test sample for {prod_ext}')

    # now ignore them forever
    sample_set -= set(keep)

    assays_by_sg = get_assays_for_sgs(project, sample_set)
    analysis_sid_type = get_latest_analyses(project, sample_set)

    # get participant objects for all SG IDs
    if skip_ped:
        logging.info('Skipping pedigree/family information')
        external_ids = transfer_participants(project, target_project, sample_set)

    else:
        logging.info('Transferring pedigree/family information')
        family_ids = transfer_families(project, target_project, sample_set)
        external_ids = transfer_pedigree(project, target_project, family_ids)

    ext_sam_int_participant_map = get_ext_sam_id_to_int_participant_map(
        project, target_project, external_ids
    )
    sapi = SampleApi()
    for sgid in sample_set:
        logging.info(f'Processing Sample {sgid}')
        full_sample = metamist_main_content[sgid]
        new_s_id = sapi.create_sample(
            project=target_project,
            sample_upsert=SampleUpsert(
                external_id=full_sample['sample'],
                type=full_sample['type'],
                meta=(copy_files_in_dict(full_sample['meta'], project) or {}),
                participant_id=ext_sam_int_participant_map[full_sample['sample']],
            ),
        )
        new_sample_map[sgid] = new_s_id

        if sg_assay := assays_by_sg.get(sgid):
            logging.info('Processing sequence entry')
            new_meta = copy_files_in_dict(sg_assay.get('meta'), project)
            logging.info('Creating sequence entry in test')
            assayapi.create_assay(
                assay_upsert=AssayUpsert(
                    sample_id=new_s_id,
                    meta=new_meta,
                    type=sg_assay['type'],
                    external_ids=sg_assay['externalIds'],
                ),
            )

        for a_type in ['cram', 'gvcf']:
            if analysis := analysis_sid_type[a_type].get(sgid):
                logging.info(f'Processing {a_type} analysis entry')
                am = Analysis(
                    type=str(a_type),
                    output=copy_files_in_dict(
                        analysis['output'],
                        project,
                        (str(sgid), new_sample_map[sgid]),
                    ),
                    status=AnalysisStatus(analysis['status']),
                    sample_ids=[new_sample_map[sgid]],
                    meta=analysis['meta'],
                )
                logging.info(f'Creating {a_type} analysis entry in test')
                analapi.create_analysis(project=target_project, analysis=am)


def copy_files_in_dict(d, dataset: str, sid_replacement: tuple[str, str] = None):
    """
    Replaces all `gs://cpg-{project}-main*/` paths
    into `gs://cpg-{project}-test*/` and creates copies if needed
    If `d` is dict or list, recursively calls this function on every element
    If `d` is str, replaces the path
    """
    if not d:
        return d
    if isinstance(d, str) and d.startswith(f'gs://cpg-{dataset}-main'):
        logging.info(f'Looking for analysis file {d}')
        old_path = d
        if not to_path(old_path).exists():
            logging.warning(f'File {old_path} does not exist')
            return d
        new_path = old_path.replace(
            f'gs://cpg-{dataset}-main', f'gs://cpg-{dataset}-test'
        )
        # Replace the internal sample ID from the original project
        # With the new internal sample ID from the test project
        if sid_replacement is not None:
            new_path = new_path.replace(sid_replacement[0], sid_replacement[1])

        if not to_path(new_path).exists():
            cmd = f'gsutil cp {old_path!r} {new_path!r}'
            logging.info(f'Copying file in metadata: {cmd}')
            subprocess.run(cmd, check=False, shell=True)
        extra_exts = ['.md5']
        if new_path.endswith('.vcf.gz'):
            extra_exts.append('.tbi')
        if new_path.endswith('.cram'):
            extra_exts.append('.crai')
        for ext in extra_exts:
            if (
                to_path(old_path + ext).exists()
                and not to_path(new_path + ext).exists()
            ):
                cmd = f'gsutil cp {old_path + ext!r} {new_path + ext!r}'
                logging.info(f'Copying extra file in metadata: {cmd}')
                subprocess.run(cmd, check=False, shell=True)
        return new_path
    if isinstance(d, list):
        return [copy_files_in_dict(x, dataset) for x in d]
    if isinstance(d, dict):
        return {k: copy_files_in_dict(v, dataset) for k, v in d.items()}
    return d


def get_ext_sam_id_to_int_participant_map(
    project: str, target_project: str, external_ids: list[str]
) -> dict[str, str]:
    """
    I hate this method so much. It may have taken me an hour to grok
    - find the participant IDs for the external IDs in the test project
    - find the corresponding sample and participant IDs in test
    - map each prod sample ext ID<->internal test participant ID (int)

    Args:
        project (str):
        target_project (str): test version of the same project
        external_ids ():

    Returns:
        a mapping of all prod sample IDs to test participant IDs
    """

    # in test project, find participant ext.& internal IDs
    test_ep_ip_map = query(
        gql(
            """
            query MyQuery($project: String!) {
                project(name: $project) {
                    participants {
                        externalId
                        id
                    }
                }
            }"""
        ),
        variables={'project': target_project},
    )
    test_ext_to_int_pid = {
        party['externalId']: party['id']
        for party in test_ep_ip_map['project']['participants']
        if party['externalId'] in external_ids
    }
    prod_map = query(
        gql(
            """
            query MyQuery($project: String!) {
                project(name: $project) {
                    participants {
                        externalId
                        id
                        samples {
                            externalId
                        }
                    }
                }
            }"""
        ),
        variables={'project': project},
    )

    ext_sam_int_participant_map = {}
    for party in prod_map['project']['participants']:
        if party['externalId'] in test_ext_to_int_pid:
            for sample in party['samples']:
                ext_sam_int_participant_map[sample['externalId']] = test_ext_to_int_pid[
                    party['externalId']
                ]

    return ext_sam_int_participant_map


def transfer_pedigree(initial_project, target_project, family_ids):
    """
    Pull relevant pedigree information from the input project, and copy to target_project
    Args:
        initial_project ():
        target_project ():
        family_ids ():

    Returns:
        a list of all relevant family IDs
    """

    pedigree = query(
        gql(
            """
            query MyQuery($project: String!, $families: [String!]!) {
                project(name: $project) {
                   pedigree(internalFamilyIds: $families)
                }
            }"""
        ),
        variables={'families': list(family_ids), 'project': initial_project},
    )
    ext_ids = []
    ped_lines = ['\t'.join(PED_HEADINGS)]
    for line in pedigree['project']['pedigree']:
        ext_ids.append(line['individual_id'])
        ped_lines.append(
            '\t'.join(
                [
                    line['family_id'],
                    line['individual_id'],
                    line['paternal_id'] or '0',
                    line['maternal_id'] or '0',
                    line['sex'],
                    line['affected'],
                ]
            )
        )

    tmp_ped_file = 'tmp_ped.tsv'
    with open(tmp_ped_file, 'wt') as tmp_ped:
        tmp_ped.write('\n'.join(ped_lines))

    with open(tmp_ped_file) as ped_handle:
        fapi.import_pedigree(
            file=ped_handle,
            has_header=True,
            project=target_project,
            create_missing_participants=True,
        )
    return ext_ids


def transfer_families(
    initial_project: str, target_project: str, participant_ids: set[int]
) -> list[int]:
    """
    Pull relevant families from the input project, and copy to target_project

    Args:
        initial_project ():
        target_project ():
        participant_ids ():

    Returns:

    """

    families = query(
        gql(
            """
            query MyQuery($project: String!, $samples: [String!]!) {
                project(name: $project) {
                    sequencingGroups(id: {in_: $samples}) {
                        sample {
                            participant {
                                families {
                                    codedPhenotype
                                    description
                                    externalId
                                    id
                                }
                            }
                        }
                    }
                }
            }"""
        ),
        variables={'samples': list(participant_ids), 'project': initial_project},
    )

    family_lines = set()
    family_ids = []
    for sg in families['project']['sequencingGroups']:
        for family in sg['sample']['participant']['families']:
            family_ids.append(family['id'])
            family_lines.add(
                '\t'.join(
                    [
                        family['externalId'],
                        family['description'] or '',
                        family['codedPhenotype'] or '',
                        '',  # never any display name?
                    ]
                )
            )

    tmp_family_tsv = 'tmp_families.tsv'
    family_headers = '\t'.join(FAMILY_HEADINGS)
    with open(tmp_family_tsv, 'wt') as tmp_families:
        tmp_families.write(family_headers + '\n')
        for family_line in family_lines:
            tmp_families.write(family_line + '\n')

    with open(tmp_family_tsv) as family_file:
        fapi.import_families(file=family_file, project=target_project)

    return family_ids


def transfer_participants(
    initial_project: str, target_project: str, sg_ids: set[int]
) -> list[str]:
    """
    Transfers relevant participants between projects
    Args:
        initial_project ():
        target_project ():
        sg_ids ():

    Returns:
        list of all upserted participant IDs
    """
    participants = query(
        gql(
            """
            query MyQuery($project: String!, $samples: [String!]!) {
                project(name: $project) {
                    sequencingGroups(id: {in_: $samples}) {
                        sample {
                            participant {
                                id
                                externalId
                                karyotype
                                meta
                                reportedGender
                                reportedSex
                            }
                        }
                    }
                }
            }
            """
        ),
        variables={'samples': list(sg_ids), 'project': initial_project},
    )

    # sgs were already pre-filtered to remove those in test
    participants_to_transfer = [
        sg['sample']['participant']
        for sg in participants['project']['sequencingGroups']
    ]

    upserted_participants = papi.upsert_participants(
        target_project, participant_upsert=participants_to_transfer
    )
    return list(upserted_participants.keys())


def get_latest_analyses(project: str, sg_ids: set[str]) -> dict:
    """
    Query for the latest CRAM & GVCF analyses for a list of SG IDs
    Args:
        project ():
        sg_ids ():

    Returns:
        a dictionary of latest analyses, keyed by type & SG ID
    """

    results = query(
        gql(
            """
                query MyQuery($project: String!, $samples: [String!]!) {
                    project(name: $project) {
                        sequencingGroups(id: {in_: $samples}) {
                            id
                            analyses(
                                type: {in_: ["cram", "gvcf"]}
                                active: {eq: true}
                                status: {eq: COMPLETED}
                            ) {
                                id
                                meta
                                output
                                type
                                timestampCompleted
                            }
                        }
                    }
                }
            """
        ),
        variables={'samples': list(sg_ids), 'project': project},
    )
    # flip through the results and find the latest analysis for each SG ID
    analysis_by_sid_by_type: dict[str, dict[str, dict[str, str]]] = {
        'cram': {},
        'gvcf': {},
    }
    for sg in results['project']['sequencingGroups']:
        for analysis in sg['analyses']:
            analysis_by_sid_by_type[analysis['type']][sg['id']] = analysis

    return analysis_by_sid_by_type


def get_assays_for_sgs(project: str, sg_ids: set[str]) -> dict[str, dict]:
    """
    GQL query aiming for equivalence with
    metamist.AnalysisApi().get_assays_by_criteria(
        body_get_assays_by_criteria=metamist.models.BodyGetAssaysByCriteria(
            sample_ids=sample_ids,
        )
    )

    Issue here is that GQL returns many results - is it important which we use?
    Args:
        project ():
        sg_ids ():

    Returns:
        {SG_ID: [assay1]}
    """
    assays = query(
        gql(
            """
            query SGAssayQuery($samples: [String!]!, $project: String!) {
                project(name: $project) {
                    sequencingGroups(id: {in_: $samples}) {
                        id
                        assays {
                                externalIds
                                id
                                meta
                                type
                            }
                        }
                    }
                }
            """
        ),
        variables={'samples': list(sg_ids), 'project': project},
    )
    return {
        sg['id']: sg['assays'][0] for sg in assays['project']['sequencingGroups'][0]
    }


def process_existing_test_samples(
    test_project_data: PROJECT_DATA,
    sg_ids: set[str],
    main_project_data: PROJECT_DATA,
    clear_out_test: bool = False,
) -> tuple[set[str], set[str]]:
    """
    Find transfer targets which already exist in the test project
    Optionally also find test entities to delete

    Args:
        test_project_data (PROJECT_DATA):
        sg_ids ():
        main_project_data ():
        clear_out_test ():

    Returns:
         two lists - test samples to keep, test samples to remove
    """
    test_ext_ids = {v['sample'] for v in test_project_data.values()}
    keep, remove = set(), set()
    for sg_id in sg_ids:
        main_entity = main_project_data[sg_id]
        if main_entity['sample'] in test_ext_ids:
            keep.add(sg_id)

    if clear_out_test:
        # find all test SG IDs which are not in the SGIDs to copy in
        main_ext_ids = {v['sample'] for v in main_project_data.values()}
        for sg_id, test_entity in test_project_data.items():
            if test_entity['sample'] not in main_ext_ids:
                remove.add(sg_id)

    return keep, remove

    # external_ids = [s['external_id'] for s in samples]
    # test_samples_to_remove = [
    #     s for s in test_samples if s['external_id'] not in external_ids
    # ]
    # test_samples_to_keep = [s for s in test_samples if s['external_id'] in external_ids]
    # if test_samples_to_remove:
    #     logger.info(
    #         f'Removing test samples: {_pretty_format_samples(test_samples_to_remove)}'
    #     )
    #     for s in test_samples_to_remove:
    #         sapi.update_sample(s['id'], SampleUpsert(active=False))
    #
    # if test_samples_to_keep:
    #     logger.info(
    #         f'Test samples already exist: {_pretty_format_samples(test_samples_to_keep)}'
    #     )
    #
    # return {s['external_id']: s for s in test_samples_to_keep}


def pull_samples_from_families(
    families: set[str], all_samples: dict[str, dict[str, str]]
) -> set[str]:
    """
    Pull all samples from a list of families
    all_samples:
    {CPG_id: {'sample': externalID, 'family': familyID}}

    Args:
        families (list[str]): list of family IDs
        all_samples (dict[str, dict[str, str]]): all samples in the project

    Returns:
        set[str]: set of sample IDs
    """

    sg_ids = set()
    for sg_id, details in all_samples.items():
        if details['family'] in families:
            sg_ids.add(sg_id)
    return sg_ids


if __name__ == '__main__':

    # n.b. I'm switching to ArgumentParser because it's more flexible when
    # handling multiple parameters which take an arbitrary number of arguments.
    parser = ArgumentParser(description='Argument parser for subset generator')
    parser.add_argument(
        '--project', required=True, help='The sample-metadata project ($DATASET)'
    )
    parser.add_argument('-n', type=int, help='Number of samples to subset')
    parser.add_argument('-f', type=int, help='Min # families to include')
    # Flag to be used when there isn't available pedigree/family information.
    parser.add_argument(
        '--skip-ped',
        action='store_true',
        help='Skip transferring pedigree/family information',
    )
    parser.add_argument(
        '--families', nargs='+', help='Additional families to include.', default=[]
    )
    parser.add_argument(
        '--samples', nargs='+', help='Additional samples to include.', default=[]
    )
    parser.add_argument(
        '--noninteractive', action='store_true', help='Skip interactive confirmation'
    )
    args, fail = parser.parse_known_args()
    if fail:
        parser.print_help()
        raise AttributeError(f'Invalid arguments: {fail}')
    main(
        project=args.project,
        samples_n=args.n,
        families_n=args.f,
        skip_ped=args.skip_ped,
        additional_families=args.families,
        additional_samples=args.samples,
        noninteractive=args.noninteractive,
    )
