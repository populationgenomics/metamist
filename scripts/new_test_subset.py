#!/usr/bin/env python3
# pylint: disable=unsubscriptable-object,too-many-locals

"""
Versatile script for migrating production data to a test bucket & corresponding
metamist project.

Example Invocation

analysis-runner \
    --dataset validation \
    --description "populate validation test subset" \
    --output-dir "validation-test-copy" \
    --access-level full \
    scripts/new_test_subset.py \
        --project acute-care \
        -n 6 \
        --families Family1 Family2 \
        --samples SG-1 SG-2 SG-3

This example will populate validation-test with the metamist data for:
    - All members of Family1 & Family 2
    - SGs 1, 2, and 3
    - 6 additional SG IDs randomly selected
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


def dont_log_queries(
    query_string: str, log_response: bool = False, variables: dict | None = None
):
    """
    Don't log the queries - switch off the logging for duration of a query

    Args:
        query_string ():
        log_response (bool): Override the logging silencer
        variables ():

    Returns:
        dict, the query result
    """
    current_level = (
        logging.root.level
    )  # even if not set this defaults to WARN(30) on logging import
    if current_level >= logging.INFO and not log_response:
        logging.getLogger().setLevel(logging.WARN)
    result = query(gql(query_string), variables=variables)
    # reset logger to baseline
    logging.getLogger().setLevel(current_level)
    return result


def get_project_samples(project: str) -> PROJECT_DATA:
    """
    Get all prod SG IDs for a project using gql
    Return in form
    {
        CPG_id: {
           'sample': externalID,
           'family': familyID,
           'meta': sequencing group meta,
           'type': type
        }
    }

    Args:
        project (str): the project to query

    Returns:
        {SG_ID: {various metadata},}
    """
    result = dont_log_queries(
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
    }""",
        variables={'project': project},
    )
    return {
        sg['id']: {
            'sample': sg['sample']['externalId'],
            'family': sg['sample']['participant']['families'][0]['externalId'],
            'meta': sg['meta'],
            'type': sg['type'],
        }
        for sg in result['project']['sequencingGroups']
    }


def get_random_families_from_fam_sg_dict(
    sgd_by_family_id, dodge_families: set[str], families_n: int, min_fam_size: int = 0
) -> set[str]:
    """
    Get a random subset of families
    Balance family selection to create an even distribution of family sizes
    Do this using the random.choices function, which allows for weighted random selection

    We pull all families, remove families we already intend to select, and sample from
    the remaining families

    Args:
        sgd_by_family_id (dict[str, set[str]]): a lookup of SGID sets by family ID
        dodge_families (set[str]): we already want these fams, don't include in random selection
        families_n (int): number of new families to include
        min_fam_size (int): currently unused

    Returns:
        set[str] - additional families to select
    """

    total_counter = 0
    family_counter = defaultdict(list)
    for fam_id, sgid_set in sgd_by_family_id.items():
        # skip over fams we already want, or who are undersized
        if fam_id in dodge_families or len(sgid_set) < min_fam_size:
            continue
        total_counter += 1
        family_counter[len(sgid_set)].append(fam_id)

    # unable to select anough additional families
    if total_counter < families_n:
        raise ValueError(
            f'Not enough families to choose from. You explicitly requested '
            f'{len(dodge_families)} families through sample/family ID, and '
            f'Only {total_counter} other families are available, '
            f'{families_n} additional random families were requested.'
        )

    # exactly the right number - return every remaining family
    if total_counter == families_n:
        return set(chain.from_iterable(family_counter.values()))

    # weighted random selection from remaining families
    # families are sorted by size {1: {fam1, fam2}, 2: {fam3}}
    # length of each list represents the weighting across all family groups
    weights = [len(family_counter[k]) for k in sorted(family_counter.keys())]

    # Counter records how many selections should be made from each family size group
    family_size_choices = dict(
        Counter(
            random.choices(sorted(family_counter.keys()), weights=weights, k=families_n)
        )
    )

    # build 1 set from all the randomly selected families, weighted by size
    return_families = set()
    for k, v in family_size_choices.items():
        return_families.update(random.sample(family_counter[k], v))

    return return_families


def copy_files_in_dict(to_copy, project: str, sid_replacement: tuple[str, str] = None):
    """
    Copied from prev script --
    Replaces all `gs://cpg-{project}-main*/` paths
    into `gs://cpg-{project}-test*/` and creates copies if needed
    If `to_copy` is dict or list, recursively calls this function on every element
    If `to_copy` is str, replaces the path

    Args:
        to_copy (Any): a file or collection of files to copy to test
        project (str):
        sid_replacement (tuple[str, str]): a mapping of Prod -> Test SG IDs
    """
    if not to_copy:
        return to_copy
    if isinstance(to_copy, str) and to_copy.startswith(f'gs://cpg-{project}-main'):
        logging.info(f'Looking for analysis file {to_copy}')
        old_path = to_copy
        if not to_path(old_path).exists():
            logging.warning(f'File {old_path} does not exist')
            return to_copy
        new_path = old_path.replace(
            f'gs://cpg-{project}-main', f'gs://cpg-{project}-test'
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
    if isinstance(to_copy, list):
        return [copy_files_in_dict(x, project) for x in to_copy]
    if isinstance(to_copy, dict):
        return {k: copy_files_in_dict(v, project) for k, v in to_copy.items()}
    return to_copy


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
    test_ep_ip_map = dont_log_queries(
        """
        query MyQuery($project: String!) {
            project(name: $project) {
                participants {
                    externalId
                    id
                }
            }
        }""",
        variables={'project': target_project},
    )
    test_ext_to_int_pid = {
        party['externalId']: party['id']
        for party in test_ep_ip_map['project']['participants']
        if party['externalId'] in external_ids
    }
    prod_map = dont_log_queries(
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
        }""",
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
        initial_project (str): prod project name
        target_project (str): test project name
        family_ids ():

    Returns:
        a list of all relevant family IDs
    """

    pedigree = dont_log_queries(
        """
        query MyQuery($project: String!, $families: [String!]!) {
            project(name: $project) {
               pedigree(internalFamilyIds: $families)
            }
        }""",
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
        initial_project (str): prod project name
        target_project (str): test project name
        participant_ids (set[int]):

    Returns:

    """

    families = dont_log_queries(
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
        }""",
        variables={'samples': list(participant_ids), 'project': initial_project},
    )

    # collect all the TSV lines into a set of Strings, then write all at once
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

    family_headers = '\t'.join(FAMILY_HEADINGS)
    with open('tmp_families.tsv', 'wt') as tmp_families:
        tmp_families.write(family_headers + '\n')
        for family_line in family_lines:
            tmp_families.write(family_line + '\n')

    with open('tmp_families.tsv') as family_file:
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
    participants = dont_log_queries(
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
        """,
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

    results = dont_log_queries(
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
        """,
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
        project (str):
        sg_ids (list[str]):

    Returns:
        {SG_ID: [assay1]}
    """
    assays = dont_log_queries(
        """
        query SGAssayQuery($samples: [String!]!, $project: String!) {
            project(name: $project) {
                sequencingGroups(id: {in_: $samples}) {
                    id
                    assays {
                            id
                            meta
                            type
                        }
                    }
                }
            }
        """,
        variables={'samples': list(sg_ids), 'project': project},
    )
    return {sg['id']: sg['assays'][0] for sg in assays['project']['sequencingGroups']}


def process_existing_test_sgids(
    test_data: PROJECT_DATA,
    sg_ids: set[str],
    main_data: PROJECT_DATA,
    clear_test: bool = False,
) -> tuple[set[str], set[str]]:
    """
    Find transfer targets which already exist in the test project
    Optionally also find test entities to delete

    Args:
        test_data (PROJECT_DATA):
        sg_ids (set[str]):
        main_data (PROJECT_DATA):
        clear_test (bool):

    Returns:
         two lists - test samples to keep, test samples to remove
    """
    test_ext_ids = {v['sample'] for v in test_data.values()}
    keep, remove = set(), set()
    for sg_id in sg_ids:
        if main_data[sg_id]['sample'] in test_ext_ids:
            keep.add(sg_id)

    if clear_test:
        # remove action is not currently done
        # find all test SG IDs which are not in the SGIDs to copy in
        main_ext_ids = {v['sample'] for v in main_data.values()}
        for sg_id, test_entity in test_data.items():
            if test_entity['sample'] not in main_ext_ids:
                remove.add(sg_id)

    return keep, remove


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
    metamist_main_content = get_project_samples(project)
    main_sgs_by_family = defaultdict(set)
    for sg_id, data in metamist_main_content.items():
        main_sgs_by_family[data['family']].add(sg_id)

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
            family_set |= {metamist_main_content[sgid]['family'] for sgid in sample_set}

        # family_set is ones we definitely want to include
        # we need to make sure we don't include them in the random selection
        family_set |= get_random_families_from_fam_sg_dict(
            main_sgs_by_family, family_set, families_n
        )

        # update the set of chosen samples (which can be empty)
        # with all the SGIDs from the selected families
        # chain.from_iterable flattens a generator of all SG ID sets
        # across all families into a single set
        sample_set |= set(
            chain.from_iterable(
                v for k, v in main_sgs_by_family.items() if k in family_set
            )
        )

    elif samples_n:
        # if there are additional samples specified, find the corresponding families
        # these specifically requested families & samples are copied over in addition
        # to the random selection families_n number of families
        if family_set:
            # we already have this data in query result
            sample_set |= set(
                chain.from_iterable(
                    v for k, v in main_sgs_by_family.items() if k in family_set
                )
            )

        # top up the selected SGIDs with random selections
        # resulting SG IDs we want to copy into the test project
        sample_set |= set(
            random.sample(set(metamist_main_content.keys()) - sample_set, samples_n)
        )

    # maybe we only want to copy specific samples or families...
    else:
        if sample_set:
            sample_set |= set(
                chain.from_iterable(
                    v for k, v in main_sgs_by_family.items() if k in family_set
                )
            )

    logging.info(f'Subset to {len(sample_set)} samples')

    # Populating test project
    target_project = project + '-test'
    logging.info('Checking any existing test samples in the target test project')
    metamist_test_content = get_project_samples(target_project)
    keep, remove = process_existing_test_sgids(
        metamist_test_content, sample_set, metamist_main_content, clear_out_test
    )

    if remove and clear_out_test:
        logging.info(f'Removing test samples: {remove}')
        # TODO do removal

    # don't bother actioning these guys - already exist in target
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

    # now ignore them forever - don't try to transfer again
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
                    # external_ids=sg_assay['externalIds'],  ## issue raised
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
