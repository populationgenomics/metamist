from collections import defaultdict
from itertools import chain
import logging
import json
import click
from google.cloud import storage
from sample_metadata.apis import ProjectApi, AnalysisApi
from sample_metadata.model.analysis_query_model import AnalysisQueryModel
from sample_metadata.model.analysis_type import AnalysisType


# Global vars
EXTENSIONS = ['.fastq.gz', '.fastq', '.bam', '.cram', '.fq', 'fq.gz']
BUCKET_TYPES = ['main', 'test', 'archive',' release', 'upload', 'tmp', 'analysis', 'web']

logger = logging.getLogger(__file__)
logger.setLevel(level=logging.INFO)

aapi = AnalysisApi()
client = storage.Client()


def get_datasets() -> list[dict]:
    """API call to get project list"""
    return ProjectApi().get_seqr_projects()


def get_analyses_for_datasets(datasets, analysis_type) -> list[dict]:
    """API call to get all analysis objects in datasets list"""
    return aapi.query_analyses(
        AnalysisQueryModel(
            projects=datasets,
            type=AnalysisType(analysis_type),
            # status=AnalysisStatus('completed'),
        )
    )


def get_path_components_from_path(path):
    """
    Return the {bucket_name}, {dataset}, and {subdir} for GS only paths

    >>> get_bucket_name_path_from_path('gs://cpg-dataset-main/subfolder/subfolder2/my.cram')
    {bucket_name:'cpg-dataset-main', dataset:'dataset', subdir:'subfolder/subfolder2'}

    >>> get_bucket_name_path_from_path('gs://cpg-dataset-test-upload/my.cram')
    {bucket_name:'cpg-dataset-test-upload', dataset:'dataset', subdir:''}
    """
    if not path.startswith('gs://'):
        logging.error(f'Analysis path {path} not bucket storage.')
        return

    short_path = path.removeprefix('gs://').split('/', maxsplit=1)
    bucket_name = short_path[0]

    dataset_with_bucket_type = bucket_name.removeprefix('cpg-')

    split_bucket_name = dataset_with_bucket_type.split('-')
    for bucket_component in split_bucket_name:
        if bucket_component in BUCKET_TYPES:
            bt_index = split_bucket_name.index(bucket_component)

    #bucket_type = '-'.join(split_bucket_name[bt_index:])
    dataset = '-'.join(split_bucket_name[:bt_index])

    subdir, _ = short_path[1].rsplit('/', 1)

    return {'bucket_name': bucket_name, 'dataset': dataset, 'subdir': subdir}


def find_duplicate_analyses(
    analyses: list[dict],
) -> tuple[list[dict], set[int]]:
    """
    Check which analysis IDs point to the same analysis output path
    Keep the smallest ID pointing to a given analysis output, label the others as duplicates

    Return the analyses and the duplicate analysis IDs which can be deleted
    """
    all_analyses = defaultdict(set)

    # map each output path to the analysis ids that point to it
    for analysis in analyses:
        all_analyses[analysis['output']].add(analysis['id'])

    duplicate_analyses = [vals for vals in all_analyses.values() if len(vals) > 1]
    print(
        'Path with most analysis entries pointing to it:',
        sorted(all_analyses.items(), key=lambda x: len(x[1]), reverse=True)[0],
    )

    # keep the first entry pointing to a path, the rest are duplicates
    ids_to_delete = set(chain.from_iterable(duplicate_analyses))
    #for duplicate_ids in duplicate_analyses:
    #    ids_to_delete.update(sorted(duplicate_ids)[1:])

    return analyses, ids_to_delete


def remove_dupes_from_analyses(
    analyses: list[dict], duplicate_ids: list[int], print_dupes: bool
) -> list[dict]:
    """Takes all analysis objects and removes any from the master analyses list if the analysis id is a duplicate"""

    all_duplicate_ids = set(duplicate_ids)
    all_analysis_ids = set()

    for analysis in analyses:
        all_analysis_ids.add(analysis['id'])

    # use set difference to filter out all duplicate ids from the master list
    remaining_analyses_ids = all_analysis_ids.difference(all_duplicate_ids)

    remaining_analyses = []
    for analysis in analyses:
        if analysis['id'] in remaining_analyses_ids:
            remaining_analyses.append(analysis)

    if print_dupes:
        print(len(all_analysis_ids), 'total analyses entries')
        print(len(all_duplicate_ids), 'total duplicates')
        print(
            len(remaining_analyses),
            'analysis entries remaining after duplicates removed',
        )

    return remaining_analyses


def check_all_analyses_belongs_to_project(analyses: list[dict], project_id_map: dict):
    """
    Check the analysis['project'] matches the bucket name, `cpg-{dataset}-{main/test}`
    Uses the project id map to get the project name from the project id
    """

    for analysis in analyses:
        project_id = analysis['project']
        project_name = project_id_map[project_id]
        try:
            dataset_name = get_path_components_from_path(analysis['output'])['dataset']
        except TypeError:
            continue
        if dataset_name == project_name:
            continue
        logging.error(
            f'Analysis ID: {str(analysis["id"])} has project ID: {project_id} which maps to: {project_name}, but analysis dataset is: {dataset_name}'
        )


def validate_paths(
    analyses: list[dict], analysis_by_path: dict, analysis_type: str
) -> tuple[list[dict], dict, dict]:
    """
    Checks if the paths to the analysis objects are pointing to files that exist in the bucket
    First remove any analysis objects with paths that dont start with gs://.
    Then iterate through the remaining paths to get all bucket/subdirectory combinations.
    Then call validate_paths_for_bucket to check each analysis path against the blobs in each bucket

    Returns dictionaries of valid/invalid paths and valid/invalid ids
    """

    # Flip the analysis id : analysis path dictionary
    path_by_analysis = dict(
        (path, analysis_id) for analysis_id, path in analysis_by_path.items()
    )

    # Replace each analysis output value with the cleaned up analysis path
    for analysis in analyses:
        analysis['output'] = analysis_by_path[analysis['id']]

    # Find any immediately invalid paths and remove them from the analyses dictionary
    invalid_ids_paths = [
        a for a in analysis_by_path.items() if not a[1].startswith('gs://')
    ]
    invalid_paths = [p[1] for p in invalid_ids_paths]
    if invalid_paths != []:
        logging.error(f'Invalid paths found: {invalid_paths}')
        for invalid_id_path in invalid_ids_paths:
            analysis_by_path.pop(invalid_id_path[0], None)

    analyses[:] = [a for a in analyses if a['output'] not in invalid_paths]

    paths_by_bucket = defaultdict(list)
    # iterate through all subdirectories/paths in a bucket
    for p in analysis_by_path.values():
        bucket_name = p.removeprefix('gs://').split('/', maxsplit=1)[0]
        paths_by_bucket[bucket_name].append(p)

    valid_analysis_paths = set()
    invalid_analysis_paths = set(invalid_paths)
    # Check if each subdirectory/path in a bucket is valid or not
    for bucket_name, paths in paths_by_bucket.items():
        valid, invalid = validate_paths_for_bucket(bucket_name, paths, analysis_type)
        valid_analysis_paths.update(valid)
        invalid_analysis_paths.update(invalid)

    path_exists = {
        'valid_paths': valid_analysis_paths,
        'invalid_paths': invalid_analysis_paths,
    }

    # Use the flipped analysis id : analysis path dictionary to get the valid and invalid ids
    valid_analysis_ids = set()
    invalid_analysis_ids = set()

    for valid_path in path_exists['valid_paths']:
        valid_analysis_ids.add(path_by_analysis[valid_path])

    for invalid_path in path_exists['invalid_paths']:
        invalid_analysis_ids.add(path_by_analysis[invalid_path])

    id_exists = {'valid_ids': valid_analysis_ids, 'invalid_ids': invalid_analysis_ids}

    return analyses, path_exists, id_exists


def validate_paths_for_bucket(
    bucket_name, paths: list[str], analysis_type: str
) -> tuple[set[str], set[str]]:
    """
    Takes a bucket name and list of paths found in analysis objects
    Uses get_paths_for_subdir to return all blobs found in the bucket/subdir
    Returns valid analysis paths that match existing blobs and invalid paths with no match
    """

    path_set = set(paths)

    subdirs_from_analysis_paths = set()
    for p in paths:
        pc = get_path_components_from_path(p)
        subdirs_from_analysis_paths.add(pc['subdir'])

    # Read all file paths with the specified analysis type in the gcp bucket, grouping by subdir
    files_by_subdir = {}
    for subdir in subdirs_from_analysis_paths:
        # don't ever read a directory with a path component that ends in [.ht, .mt]
        if '.mt' in subdir or '.ht' in subdir:
            continue
        files_by_subdir[subdir] = get_paths_for_subdir(
            client, bucket_name, subdir, analysis_type
        )

    files_set = set([e for sublist in files_by_subdir.values() for e in sublist])

    # Has analysis path but file moved or deleted:
    analysis_entries_missing_files = path_set.difference(files_set)

    # valid paths are all analysis output paths that intersect with an existing gcp file
    valid_entries = path_set.intersection(files_set)

    return valid_entries, analysis_entries_missing_files


def get_paths_for_subdir(client, bucket_name, subdirectory, analysis_type):
    """Iterate through a bucket/subdir and get all the blobs with analysis_type file extension"""
    files_in_bucket_subdir = []
    for blob in client.list_blobs(
        bucket_name, prefix=(subdirectory + '/'), delimiter='/'
    ):
        # todo: extra checks here,

        # Check if file ends with specified analysis type
        if not blob.name.endswith(analysis_type):
            continue
        files_in_bucket_subdir.append(f'gs://{bucket_name}/{blob.name}')

    return files_in_bucket_subdir


def cleanup_analysis_output_paths(analyses: list[dict]) -> dict[int, str]:
    """
    Parses analysis output paths into simple path strings

    Analysis output path is usually just a GS path: gs://some-bucket/file.cram, but
    sometimes its a key-value list of different output paths. In this case, the output
    paths are read into a dictionary then relevant path is extracted as a string.

    Returns a dictionary of {analysis ID : analysis path}
    """

    analysis_paths = {}
    for analysis in analyses:
        # json.loads requires double quotes so we replace all single quotes
        # GSPath objects need to be converted to str, e.g. GSPath('gs://...') -> "gs:/..."
        analysis_output = analysis['output'].replace("'", "\"")
        analysis_output = analysis_output.replace('GSPath("', '"')
        analysis_output = analysis_output.replace(')', '')

        try:
            # For the case where the analysis output is a key-value list of paths
            output_dict = json.loads(analysis_output)
            analysis_path = str(output_dict['cram'])

        except json.decoder.JSONDecodeError:
            # For the case where the analysis output is just the path
            analysis_path = analysis['output']

        analysis_paths[analysis['id']] = analysis_path

    return analysis_paths


@click.command()
@click.option('--analysis-type', default='cram')
@click.option('--dry-run', is_flag=True)
@click.option('--print-dupes', is_flag=True)
def main(analysis_type, dry_run, print_dupes):
    """
    Finds all analysis entries across the various seqr projects
    Checks each entry has a valid path to an analysis file
    Checks if multiple analysis entries point to the same path

    TODO:
    Delete analysis entries that are duplicates or have invalid path
    """

    # get all seqr projects
    projects = get_datasets()

    datasets = [p['name'] for p in projects]
    project_id_map = {p['id']: p['name'] for p in projects}

    analyses = get_analyses_for_datasets(datasets, analysis_type)

    analyses, duplicate_ids = find_duplicate_analyses(analyses)

    # Remove analysis from analyses list if analysis id in duplicate_ids
    remaining_analyses = remove_dupes_from_analyses(
        analyses, duplicate_ids, print_dupes
    )

    # Get all {analysis id : raw analysis path string}
    analysis_by_path = cleanup_analysis_output_paths(remaining_analyses)

    # Check which analysis paths lead to files that exist
    remaining_analyses, path_validity, id_validity = validate_paths(
        remaining_analyses, analysis_by_path, analysis_type
    )

    # log warnings if the analyses['project'] doesn't match the bucket name, `cpg-{dataset}-{main/test}`
    # lots of warnings here...
    check_all_analyses_belongs_to_project(remaining_analyses, project_id_map)

    print(
        f'{len(path_validity["valid_paths"])} valid paths, {len(path_validity["invalid_paths"])} invalid paths'
    )

    # print('Analysis paths with no file discovered:', path_validity['invalid_paths'])

    # Combine the duplicates ids with the invalid ids
    ids_to_delete = set(duplicate_ids).union(id_validity['invalid_ids'])

    # now do more stuff to check:
    #   analysis-entries we have to delete because files don't exist
    #   samples that DID have analysis and now don't
    if dry_run:
        print(f'Ready for ids to delete: {ids_to_delete}')
    else:
        # TODO
        # delete the analysis entries in ids_to_delete
        pass


if __name__ == "__main__":
    # import doctest
    # doctest.testmod()

    main()
