from collections import defaultdict

from sample_metadata.apis import ProjectApi, AnalysisApi
from sample_metadata.model.analysis_query_model import AnalysisQueryModel
from sample_metadata.model.analysis_status import AnalysisStatus
from sample_metadata.model.analysis_type import AnalysisType

import logging
import click
import json
from google.cloud import storage

# Global vars
EXTENSIONS = ['.fastq.gz', '.fastq', '.bam', '.cram', '.fq', 'fq.gz']

logger = logging.getLogger(__file__)
logger.setLevel(level=logging.INFO)

aapi = AnalysisApi()
client = storage.Client()

def get_datasets() -> list[dict]:
    return ProjectApi().get_seqr_projects()


@click.command()
@click.option('--analysis-type', default='cram')
@click.option('--dry-run', is_flag=True)
def main(analysis_type, dry_run):
    # get all projects
    projects = get_datasets()
    
    datasets = [p['name'] for p in projects]
    project_id_map = {p['id']: p['name'] for p in projects}
    
    analyses = get_analyses_for_datasets(datasets, analysis_type)
    
    # project_ids = {a['project'] for a in analyses}
    
    samples = {a['sample_ids'][0] for a in analyses}

    analyses, ids_to_delete, duplicate_analyses = deduplicate_analyses(analyses)

    # TODO remove analysis from analyses dict if analysis id in ids_to_delete
    all_analysis_ids = set()
    for analysis in analyses:
        all_analysis_ids.add(analysis['id'])

    print(len(all_analysis_ids), "total analyses entries")

    all_ids_to_delete = set(ids_to_delete)
    print(len(all_ids_to_delete), 'unique duplicates')

    remaining_analyses_ids = all_analysis_ids.difference(all_ids_to_delete)

    remaining_analyses = []
    for analysis in analyses:
        if analysis['id'] in  remaining_analyses_ids:
            remaining_analyses.append(analysis)

    print(len(remaining_analyses), 'analysis entries remaining after duplicates removed')

    # example = duplicate_analyses[0]
    # print(example)
    # exit()

    # throw warnings if the analyses['project'] doesn't match the bucket name, `cpg-{dataset}-{main/test}`
    # check_all_analyses_belongs_to_project(remaining_analyses, project_id_map)

    analysis_by_path = cleanup_analysis_output_paths(remaining_analyses)
    exit()
    for analysis in remaining_analyses:
        analysis_output = analysis['output'].replace("'","\"")

    
    print(len(analysis_by_path), "unique paths found")
 
    # TODO just write a function to check if paths are valid, too much dictionary passing in main
    invalid_paths = []
    for a in analysis_by_path:
        a_json_str = a.replace("'","\"")
        try:
            a_dict = json.loads(a_json_str)
            ap = str(a_dict['cram'])
            print("dict read:", ap)
        except json.decoder.JSONDecodeError:
            ap = a
            print("string read:", ap)

        #if not ap.startswith('gs://'):
        #    invalid_paths.append(ap)

    # invalid_paths = [a['cram'] for a in analysis_by_path if not a.startswith('gs://')]
    if invalid_paths != []:
        logging.error(f'Invalid paths found: {invalid_paths}')
    # TODO: error handling for invalid paths

    # let's check which files exist
    path_existence = validate_paths(analysis_by_path)

    print(f'{len(path_existence["valid"])} valid paths, {len(path_existence["invalid"])} invalid paths')

    print('Analysis paths with no file discovered:', path_existence['invalid'])


    # now do more stuff to check:
    #   analysis-entries we have to delete because files don't exist
    #   samples that DID have analysis and now don't
    if dry_run:
        print(f'Ready for ids to delete: {ids_to_delete}')
    else:
        # delete some analysis entries
        pass

def get_analyses_for_datasets(datasets, analysis_type) -> list[dict]:
    # get all analysis-entries 
    return aapi.query_analyses(
            AnalysisQueryModel(
                projects=datasets,
                type=AnalysisType(analysis_type),
                # status=AnalysisStatus('completed'),
            )
        )

def find_invalid_analyses(analyses: list[dict]):
    """
    
    
    """
    # flag analyses in a bad state
    pass


def get_path_components_from_path(path):
    """
    Return the {bucket_name}, {dataset}, and {subdir} for GS only paths

    >>> get_bucket_name_path_from_path('gs://cpg-dataset-main/subfolder/subfolder2/my.cram')
    {bucket_name:'cpg-dataset-main', dataset:'dataset', subdir:'subfolder/subfolder2'}
    """
    if not path.startswith('gs://'):
        # raise warnings.warn(f'Analysis path {path} not bucket storage.')
        pass
    short_path = path.removeprefix('gs://').split('/', maxsplit=1)
    bucket_name = short_path[0]

    dataset_with_bucket_type = bucket_name.removeprefix('cpg-')
    bucket_type = dataset_with_bucket_type.split('-')[-1]
    dataset = dataset_with_bucket_type.removesuffix(bucket_type)[:-1]

    _, subdir = short_path[1].rsplit('/',1)
    
    return {'bucket_name':bucket_name, 'dataset':dataset, 'subdir':subdir}


def deduplicate_analyses(analyses: list[dict]) -> tuple[list[dict], list[int], list[list]]:
    # Check which analysis IDs point to the same analysis output path
    # Keep the smallest ID pointing to a given analysis output, duplicates can be scheduled for deletion

    all_analyses = defaultdict(set)
    
    # map each output path to the analysis ids that point to it
    for analysis in analyses:
        all_analyses[analysis['output']].add(analysis['id'])

    duplicate_analyses = [vals for vals in all_analyses.values() if len(vals)>1]
    print('Path with most analysis entries pointing to it:',
          sorted(all_analyses.items(), key=lambda x: len(x[1]), reverse=True)[0]
          )
    
    # keep the first entry pointing to a path, duplicates can be deleted
    duplicate_analyses_ids = []
    for duplicate_ids in duplicate_analyses:
        duplicate_analyses_ids.append(sorted(duplicate_ids)[1:])

    ids_to_delete = [e for sublist in duplicate_analyses_ids for e in sublist]

    return (analyses, ids_to_delete, duplicate_analyses_ids)

def check_all_analyses_belongs_to_project(analyses: list[dict], project_id_map: dict):
    # Check the analyses ['project'] matches the bucket name, `cpg-{dataset}-{main/test}`

    for analysis in analyses:
        project_id = analysis['project']
        project_name = project_id_map[project_id]
        dataset_name = get_path_components_from_path(analysis['output'])['dataset']
        if dataset_name == project_name:
            continue
        logging.error(f'Analysis ID: {str(analysis["id"])} has project ID: {project_id} which maps to: {project_name}, but analysis dataset is: {dataset_name}')

    return True


def validate_paths(paths: iter) -> dict[str, bool]:
    # Check if a path to an analysis object exists in the bucket
    # return a dictionary of valid paths 

    paths_by_bucket = defaultdict(list)
    # iterate through all subdirectories/paths in a bucket
    for p in paths:
        bucket_name = p.removeprefix('gs://').split('/', maxsplit=1)[0]
        paths_by_bucket[bucket_name].append(p)


    valid_paths = {}
    invalid_paths = {}
    # Check if each subdirectory/path in a bucket is valid or not
    for bucket_name, paths in paths_by_bucket.items():
        valid, invalid = validate_paths_for_bucket(bucket_name, paths)
        valid_paths.update(valid)
        invalid_paths.update(invalid)

    path_exists = {'valid': valid_paths, 'invalid': invalid_paths}
    
    return path_exists

def validate_paths_for_bucket(bucket_name, paths: list[str]) -> tuple[set[str], set[str]]:
    # group by path prefix

    path_set = set()
    for p in paths:
        # subdir = p.removeprefix(f'gs://{bucket_name}/')
        pc = get_path_components_from_path(p)
        path_set.add(pc['subdir'])

    files_by_subdir = {}
    for subdir in path_set:
        # don't ever read a directory with a path component that ends in [.ht, .mt]
        if '.mt' in subdir or '.ht' in subdir:
            continue
        
        files_by_subdir[subdir] = get_paths_for_subdir(client, bucket_name, subdir) 

    # paths = all paths found in Metamist for a given bucket name from analysis['output']
    # files_by_subdir = all paths found in gcloud storage for a given bucket grouped by bucket subdir
    files_set = set([e for sublist in files_by_subdir.values for e in sublist])

    # Has analysis entry but file moved or deleted:
    analysis_entries_missing_files = path_set.difference(files_set)

    # valid entries are all analysis output paths that intersect with all gcp output paths
    valid_entries = path_set.intersect(files_set)

    return valid_entries, analysis_entries_missing_files


def cleanup_analysis_output_paths(analyses):
    analysis_paths = set()
    for analysis in analyses:
        analysis_output = analysis['output'].replace("'","\"")
        analysis_output = analysis_output.replace('GSPath("','"')
        analysis_output = analysis_output.replace(')','')

        try:
            output_dict = json.loads(analysis_output)
            ap = str(output_dict['cram'])
            print('JSON cram output:', ap)
            
        except json.decoder.JSONDecodeError:
            #print('JSON decode error')
            ap = analysis['output']
            print('Simple output:',ap)
        ap = analysis['output']
        analysis_paths.add(ap)
    
    return analysis_paths


def get_paths_for_subdir(client, bucket_name, subdirectory):
    # bucket = client.get_bucket(bucket_name)

    # no prefix means it will get all blobs in the bucket (regardless of path)
    # this can be a dangerous call
    files_in_bucket_subdir = []
    for blob in client.list_blobs(
            bucket_name, prefix=subdirectory, delimiter='/'
        ):

        # todo: extra checks here,
        
        

        # you can decide to filter by extension, or you could cleverly decide which extension
        # to look for based on the analyses['output'] paths.
        if not any(blob.endswith(ext) for ext in EXTENSIONS):
            continue
        files_in_bucket_subdir.append(f'gs://{bucket_name}/{blob.name}')


if __name__ == "__main__":
    # import doctest
    # doctest.testmod()

    main()
