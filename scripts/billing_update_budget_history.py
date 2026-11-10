"""
This script goes through all the commit history to cpg-infrastructure-private and
check for changes to budgets.yaml files for each of the projects.
Once collected it gets checked agains budget BQ table and
it inserts any missing records.
At this stage it does not delete any records from BQ table.
"""
import argparse
import logging
import os
import re
import sys
from datetime import datetime, timezone

import google.cloud.bigquery as bq

# name of the BQ table to insert the records
SM_GCP_BQ_BUDGET_VIEW = os.getenv('SM_GCP_BQ_BUDGET_VIEW')
SM_GCP_BQ_AGGREG_VIEW = os.getenv('SM_GCP_BQ_AGGREG_VIEW')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# print logs to terminal as well
logger.addHandler(logging.StreamHandler())


def get_gcp_project_names() -> dict[str, str]:
    """
    Returns list of projects active SM_GCP_BQ_AGGREG_VIEW.
    SM_GCP_BQ_AGGREG_VIEW is aggregated by day so it is not expensive to do
    a full scan on project column only.

    Reason we need actual gcp_project names is the cpg_infrastructure_private can
    contain only the suffix.
    E.g. project name in cpg-infra-private is 'seqr',
    but gcp_project name is 'seqr-123456'
    """
    query = f"""
    SELECT DISTINCT gcp_project FROM `{SM_GCP_BQ_AGGREG_VIEW}`
    WHERE gcp_project IS NOT NULL
    """
    logger.info(f'Executing {query}')
    bq_client = bq.Client()
    query_job = bq_client.query(
        query,
    )
    results = query_job.result()
    # this would contain the mapping of project name to gcp_project name
    # considering last part as number specific for gcp_project name
    projects = {}
    for row in results:
        gcp_project_name = row['gcp_project']
        # by default gcp and project name are the same
        project_name = gcp_project_name
        # get the suffix
        gcp_name_suffix = gcp_project_name.split('-')[-1]
        # check if gcp_name_suffix is number
        if gcp_name_suffix.isdigit():
            # if yes remove the number and leading '-'
            project_name = gcp_project_name.replace(f'-{gcp_name_suffix}', '')

        projects[project_name] = gcp_project_name

    return projects


def extract_budget_updates(folder_path: str, project_name: str) -> dict[datetime, int]:
    """
    Execute cmd on the path and return the output.
    """
    budget_records: dict[datetime, int] = {}

    cmd = f'git log -L2,+1:"{project_name}/budgets.yaml" --pretty="format:%ci"'
    logger.info(f'Executing {cmd}')
    # save the current directory
    cwd = os.getcwd()
    # change the directory to the folder_path and execute the command
    os.chdir(folder_path)
    output = os.popen(cmd).read()
    # change back to the original directory
    os.chdir(cwd)
    logger.info(f'Output: {output}')
    if not output:
        logger.warning(f'Failed to get git history for {project_name}')
        return budget_records

    lines = output.split('\n')
    # loop through lines in the reverse order
    # and look for the first line that has a date or
    # contains string monthly_budget
    last_budget_value = None
    for line in reversed(lines):
        if '+  monthly_budget' in line:
            logger.info(f'Found monthly_budget for {project_name}')
            # line is in the format '+  monthly_budget: XYZ'
            last_budget_value = int(line.split(':')[1].strip())

        elif last_budget_value and re.match(r'\d{4}-\d{2}-\d{2}', line):
            logger.info(f'Found date {line} for {project_name}')
            # 2023-03-02 10:30:32 +1100
            dt = datetime.strptime(line.strip(), '%Y-%m-%d %H:%M:%S %z')
            budget_records[dt.astimezone(timezone.utc)] = last_budget_value
            last_budget_value = None

    return budget_records


def get_bq_budgets() -> dict[str, dict[datetime, int]]:
    """
    Get all the budget records from BQ table.
    """
    query = f'SELECT * FROM {SM_GCP_BQ_BUDGET_VIEW}'
    logger.info(f'Executing {query}')
    bq_client = bq.Client()
    query_job = bq_client.query(
        query,
    )
    results = query_job.result()
    bq_budgets: dict[str, dict[datetime, int]] = {}
    for row in results:
        project_name = row['gcp_project']
        date = row['created_at']
        budget = row['budget']
        bq_budgets.setdefault(project_name, {})[date] = budget

    return bq_budgets


def process(folder_path: str):
    """
    Loop through all the folders in the path and check for changes to budgets.yaml files.
    """
    logger.info(f'Processing {folder_path}')
    gcp_project_names = get_gcp_project_names()
    project_budgets = {}
    for root, _dirs, files in os.walk(folder_path):
        if 'budgets.yaml' in files:
            logger.info(f'Found budgets.yaml in {root}')
            # extract the project name from the path, e.g. /Users/xyz/cpg-infrastructure-private/xyz
            project_name = root.split('/')[-1]

            if project_name in gcp_project_names:
                # mapp the project name to gcp_name
                gcp_project_name = gcp_project_names[project_name]
            else:
                logger.warning(
                    f'Could not find gcp_project name for {project_name}, '
                    'looks like brand new project'
                )
                gcp_project_name = project_name

            project_budgets[gcp_project_name] = extract_budget_updates(
                folder_path, project_name
            )

    # we have budget history for all the projects
    # now check against BQ table
    logger.info(f'Checking against BQ table {SM_GCP_BQ_BUDGET_VIEW}')
    bq_budgets = get_bq_budgets()
    # now compare the two
    missing_records: dict[str, dict[datetime, int]] = {}
    for project_name, budget_records in project_budgets.items():
        bq_project_budgets = bq_budgets.get(project_name, {})

        # now compare individual datetime records per project
        for date, budget in budget_records.items():
            if date not in bq_project_budgets:
                missing_records.setdefault(project_name, {})[date] = budget

    logger.info(f'missing_records {missing_records}')

    # now insert the missing records
    bq_client = bq.Client()

    logger.info(f'Inserting {len(missing_records)} missing records')
    for project_name, budget_records in missing_records.items():
        for date, budget in budget_records.items():
            query_params = [
                bq.ScalarQueryParameter('project_name', 'STRING', project_name),
                bq.ScalarQueryParameter('created_at', 'TIMESTAMP', date),
                bq.ScalarQueryParameter('budget', 'INT64', budget),
                # we only use AUD in budget case
                bq.ScalarQueryParameter('currency', 'STRING', 'AUD'),
            ]

            query = f"""INSERT INTO {SM_GCP_BQ_BUDGET_VIEW}
            (gcp_project, created_at, budget, currency)
            VALUES (@project_name, @created_at, @budget, @currency)
            """
            logger.info(f'Executing {query}')
            query_job = bq_client.query(
                query, job_config=bq.QueryJobConfig(query_parameters=query_params)
            )
            query_job.result()
            logger.info(f'Inserted {project_name}, {date}, {budget}')

    logger.info('Done')


def main():
    """
    Expect path to cpg-infrastructure-private folder as command line argument
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p',
        '--cpg_infra_path',
        help='Path to cpg-infrastructure-private folder',
        type=str,
    )
    args = parser.parse_args()
    cpg_infra_path: str = args.cpg_infra_path

    if not os.path.isdir(cpg_infra_path):
        print(f'{cpg_infra_path} is not a directory')
        sys.exit(1)

    # process budget history
    process(cpg_infra_path)


if __name__ == '__main__':
    # check env vars
    if not SM_GCP_BQ_BUDGET_VIEW:
        print('SM_GCP_BQ_BUDGET_VIEW is not set')
        sys.exit(1)
    if not SM_GCP_BQ_AGGREG_VIEW:
        print('SM_GCP_BQ_AGGREG_VIEW is not set')
        sys.exit(1)

    # execute main function
    main()
