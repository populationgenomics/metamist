"""
This script goes through the billing records and prints out batchIds for which we missing billing records
"""

import argparse
import json
import logging
import os
import sys

import google.cloud.bigquery as bq

# name of the BQ table to insert the records
SM_GCP_BQ_BATCHES_VIEW = os.getenv('SM_GCP_BQ_BATCHES_VIEW')
GCP_PROJECT = os.getenv('GCP_PROJECT')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# print logs to terminal as well
logger.addHandler(logging.StreamHandler())


def get_min_max_batch_id(
    from_day: str,
) -> tuple[int | None, int | None]:
    """
    Get min and max batch id from given day
    """

    query = f"""
        SELECT MIN(CAST(batch_id as INT)) as min_batch_id, MAX(CAST(batch_id as INT)) as max_batch_id
        FROM `{SM_GCP_BQ_BATCHES_VIEW}`
        WHERE min_day > @min_day
    """
    print(query)
    bq_client = bq.Client()
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter('min_day', 'STRING', from_day),
        ],
    )
    query_job_result = bq_client.query(query, job_config=job_config)
    if query_job_result:
        for row in query_job_result:
            return dict(row)['min_batch_id'], dict(row)['max_batch_id']

    return None, None


def get_missing_batches(
    from_day: str,
) -> list[str]:
    """
    Get the list of batchIds not present in the billing records
    """
    min_batch_id, max_batch_id = get_min_max_batch_id(from_day)
    if not min_batch_id or not max_batch_id:
        return []

    query = f"""WITH b as (
            SELECT CAST(batch_id as STRING) AS batch_id
            FROM UNNEST(GENERATE_ARRAY(@min_batch_id, @max_batch_id)) AS batch_id
        ),
        t as (
            SELECT b.batch_id
            FROM b LEFT JOIN
            `{SM_GCP_BQ_BATCHES_VIEW}` d on d.batch_id = b.batch_id
            WHERE d.batch_id IS NULL
        )
        SELECT batch_id from t
        order by 1 asc
    """
    print(query)
    bq_client = bq.Client()
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter('min_batch_id', 'INT64', min_batch_id),
            bq.ScalarQueryParameter('max_batch_id', 'INT64', max_batch_id),
        ],
    )
    query_job_result = bq_client.query(query, job_config=job_config)
    results = []
    if query_job_result:
        for row in query_job_result:
            results.append(str(dict(row)['batch_id']))

    return results


def get_hail_token() -> str:
    """
    TODO Get Hail token from local tokens file
    """
    # if os.getenv('DEV') in ('1', 'true', 'yes'):
    #     with open(os.path.expanduser('~/.hail/tokens.json'), encoding='utf-8') as f:
    #         config = json.load(f)
    #         return config['default']

    # assert GCP_PROJECT
    # secret_value = read_secret(
    #     GCP_PROJECT,
    #     'aggregate-billing-hail-token',
    #     fail_gracefully=False,
    # )
    # if not secret_value:
    #     raise ValueError('Could not find Hail token')

    # return secret_value
    return ''


def main():
    """
    Expect year and optional output path as command line argument
    """
    # check env vars
    if not SM_GCP_BQ_BATCHES_VIEW:
        print('SM_GCP_BQ_BATCHES_VIEW is not set')
        sys.exit(1)

    if not GCP_PROJECT:
        print('GCP_PROJECT is not set')
        sys.exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-st',
        '--start',
        help='Start date to check from',
        type=str,
    )
    args = parser.parse_args()
    if args.start is None:
        print('Missing start argument')
        sys.exit(1)

    start_date: str = args.start

    missing_batches = get_missing_batches(start_date)
    if not missing_batches:
        print('No missing batches found')
        return

    # we need to cross check with Hail Batch API to see if the batch information is available
    # otherwise we would get alerts for missing batches when reloading
    batches_to_be_loaded = []
    # for b in missing_batches:
    # TODO: check if batch is available in Hail
    batches_to_be_loaded = missing_batches

    # group the batches to be loaded
    # if the difference between two consecutive batches is more than 50, then start a new group
    # This is to avoid loading too many batches in a single run
    # 50 is just arbitrary number, can be changed if needed
    batches_group = []
    prev_batch_id = None
    for b in batches_to_be_loaded:
        if prev_batch_id is None or int(b) - int(prev_batch_id) > 50:
            # add new group
            batches_group.append([b])
        else:
            batches_group[-1].append(b)

        prev_batch_id = b

    print('Batches to be loaded, here are relevent URL calls to be made manually:')
    for i, group in enumerate(batches_group):
        print(f'Group {i+1}: {group}')

        print(
            f"""
        curl -X 'POST' 'https://australia-southeast1-billing-admin-290403.cloudfunctions.net/billing-aggregator-hail-billing-function-e174484' \
        -H 'accept: application/json' \
        -H 'Content-Type: application/json' \
        -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
        {f"-d '{json.dumps({'batch_ids': group})}'" if group else ''}
        """
        )


if __name__ == '__main__':
    # execute main function
    main()
