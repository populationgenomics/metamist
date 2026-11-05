"""
This script goes through all the commit history to cpg-infrastructure-private and
check for changes to budgets.yaml files for each of the projects.
Once collected it gets checked agains budget BQ table and
it inserts any missing records.
At this stage it does not delete any records from BQ table.

example of usage:
# generate the billing data for 2023:

# aggregate and compare by topic/service, include all charges
python scripts/billing_compare.py -yr 2023 -t 1 -s 1 -o billing-2023-topic-service.csv

# aggregate and compare by service only
python scripts/billing_compare.py -yr 2023 -t 0 -s 1 -o billing-2023-service.csv

# aggregate and compare by tax-only service charges
python scripts/billing_compare.py -yr 2023 -t 1 -s 1 -tx 1 -o billing-2023-tax-only.csv

"""

import argparse
import logging
import os
import re
import sys

import google.cloud.bigquery as bq
import numpy as np
import pandas as pd

# name of the BQ table to insert the records
GCP_BILLING_SOURCE_TABLE = os.getenv('GCP_BILLING_SOURCE_TABLE')
SM_GCP_BQ_AGGREG_RAW = os.getenv('SM_GCP_BQ_AGGREG_RAW')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# print logs to terminal as well
logger.addHandler(logging.StreamHandler())


TAX_ONLY_FILTER = "AND service.description = 'Invoice' AND sku.description = 'Tax'"


def generate_invoice_months(yr: str) -> str:
    """
    Generate the invoice months for the year
    """
    return "','".join([f'{yr}{str(m).zfill(2)}' for m in range(1, 13)])


def generate_group_by(columns: list) -> str:
    """
    Generate the group by clause
    """
    return ','.join([str(r) for r in range(1, len(columns) + 1)])


def set_index(
    df: pd.DataFrame,
    include_topic: bool,
    include_service: bool,
) -> pd.DataFrame:
    """
    Set the index for the dataframe based on selected columns
    """
    if include_topic and include_service:
        df['idx'] = (
            df['month'].astype(str)
            + '|'
            + df['topic'].astype(str)
            + '|'
            + df['service'].astype(str)
            + '|'
            + df['service_id'].astype(str)
        )
    elif include_topic:
        df['idx'] = df['month'].astype(str) + '|' + df['topic'].astype(str)
    elif include_service:
        df['idx'] = (
            # set service first so we can easy sort in the final sprradsheet
            df['service'].astype(str)
            + '|'
            + df['month'].astype(str)
            + '|'
            + df['service_id'].astype(str)
        )
    else:
        df['idx'] = df['month'].astype(str)
    return df.set_index('idx')


def get_metamist_billing(
    yr: str,
    include_topic: bool = True,
    include_service: bool = False,
    in_tax_only: bool = False,
) -> pd.DataFrame:
    """
    Get the billing data from metamist BQ table
    If tax only is set then only include the tax records
    """
    columns = ['invoice.month as month']
    if include_topic:
        columns.append('topic')
    if include_service:
        columns.append('service.id as service_id')
        columns.append('service.description as service')

    query = f"""
        SELECT {','.join(columns)}, sum(cost) as metamist_total_cost
        FROM `{SM_GCP_BQ_AGGREG_RAW}`
        WHERE
        -- only consider the last few days before and after to limit the size of the data
        DATE_TRUNC(usage_end_time, DAY) >= TIMESTAMP("{int(yr) - 1}-12-28")
        AND DATE_TRUNC(usage_end_time, DAY) <= TIMESTAMP("{int(yr) + 1}-01-05")
        AND invoice.month in ('{generate_invoice_months(yr)}')
        {TAX_ONLY_FILTER if in_tax_only else ''}
        GROUP BY {generate_group_by(columns)};
    """
    # print(query)
    bq_client = bq.Client()
    df = bq_client.query(query).to_dataframe()
    return set_index(df, include_topic, include_service)


def get_gcp_billing(
    yr: str,
    include_topic: bool = True,
    include_service: bool = False,
    in_tax_only: bool = False,
) -> pd.DataFrame:
    """
    Get the billing data from GCP BQ table
    """
    columns = ['invoice.month as month']
    if include_topic:
        columns.append('project.name as gcp_project_name')
    if include_service:
        columns.append('service.id as service_id')
        columns.append('service.description as service')

    query = f"""
        SELECT {','.join(columns)}, sum(cost) as gcp_total_cost
        FROM `{GCP_BILLING_SOURCE_TABLE}`
        WHERE
        -- only consider the last few days before and after to limit the size of the data
        TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP("{int(yr) - 1}-12-28")
        AND TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) <= TIMESTAMP("{int(yr) + 1}-01-05")
        AND invoice.month in ('{generate_invoice_months(yr)}')
        {TAX_ONLY_FILTER if in_tax_only else ''}
        GROUP BY {generate_group_by(columns)};
    """
    # print(query)
    bq_client = bq.Client()
    df = bq_client.query(
        query,
    ).to_dataframe()
    # create topic as project_name, remove the number suffix
    df['gcp_gcp'] = df['month']
    return_cols = []
    if include_topic:
        df['topic'] = (
            df['gcp_project_name']
            .fillna('')
            .apply(lambda x: re.sub(r'\d+$', '', x).rstrip('-'))
        )
        return_cols.append('gcp_project_name')
    if include_service:
        # include project service in the final df
        df['gcp_service_id'] = df['service_id']
        df['gcp_service'] = df['service']
        return_cols.append('gcp_service_id')
        return_cols.append('gcp_service')

    # total cost as last gcp column
    return_cols.append('gcp_total_cost')

    return set_index(df, include_topic, include_service)[return_cols]


def main():
    """
    Expect year and optional output path as command line argument
    """
    # check env vars
    if not GCP_BILLING_SOURCE_TABLE:
        print('GCP_BILLING_SOURCE_TABLE is not set')
        sys.exit(1)
    if not SM_GCP_BQ_AGGREG_RAW:
        print('SM_GCP_BQ_AGGREG_RAW is not set')
        sys.exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-yr',
        '--year',
        help='Which year to report',
        type=str,
    )
    parser.add_argument(
        '-o',
        '--output',
        help='CSV file name to save the output to',
        type=str,
    )
    parser.add_argument(
        '-t',
        '--topic',
        help='Include Topics',
        type=int,
        default=1,
    )
    parser.add_argument(
        '-s',
        '--service',
        help='Include Services',
        type=int,
        default=0,
    )
    parser.add_argument(
        '-tx',
        '--tax_only',
        help='Include Only TAX records',
        type=int,
        default=0,
    )
    args = parser.parse_args()
    if args.year is None:
        print('Missing year argument')
        sys.exit(1)

    in_year: str = args.year
    in_topic: bool = args.topic == 1
    in_service: bool = args.service == 1
    in_tax_only: bool = args.tax_only == 1

    # get
    m_df = get_metamist_billing(in_year, in_topic, in_service, in_tax_only)
    gcp_df = get_gcp_billing(in_year, in_topic, in_service, in_tax_only)

    total_metamist = np.sum(m_df.metamist_total_cost)
    total_gcp = np.sum(gcp_df.gcp_total_cost)

    logger.info(f'GCP Total: {total_gcp}')
    logger.info(f'Metamist Total: {total_metamist}')
    logger.info(f'Difference: {total_metamist - total_gcp}')
    logger.info('-------------------------------')

    combined_df = gcp_df.join(m_df, how='outer')
    combined_df = combined_df.fillna(0)
    combined_df['diff'] = (
        combined_df['metamist_total_cost'] - combined_df['gcp_total_cost']
    )
    pd.set_option('display.max_rows', None)
    print(combined_df.sort_values(by='diff', ascending=False, na_position='first'))
    if args.output is not None:
        combined_df.to_csv(args.output)


if __name__ == '__main__':
    # execute main function
    main()
