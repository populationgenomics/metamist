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
import numpy as np
import pandas as pd

# name of the BQ table to insert the records
GCP_BILLING_SOURCE_TABLE = os.getenv(
    'GCP_BILLING_SOURCE_TABLE',
    'billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343',
)
SM_GCP_BQ_AGGREG_RAW = os.getenv(
    'SM_GCP_BQ_AGGREG_RAW', 'billing-admin-290403.billing_aggregate.aggregate'
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# print logs to terminal as well
logger.addHandler(logging.StreamHandler())


def get_metamist_billing(yr: str) -> pd.DataFrame:
    """ """
    invoice_months = [f'{yr}{str(m).zfill(2)}' for m in range(1, 13)]
    query = f"""
        SELECT topic, invoice.month as month, sum(cost) as total_cost_metamist
        FROM `{SM_GCP_BQ_AGGREG_RAW}`
        WHERE 
        -- only consider the last few days before and after to limit the size of the data
        DATE_TRUNC(usage_end_time, DAY) >= TIMESTAMP("{int(yr) - 1}-12-28")
        AND DATE_TRUNC(usage_end_time, DAY) <= TIMESTAMP("{int(yr) + 1}-01-05")
        AND invoice.month in ('{"','".join(invoice_months)}')
        GROUP BY 1, 2
        ORDER BY 2, 1;
    """
    logger.debug(f'Executing {query}')
    bq_client = bq.Client()
    df = bq_client.query(
        query,
    ).to_dataframe()
    df['idx'] = df['month'].astype(str) + '|' + df['topic'].astype(str)
    return df.set_index('idx')


def get_gcp_billing(yr: str) -> pd.DataFrame:
    """ """
    invoice_months = [f'{yr}{str(m).zfill(2)}' for m in range(1, 13)]
    query = f"""
        SELECT project.name as project_name, invoice.month as month, sum(cost) as total_cost_gcp
        FROM `{GCP_BILLING_SOURCE_TABLE}`
        WHERE 
        -- only consider the last few days before and after to limit the size of the data
        TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP("{int(yr) - 1}-12-28")
        AND TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) <= TIMESTAMP("{int(yr) + 1}-01-05")
        AND invoice.month in ('{"','".join(invoice_months)}')
        GROUP BY 1, 2
        ORDER BY 2, 1;
    """
    logger.debug(f'Executing {query}')
    bq_client = bq.Client()
    df = bq_client.query(
        query,
    ).to_dataframe()
    # create topic as project_name, remove the number suffix
    df['topic'] = (
        df['project_name']
        .fillna('')
        .apply(lambda x: re.sub(r'\d+$', '', x).rstrip('-'))
    )
    df['idx'] = df['month'].astype(str) + '|' + df['topic'].astype(str)
    return df.set_index('idx')[['project_name', 'total_cost_gcp']]


def main():
    """
    Expect path to cpg-infrastructure-private folder as command line argument
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
    args = parser.parse_args()
    if args.year is None:
        print('Missing year argument')
        sys.exit(1)

    in_year: str = args.year

    # get
    m_df = get_metamist_billing(in_year)
    gcp_df = get_gcp_billing(in_year)

    total_metamist = np.sum(m_df.total_cost_metamist)
    total_gcp = np.sum(gcp_df.total_cost_gcp)

    logger.info(f'GCP Total: {total_gcp}')
    logger.info(f'Metamist Total: {total_metamist}')
    logger.info(f'Difference: {total_metamist - total_gcp}')
    logger.info('-------------------------------')

    combined_df = gcp_df.join(m_df, how='outer', lsuffix='_gcp', rsuffix='_metamist')
    combined_df = combined_df.fillna(0)
    combined_df['diff'] = (
        combined_df['total_cost_metamist'] - combined_df['total_cost_gcp']
    )
    pd.set_option('display.max_rows', None)
    print(combined_df.sort_values(by='diff', ascending=False, na_position='first'))
    if args.output is not None:
        combined_df.to_csv(args.output)


if __name__ == '__main__':
    # execute main function
    main()
