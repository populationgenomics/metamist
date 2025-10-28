# This file contains billing specific functions which are common
# across different billing tables and views
# in Big Query.
from collections import namedtuple

from google.cloud import bigquery

from api.settings import (
    BQ_DAYS_BACK_OPTIMAL,
)
from api.utils.db import (
    Connection,
)
from db.python.tables.bq.function_bq_filter import FunctionBQFilter
from models.enums import BillingTimeColumn, BillingTimePeriods
from models.models import (
    BillingColumn,
    BillingCostBudgetRecord,
    BillingCostDetailsRecord,
    BillingTotalCostQueryModel,
)

# constants to abbrevate (S)tores and (C)ompute
STORAGE = 'S'
COMPUTE = 'C'

# Day Time details used in grouping and parsing formulas
TimeGroupingDetails = namedtuple(
    'TimeGroupingDetails', ['field', 'formula', 'separator']
)


def abbrev_cost_category(cost_category: str) -> str:
    """abbreviate cost category"""
    return STORAGE if cost_category.startswith('Cloud Storage') else COMPUTE


def prepare_time_periods(
    query: BillingTotalCostQueryModel,
) -> TimeGroupingDetails:
    """Prepare Time periods grouping and parsing formulas"""
    time_column: BillingTimeColumn = query.time_column or BillingTimeColumn.DAY

    # Based on specified time period, add the corresponding column
    if query.time_periods == BillingTimePeriods.DAY:
        return TimeGroupingDetails(
            field=f'FORMAT_DATE("%Y-%m-%d", {time_column.value}) as day',
            formula='PARSE_DATE("%Y-%m-%d", day) as day',
            separator=',',
        )

    if query.time_periods == BillingTimePeriods.WEEK:
        return TimeGroupingDetails(
            field=f'FORMAT_DATE("%Y%W", {time_column.value}) as day',
            formula='PARSE_DATE("%Y%W", day) as day',
            separator=',',
        )

    if query.time_periods == BillingTimePeriods.MONTH:
        return TimeGroupingDetails(
            field=f'FORMAT_DATE("%Y%m", {time_column.value}) as day',
            formula='PARSE_DATE("%Y%m", day) as day',
            separator=',',
        )

    if query.time_periods == BillingTimePeriods.INVOICE_MONTH:
        return TimeGroupingDetails(
            field='invoice_month as day',
            formula='PARSE_DATE("%Y%m", day) as day',
            separator=',',
        )

    return TimeGroupingDetails('', '', '')


def time_optimisation_parameter() -> bigquery.ScalarQueryParameter:
    """
    BQ tables and views are partitioned by day, to avoid full scans
    we need to limit the amount of data scanned
    """
    return bigquery.ScalarQueryParameter('days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL))


async def get_topic_to_project_map(
    connection: Connection | None = None,
) -> dict[str, str]:
    """Get project_id to dataset/topic mapping"""
    result: dict[str, str] = {}
    if not connection:
        return result

    # run the SQL to get all topics and their projects
    _query = """select id as project_id, dataset from project;"""
    _query_results = await connection.connection.fetch_all(
        _query,
    )

    for row in _query_results:
        result[row['dataset']] = row['project_id']

    return result


async def append_sample_cost_record(
    results: list[dict],
    label: str,
    sample_counts_per_month: dict,
    row: dict,
    invoice_month: str,
    project_id: int,
    include_cost_category: str | None = None,
    exclude_cost_category: str | None = None,
):
    """Add a sample cost record to the results.

    Args:
        results (list[dict]): The list of result records.
        label (str): The label to assign to the new record.
        sample_counts_per_month (dict): The sample counts per month.
        row (dict): The original row to copy.
        invoice_month (str): The invoice month to filter by.
        project_id (int): The project ID to filter by.
        include_cost_category (str | None): The cost category to include.
        exclude_cost_category (str | None): The cost category to exclude.
    Returns:
        list[dict]: The updated list of result records.
    """
    if row.get('cost_category').startswith('Average Sample'):
        # skip sample average cost rows to avoid double counting
        return results

    sample_count = sample_counts_per_month.get(project_id, {}).get(invoice_month, 0)
    avg_cost = row.get('cost', 0) / sample_count if sample_count > 0 else 0
    # create new row based on passed row
    # with the same topic and day but with cost_category = label
    # and cost = avg_cost
    new_row = row.copy()
    new_row['cost_category'] = label
    new_row['cost'] = avg_cost

    if include_cost_category and row.get('cost_category') == include_cost_category:
        # When include_cost_category is defined, we only need to add average sample cost for that category
        results.append(new_row)

    elif exclude_cost_category and row.get('cost_category') != exclude_cost_category:
        # When exclude_cost_category is defined,
        # we need to sum all the costs except exclude_cost_category and group by topic and day
        # if topic and day does not exist, add new row into results
        # otherwise add cost to existing row
        found = False
        for existing_row in results:
            if (
                existing_row.get('topic') == new_row.get('topic')
                and existing_row.get('day') == new_row.get('day')
                and existing_row.get('cost_category') == label
            ):
                existing_row['cost'] += new_row['cost']
                found = True
                break

        if not found:
            results.append(new_row)

    return results


async def append_total_running_cost(
    field: BillingColumn,
    is_current_month: bool,
    last_loaded_day: str | None,
    total_monthly: dict,
    total_daily: dict,
    total_monthly_category: dict,
    total_daily_category: dict,
    results: list[BillingCostBudgetRecord],
) -> list[BillingCostBudgetRecord]:
    """
    Add total row: compute + storage to the results
    """
    # construct ALL fields details
    all_details = []
    for cat, mth_cost in total_monthly_category.items():
        all_details.append(
            BillingCostDetailsRecord(
                cost_group=abbrev_cost_category(cat),
                cost_category=cat,
                daily_cost=total_daily_category[cat] if is_current_month else None,
                monthly_cost=mth_cost,
            )
        )

    # Generate appropriate title based on whether filters are applied
    # Always use consistent "All X" naming for aggregate rows
    field_title = f'{BillingColumn.generate_all_title(field)}'

    # add total row: compute + storage
    results.append(
        BillingCostBudgetRecord(
            field=field_title,
            total_monthly=(
                total_monthly[COMPUTE]['ALL'] + total_monthly[STORAGE]['ALL']
            ),
            total_daily=(
                (total_daily[COMPUTE]['ALL'] + total_daily[STORAGE]['ALL'])
                if is_current_month
                else None
            ),
            compute_monthly=total_monthly[COMPUTE]['ALL'],
            compute_daily=((total_daily[COMPUTE]['ALL']) if is_current_month else None),
            storage_monthly=total_monthly[STORAGE]['ALL'],
            storage_daily=((total_daily[STORAGE]['ALL']) if is_current_month else None),
            details=all_details,
            budget_spent=None,
            budget=None,
            last_loaded_day=last_loaded_day,
        )
    )

    return results


async def append_detailed_cost_records(
    budgets_per_gcp_project: dict[str, float],
    is_current_month: bool,
    last_loaded_day: str | None,
    total_monthly: dict,
    total_daily: dict,
    field_details: dict,
    results: list[BillingCostBudgetRecord],
) -> list[BillingCostBudgetRecord]:
    """
    Add all the selected field rows: compute + storage to the results
    """
    # add rows by field
    for key, details in field_details.items():
        compute_daily = total_daily[COMPUTE][key] if key in total_daily[COMPUTE] else 0
        storage_daily = total_daily[STORAGE][key] if key in total_daily[STORAGE] else 0
        compute_monthly = (
            total_monthly[COMPUTE][key] if key in total_monthly[COMPUTE] else 0
        )
        storage_monthly = (
            total_monthly[STORAGE][key] if key in total_monthly[STORAGE] else 0
        )
        monthly = compute_monthly + storage_monthly
        budget_monthly = budgets_per_gcp_project.get(key)

        results.append(
            BillingCostBudgetRecord.from_json(
                {
                    'field': key,
                    'total_monthly': monthly,
                    'total_daily': (
                        (compute_daily + storage_daily) if is_current_month else None
                    ),
                    'compute_monthly': compute_monthly,
                    'compute_daily': compute_daily,
                    'storage_monthly': storage_monthly,
                    'storage_daily': storage_daily,
                    'details': details,
                    'budget_spent': (
                        100 * monthly / budget_monthly if budget_monthly else None
                    ),
                    'budget': budget_monthly,
                    'last_loaded_day': last_loaded_day,
                }
            )
        )

    return results


def prepare_order_by_string(order_by: dict[BillingColumn, bool] | None) -> str:
    """Prepare order by string"""
    if not order_by:
        return ''

    order_by_cols = []
    for order_field, reverse in order_by.items():
        col_name = str(order_field.value)
        col_order = 'DESC' if reverse else 'ASC'
        order_by_cols.append(f'{col_name} {col_order}')

    return f'ORDER BY {",".join(order_by_cols)}' if order_by_cols else ''


def prepare_aggregation(query: BillingTotalCostQueryModel) -> tuple[str, str]:
    """Prepare both fields for aggregation and group by string"""
    # Get columns to group by

    # if group by is populated, then we need to group by day as well
    grp_columns = ['day'] if query.group_by else []

    for field in query.fields:
        col_name = str(field.value)
        if not BillingColumn.can_group_by(field):
            # if the field cannot be grouped by, skip it
            continue

        # append to potential columns to group by
        grp_columns.append(col_name)

    fields_selected = ','.join(
        (field.value for field in query.fields if field != BillingColumn.COST)
    )

    grp_selected = ','.join(grp_columns)
    group_by = f'GROUP BY {grp_selected}' if query.group_by else ''

    return fields_selected, group_by


def prepare_labels_function(query: BillingTotalCostQueryModel):
    """Prepare labels function filter if labels filter is present"""
    if not query.filters:
        return None

    if BillingColumn.LABELS in query.filters and isinstance(
        query.filters[BillingColumn.LABELS], dict
    ):
        # prepare labels as function filters, parameterized both sides
        func_filter = FunctionBQFilter(
            name='getLabelValue',
            implementation="""
                CREATE TEMP FUNCTION getLabelValue(
                    labels ARRAY<STRUCT<key STRING, value STRING>>, label STRING
                ) AS (
                    (SELECT value FROM UNNEST(labels) WHERE key = label LIMIT 1)
                );
            """,
        )
        func_filter.to_sql(
            BillingColumn.LABELS,
            query.filters[BillingColumn.LABELS],
            query.filters_op,
        )
        return func_filter

    # otherwise
    return None


def filter_to_optimise_query() -> str:
    """Filter string to optimise BQ query"""
    return 'day >= TIMESTAMP(@start_day) AND day <= TIMESTAMP(@last_day)'


def last_loaded_day_filter() -> str:
    """Last Loaded day filter string"""
    return 'day = TIMESTAMP(@last_loaded_day)'


def convert_output(query_job_result):
    """Convert query result to json"""
    if not query_job_result or query_job_result.result().total_rows == 0:
        # return empty list if no record found
        return []

    records = query_job_result.result()
    results = []

    def transform_labels(row):
        return {r['key']: r['value'] for r in row}

    for record in records:
        drec = dict(record)
        if 'labels' in drec:
            drec.update(transform_labels(drec['labels']))

        results.append(drec)

    return results
