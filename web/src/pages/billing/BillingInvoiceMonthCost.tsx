import * as React from 'react'
import { Link, useSearchParams, useNavigate, useLocation } from 'react-router-dom'
import { Table as SUITable, Message, Button, Checkbox, Dropdown, Grid } from 'semantic-ui-react'
import _ from 'lodash'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import Table from '../../shared/components/Table'
import { BillingApi, BillingColumn, BillingCostBudgetRecord } from '../../sm-api'
import FieldSelector from './components/FieldSelector'
import { convertFieldName } from '../../shared/utilities/fieldName'
import { HorizontalStackedBarChart } from '../../shared/components/Graphs/HorizontalStackedBarChart'
import generateUrl from '../../shared/utilities/generateUrl'

const BillingCurrentCost = () => {
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [openRows, setOpenRows] = React.useState<string[]>([])

    const [costRecords, setCosts] = React.useState<BillingCostBudgetRecord[]>([])
    const [error, setError] = React.useState<string | undefined>()
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: 'undefined',
        direction: 'undefined',
    })

    const [showAsChart, setShowAsChart] = React.useState<boolean>(true)

    // Pull search params for use in the component
    const [searchParams] = useSearchParams()
    const inputGroupBy: string | null = searchParams.get('groupBy')
    const fixedGroupBy: BillingColumn = inputGroupBy
        ? (inputGroupBy as BillingColumn)
        : BillingColumn.GcpProject
    const inputInvoiceMonth = searchParams.get('invoiceMonth')

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (grp: BillingColumn, invoiceMonth: string | undefined) => {
        const url = generateUrl(location, {
            groupBy: grp,
            invoiceMonth: invoiceMonth,
        })
        navigate(url)
    }

    // toISOString() will give you YYYY-MM-DDTHH:mm:ss.sssZ
    // toISOString().substring(0, 7) will give you YYYY-MM
    // .replace('-', '') will give you YYYYMM
    const thisMonth = new Date().toISOString().substring(0, 7).replace('-', '')

    const [groupBy, setGroupBy] = React.useState<BillingColumn>(
        fixedGroupBy ?? BillingColumn.GcpProject
    )
    const [invoiceMonth, setInvoiceMonth] = React.useState<string>(inputInvoiceMonth ?? thisMonth)

    const [lastLoadedDay, setLastLoadedDay] = React.useState<string>()

    const getCosts = (grp: BillingColumn, invoiceMth: string | undefined) => {
        updateNav(grp, invoiceMth)
        setIsLoading(true)
        setError(undefined)
        let source = 'aggregate'
        if (grp === BillingColumn.GcpProject) {
            source = 'gcp_billing'
        }
        new BillingApi()
            .getRunningCost(grp, invoiceMth, source)
            .then((response) => {
                setIsLoading(false)
                setCosts(response.data)
                setLastLoadedDay(response.data[0].last_loaded_day)
            })
            .catch((er) => setError(er.message))
    }

    const onGroupBySelect = (event: any, data: any) => {
        setGroupBy(data.value)
        getCosts(data.value, invoiceMonth)
    }

    const onInvoiceMonthSelect = (event: any, data: any) => {
        setInvoiceMonth(data.value)
        getCosts(groupBy, data.value)
    }

    React.useEffect(() => {
        getCosts(groupBy, invoiceMonth)
    }, [])

    const HEADER_FIELDS = [
        { category: 'field', title: groupBy.toUpperCase(), show_always: true },
        { category: 'compute_daily', title: 'C', show_always: false },
        { category: 'storage_daily', title: 'S', show_always: false },
        { category: 'total_daily', title: 'Total', show_always: false },
        { category: 'compute_monthly', title: 'C', show_always: true },
        { category: 'storage_monthly', title: 'S', show_always: true },
        { category: 'total_monthly', title: 'Total', show_always: true },
    ]

    const handleToggle = (field: string) => {
        if (!openRows.includes(field)) {
            setOpenRows([...openRows, field])
        } else {
            setOpenRows(openRows.filter((i) => i !== field))
        }
    }

    function currencyFormat(num: number): string {
        if (num === undefined || num === null) {
            return ''
        }

        return `$${num.toFixed(2).replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,')}`
    }

    function percFormat(num: number): string {
        if (num === undefined || num === null) {
            return ''
        }

        return `${num.toFixed(0).toString()} % `
    }

    if (error)
        return (
            <Message negative>
                {error}
                <br />
                <Button negative onClick={() => getCosts(groupBy, invoiceMonth)}>
                    Retry
                </Button>
            </Message>
        )

    const rowColor = (p: BillingCostBudgetRecord) => {
        if (p.budget_spent === undefined || p.budget_spent === null) {
            return ''
        }
        if (p.budget_spent > 90) {
            return 'billing-over-budget'
        }
        if (p.budget_spent > 50) {
            return 'billing-half-budget'
        }
        return 'billing-under-budget'
    }

    const handleSort = (clickedColumn: string) => {
        if (sort.column !== clickedColumn) {
            setSort({ column: clickedColumn, direction: 'ascending' })
            return
        }
        if (sort.direction === 'ascending') {
            setSort({ column: clickedColumn, direction: 'descending' })
            return
        }
        setSort({ column: null, direction: null })
    }

    const checkDirection = (category: string) => {
        if (sort.column === category && sort.direction !== null) {
            return sort.direction === 'ascending' ? 'ascending' : 'descending'
        }
        return undefined
    }

    const linkTo = (data: string) => {
        // convert invoice month to start and end dates
        const year = invoiceMonth.substring(0, 4)
        const month = invoiceMonth.substring(4, 6)
        let nextYear = year
        let nextMonth = (parseInt(month, 10) + 1).toString().padStart(2, '0')
        if (month === '12') {
            nextYear = (parseInt(year, 10) + 1).toString()
            nextMonth = '01'
        }
        const startDate = `${year}-${month}-01`
        const nextMth = new Date(`${nextYear}-${nextMonth}-01`)
        nextMth.setDate(-0.01)
        const endDate = nextMth.toISOString().substring(0, 10)
        return `/billing/costByTime?groupBy=${groupBy}&selectedData=${data}&start=${startDate}&end=${endDate}`
    }

    return (
        <>
            <h1>Cost By Invoice Month</h1>

            <Grid columns="equal" stackable doubling>
                <Grid.Column>
                    <FieldSelector
                        label="Group By"
                        fieldName="Group"
                        onClickFunction={onGroupBySelect}
                        selected={groupBy}
                    />
                </Grid.Column>
                <Grid.Column>
                    <FieldSelector
                        label="Invoice Month"
                        fieldName={BillingColumn.InvoiceMonth}
                        onClickFunction={onInvoiceMonthSelect}
                        selected={invoiceMonth}
                    />
                </Grid.Column>

                <Grid.Column>
                    <Checkbox
                        label="Show as Chart / Table"
                        fitted
                        toggle
                        checked={showAsChart}
                        onChange={() => setShowAsChart(!showAsChart)}
                    />
                </Grid.Column>
            </Grid>

            {(() => {
                if (!showAsChart) return null
                if (String(invoiceMonth) === String(thisMonth)) {
                    return (
                        <Grid columns={2} stackable doubling>
                            <Grid.Column width={8} className="chart-card">
                                <HorizontalStackedBarChart
                                    data={costRecords}
                                    title={`24H (day UTC ${lastLoadedDay})`}
                                    series={['compute_daily', 'storage_daily']}
                                    labels={['Compute', 'Storage']}
                                    total_series="total_daily"
                                    threshold_values={[90, 50]}
                                    threshold_series="budget_spent"
                                    sorted_by="total_monthly"
                                    isLoading={isLoading}
                                    showLegend={false}
                                />
                            </Grid.Column>
                            <Grid.Column width={8} className="chart-card donut-chart">
                                <HorizontalStackedBarChart
                                    data={costRecords}
                                    title="Invoice Month (Acc)"
                                    series={['compute_monthly', 'storage_monthly']}
                                    labels={['Compute', 'Storage']}
                                    total_series="total_monthly"
                                    threshold_values={[90, 50]}
                                    threshold_series="budget_spent"
                                    sorted_by="total_monthly"
                                    isLoading={isLoading}
                                    showLegend={true}
                                />
                            </Grid.Column>
                        </Grid>
                    )
                }
                return (
                    <Grid>
                        <Grid.Column width={12}>
                            <HorizontalStackedBarChart
                                data={costRecords}
                                title="Invoice Month (Acc)"
                                series={['compute_monthly', 'storage_monthly']}
                                labels={['Compute', 'Storage']}
                                total_series="total_monthly"
                                sorted_by="total_monthly"
                                isLoading={isLoading}
                                showLegend={true}
                            />
                        </Grid.Column>
                    </Grid>
                )
            })()}

            {!showAsChart ? (
                <Table celled compact sortable>
                    <SUITable.Header>
                        <SUITable.Row>
                            <SUITable.HeaderCell></SUITable.HeaderCell>

                            <SUITable.HeaderCell></SUITable.HeaderCell>

                            {invoiceMonth === thisMonth ? (
                                <SUITable.HeaderCell colSpan="3">
                                    24H (day UTC {lastLoadedDay})
                                </SUITable.HeaderCell>
                            ) : null}

                            {groupBy === BillingColumn.GcpProject ? (
                                <SUITable.HeaderCell colSpan="4">
                                    Invoice Month (Acc)
                                </SUITable.HeaderCell>
                            ) : (
                                <SUITable.HeaderCell colSpan="3">
                                    Invoice Month (Acc)
                                </SUITable.HeaderCell>
                            )}
                        </SUITable.Row>
                        <SUITable.Row>
                            <SUITable.HeaderCell></SUITable.HeaderCell>

                            {HEADER_FIELDS.map((k) => {
                                switch (k.show_always || invoiceMonth === thisMonth) {
                                    case true:
                                        return (
                                            <SUITable.HeaderCell
                                                key={k.category}
                                                sorted={checkDirection(k.category)}
                                                onClick={() => handleSort(k.category)}
                                                style={{
                                                    borderBottom: 'none',
                                                    position: 'sticky',
                                                    resize: 'horizontal',
                                                }}
                                            >
                                                {convertFieldName(k.title)}
                                            </SUITable.HeaderCell>
                                        )
                                    default:
                                        return null
                                }
                            })}

                            {groupBy === BillingColumn.GcpProject && invoiceMonth === thisMonth ? (
                                <SUITable.HeaderCell
                                    key={'budget_spent'}
                                    sorted={checkDirection('budget_spent')}
                                    onClick={() => handleSort('budget_spent')}
                                    style={{
                                        borderBottom: 'none',
                                        position: 'sticky',
                                        resize: 'horizontal',
                                    }}
                                >
                                    Budget Spend %
                                </SUITable.HeaderCell>
                            ) : null}
                        </SUITable.Row>
                    </SUITable.Header>
                    <SUITable.Body>
                        {_.orderBy(
                            costRecords,
                            [sort.column],
                            sort.direction === 'ascending' ? ['asc'] : ['desc']
                        ).map((p) => (
                            <React.Fragment key={`total - ${p.field}`}>
                                <SUITable.Row
                                    className={`${rowColor(p)}`}
                                    key={`total-row-${p.field}`}
                                    id={`total-row-${p.field}`}
                                >
                                    <SUITable.Cell collapsing>
                                        <Checkbox
                                            checked={openRows.includes(p.field)}
                                            slider
                                            onChange={() => handleToggle(p.field)}
                                        />
                                    </SUITable.Cell>
                                    {HEADER_FIELDS.map((k) => {
                                        switch (k.category) {
                                            case 'field':
                                                return (
                                                    <SUITable.Cell className="billing-href">
                                                        <b>
                                                            <Link to={linkTo(p[k.category])}>
                                                                {p[k.category]}
                                                            </Link>
                                                        </b>
                                                    </SUITable.Cell>
                                                )
                                            default:
                                                switch (
                                                    k.show_always ||
                                                    invoiceMonth === thisMonth
                                                ) {
                                                    case true:
                                                        return (
                                                            <SUITable.Cell>
                                                                {currencyFormat(p[k.category])}
                                                            </SUITable.Cell>
                                                        )
                                                    default:
                                                        return null
                                                }
                                        }
                                    })}

                                    {groupBy === BillingColumn.GcpProject &&
                                    invoiceMonth === thisMonth ? (
                                        <SUITable.Cell>{percFormat(p.budget_spent)}</SUITable.Cell>
                                    ) : null}
                                </SUITable.Row>
                                {typeof p === 'object' &&
                                    'details' in p &&
                                    _.orderBy(p?.details, ['monthly_cost'], ['desc']).map((dk) => (
                                        <SUITable.Row
                                            style={{
                                                display: openRows.includes(p.field)
                                                    ? 'table-row'
                                                    : 'none',
                                                backgroundColor: 'var(--color-bg)',
                                            }}
                                            key={`${dk.cost_category} - ${p.field}`}
                                            id={`${dk.cost_category} - ${p.field}`}
                                        >
                                            <SUITable.Cell style={{ border: 'none' }} />
                                            <SUITable.Cell>{dk.cost_category}</SUITable.Cell>

                                            {dk.cost_group === 'C' ? (
                                                <React.Fragment>
                                                    {invoiceMonth === thisMonth ? (
                                                        <React.Fragment>
                                                            <SUITable.Cell>
                                                                {currencyFormat(dk.daily_cost)}
                                                            </SUITable.Cell>

                                                            <SUITable.Cell colSpan="2" />
                                                        </React.Fragment>
                                                    ) : null}
                                                    <SUITable.Cell>
                                                        {currencyFormat(dk.monthly_cost)}
                                                    </SUITable.Cell>
                                                    <SUITable.Cell colSpan="2" />
                                                </React.Fragment>
                                            ) : (
                                                <React.Fragment>
                                                    <SUITable.Cell />
                                                    {invoiceMonth === thisMonth ? (
                                                        <React.Fragment>
                                                            <SUITable.Cell>
                                                                {currencyFormat(dk.daily_cost)}
                                                            </SUITable.Cell>

                                                            <SUITable.Cell colSpan="2" />
                                                        </React.Fragment>
                                                    ) : null}
                                                    <SUITable.Cell>
                                                        {currencyFormat(dk.monthly_cost)}
                                                    </SUITable.Cell>
                                                    <SUITable.Cell />
                                                </React.Fragment>
                                            )}

                                            {groupBy === BillingColumn.GcpProject ? (
                                                <SUITable.Cell />
                                            ) : null}
                                        </SUITable.Row>
                                    ))}
                            </React.Fragment>
                        ))}
                    </SUITable.Body>
                </Table>
            ) : null}
        </>
    )
}

export default BillingCurrentCost
