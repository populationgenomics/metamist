import * as React from 'react'
import { Link, useSearchParams, useNavigate, useLocation } from 'react-router-dom'
import { Table as SUITable, Message, Button, Checkbox, Dropdown, Grid } from 'semantic-ui-react'
import _ from 'lodash'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import Table from '../../shared/components/Table'
import { BillingApi, BillingColumn, BillingCostBudgetRecord } from '../../sm-api'

import './Billing.css'
import FieldSelector from './FieldSelector'

import { convertFieldName } from '../../shared/utilities/fieldName'

const BillingCurrentCost = () => {
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [openRows, setOpenRows] = React.useState<string[]>([])

    const [costRecords, setCosts] = React.useState<BillingCostBudgetRecord[]>([])
    const [error, setError] = React.useState<string | undefined>()
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: 'undefined',
        direction: 'undefined',
    })

    // Pull search params for use in the component
    const [searchParams] = useSearchParams()
    const inputGroupBy: string | null = searchParams.get('groupBy')
    const fixedGroupBy: BillingColumn = inputGroupBy ? inputGroupBy as BillingColumn : BillingColumn.GcpProject
    const inputInvoiceMonth = searchParams.get('invoiceMonth')

    // use navigate and update url params
    const location = useLocation();
    const navigate = useNavigate();

    const updateNav = (grp: BillingColumn, invoiceMonth: string | undefined) => {
        let url = `${location.pathname}?groupBy=${grp}`
        if (invoiceMonth) {
            url += `&invoiceMonth=${invoiceMonth}`
        }
        navigate(url)
    }

    // toISOString() will give you YYYY-MM-DDTHH:mm:ss.sssZ
    // toISOString().substring(0, 7) will give you YYYY-MM
    // .replace('-', '') will give you YYYYMM
    const thisMonth = new Date().toISOString().substring(0, 7).replace('-', '')

    const [groupBy, setGroupBy] = React.useState<BillingColumn>(fixedGroupBy ?? BillingColumn.GcpProject)
    const [invoiceMonth, setInvoiceMonth] = React.useState<string>(inputInvoiceMonth ?? thisMonth)

    const onGroupBySelect = (event: any, data: any) => {
        setGroupBy(data.value)
        getCosts(data.value, invoiceMonth)
    }

    const onInvoiceMonthSelect = (event: any, data: any) => {
        setInvoiceMonth(data.value)
        getCosts(groupBy, data.value)
    }

    const getCosts = (grp: BillingColumn, invoiceMonth: string | undefined) => {
        updateNav(groupBy, invoiceMonth)
        setIsLoading(true)
        setError(undefined)
        new BillingApi()
            .getRunningCost(grp, invoiceMonth)
            .then((response) => {
                setIsLoading(false)
                setCosts(response.data)
            })
            .catch((er) => setError(er.message))
    }

    React.useEffect(() => {
        getCosts(groupBy, invoiceMonth)
    }, [])

    const HEADER_FIELDS = [
        { category: 'field', title: groupBy.toUpperCase() },
        { category: 'compute_daily', title: 'C' },
        { category: 'storage_daily', title: 'S' },
        { category: 'total_daily', title: 'Total' },
        { category: 'compute_monthly', title: 'C' },
        { category: 'storage_monthly', title: 'S' },
        { category: 'total_monthly', title: 'Total' },
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

    function capitalize(str: string): string {
        if (str === 'gcp_project') {
            return 'GCP-Project'
        }
        return str.charAt(0).toUpperCase() + str.slice(1)
    }

    if (error)
        return (
            <Message negative>
                {error}
                <br />
                <Button color="red" onClick={() => getCosts(groupBy, invoiceMonth)}>
                    Retry
                </Button>
            </Message>
        )

    if (isLoading)
        return (
            <div>
                <LoadingDucks />
                <p style={{ textAlign: 'center', marginTop: '5px' }}>
                    <em>This query takes a while...</em>
                </p>
            </div>
        )

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
        return `/billing/costByTime?groupBy=${groupBy}&selectedData=${data}`
    }

    return (
        <>
            <h1>Billing By Invoice Month</h1>

            <Grid columns='equal'>
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
            </Grid>


            <Table celled compact sortable>
                <SUITable.Header>
                    <SUITable.Row>
                        <SUITable.HeaderCell></SUITable.HeaderCell>

                        <SUITable.HeaderCell></SUITable.HeaderCell>

                        <SUITable.HeaderCell colSpan="3">24H</SUITable.HeaderCell>
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

                        {HEADER_FIELDS.map((k) => (
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
                                {k.title}
                            </SUITable.HeaderCell>
                        ))}

                        {groupBy === BillingColumn.GcpProject ? (
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
                            <SUITable.Row>
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
                                                <SUITable.Cell>
                                                    <b>
                                                        <Link
                                                            to={linkTo(p[k.category])}
                                                        >
                                                            {p[k.category]}
                                                        </Link>
                                                    </b>
                                                </SUITable.Cell>
                                            )
                                        default:
                                            return (
                                                <SUITable.Cell>
                                                    {currencyFormat(p[k.category])}
                                                </SUITable.Cell>
                                            )
                                    }
                                })}

                                {groupBy === BillingColumn.GcpProject ? (
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
                                    >
                                        <SUITable.Cell style={{ border: 'none' }} />
                                        <SUITable.Cell>{dk.cost_category}</SUITable.Cell>

                                        {dk.cost_group === 'C' ? (
                                            <React.Fragment>
                                                <SUITable.Cell>
                                                    {currencyFormat(dk.daily_cost)}
                                                </SUITable.Cell>

                                                <SUITable.Cell colSpan="2" />

                                                <SUITable.Cell>
                                                    {currencyFormat(dk.monthly_cost)}
                                                </SUITable.Cell>
                                                <SUITable.Cell colSpan="2" />
                                            </React.Fragment>
                                        ) : (
                                            <React.Fragment>
                                                <SUITable.Cell />
                                                <SUITable.Cell>
                                                    {currencyFormat(dk.daily_cost)}
                                                </SUITable.Cell>

                                                <SUITable.Cell colSpan="2" />
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
        </>
    )
}

export default BillingCurrentCost
