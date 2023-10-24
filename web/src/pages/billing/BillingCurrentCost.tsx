import * as React from 'react'
import { Table as SUITable, Message, Button, Checkbox, Dropdown } from 'semantic-ui-react'
import _ from 'lodash'
import Table from '../../shared/components/Table'
import { BillingApi, BillingColumn, BillingCostBudgetRecord } from '../../sm-api'

const BillingCurrentCost = () => {
    const [openRows, setOpenRows] = React.useState<string[]>([])

    const [costRecords, setCosts] = React.useState<BillingCostBudgetRecord[]>([])
    const [error, setError] = React.useState<string | undefined>()
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: 'undefined',
        direction: 'undefined',
    })

    const [groupBy, setGroupBy] = React.useState<BillingColumn>(BillingColumn.Project)

    const getCosts = (grp: BillingColumn) => {
        setError(undefined)
        new BillingApi()
            .getRunningCost(grp)
            .then((response) => {
                setCosts(response.data)
            })
            .catch((er) => setError(er.message))
    }

    React.useEffect(() => {
        getCosts(groupBy)
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

        return `${num.toFixed(0).toString()}%`
    }

    if (error)
        return (
            <Message negative>
                {error}
                <br />
                <Button color="red" onClick={() => getCosts(groupBy)}>
                    Retry
                </Button>
            </Message>
        )

    if (!costRecords) return <div>Loading...</div>

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

    const onGroupBySelect = (event: any, data: any) => {
        setGroupBy(data.value)
        getCosts(data.value)
    }

    const field_options = [
        { key: 1, text: 'By Project', value: BillingColumn.Project },
        { key: 2, text: 'By Topic', value: BillingColumn.Topic },
        { key: 3, text: 'By Dataset', value: BillingColumn.Dataset },
    ]

    return (
        <div>
            <h1>Billing Current Invoice Month</h1>

            <Dropdown
                selection
                fluid
                onChange={onGroupBySelect}
                value={groupBy}
                options={field_options}
            />
            <Table celled compact sortable>
                <SUITable.Header>
                    <SUITable.Row>
                        <SUITable.HeaderCell></SUITable.HeaderCell>

                        <SUITable.HeaderCell></SUITable.HeaderCell>

                        <SUITable.HeaderCell colSpan="3">24H</SUITable.HeaderCell>
                        {groupBy === BillingColumn.Project ? (
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

                        {groupBy === BillingColumn.Project ? (
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
                        <React.Fragment key={`total-${p.field}`}>
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
                                                    <b>{p[k.category]}</b>
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

                                {groupBy === BillingColumn.Project ? (
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
                                        key={`${dk.cost_category}-${p.field}`}
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

                                        {groupBy === BillingColumn.Project ? (
                                            <SUITable.Cell />
                                        ) : null}
                                    </SUITable.Row>
                                ))}
                        </React.Fragment>
                    ))}
                </SUITable.Body>
            </Table>
        </div>
    )
}

export default BillingCurrentCost
