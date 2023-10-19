/* eslint-disable */
// Since this is for admin only

import * as React from 'react'
import { Table as SUITable, Message, Button, Checkbox, Input, InputProps } from 'semantic-ui-react'
import Table from '../../shared/components/Table'
import { BillingApi, BillingCostBudgetRecord, BillingCostDetailsRecord } from '../../sm-api'
import _ from 'lodash'

const CurrentCost = () => {
    const [openRows, setOpenRows] = React.useState<string[]>([])

    const [costRecords, setCosts] = React.useState<BillingCostBudgetRecord[]>([])
    const [error, setError] = React.useState<string | undefined>()
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: 'undefined',
        direction: 'undefined',
    })

    const getCosts = () => {
        setError(undefined)
        new BillingApi()
            .getRunningCost()
            .then((response) => {
                setCosts(response.data)
            })
            .catch((er) => setError(er.message))
    }

    React.useEffect(() => {
        getCosts()
    }, [])

    const headers = ['topic', 'C', 'S', '24H', 'Monthly Acc', 'Budget', '%']

    const HEADER_FIELDS = [
        { category: 'topic', title: 'Topic' },
        { category: 'compute_daily', title: 'C' },
        { category: 'storage_daily', title: 'S' },
        { category: 'total_daily', title: 'Total' },
        { category: 'compute_monthly', title: 'C' },
        { category: 'storage_monthly', title: 'S' },
        { category: 'total_monthly', title: 'Total' },
    ]

    const handleToggle = (topic: string) => {
        if (!openRows.includes(topic)) {
            setOpenRows([...openRows, topic])
        } else {
            setOpenRows(openRows.filter((i) => i !== topic))
        }
        console.log('handleToggle', topic)
    }

    function currencyFormat(num: number = 0): string {
        if (num === undefined || num === null) {
            return ''
        }

        return '$' + num.toFixed(2).replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,')
    }

    function percFormat(num: number = 0): string {
        if (num === undefined || num === null) {
            return ''
        }

        return num.toFixed(0).toString() + '%'
    }

    if (error)
        return (
            <Message negative>
                {error}
                <br />
                <Button color="red" onClick={() => getCosts()}>
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

    return (
        <>
            <h1>Billing Current Costs</h1>
            <Table celled compact sortable>
                <SUITable.Header>
                    <SUITable.Row>
                        <SUITable.HeaderCell></SUITable.HeaderCell>

                        <SUITable.HeaderCell></SUITable.HeaderCell>

                        <SUITable.HeaderCell colspan="3">24H</SUITable.HeaderCell>
                        <SUITable.HeaderCell colspan="3">Invoice Month (Acc)</SUITable.HeaderCell>
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
                    </SUITable.Row>
                </SUITable.Header>
                <SUITable.Body>
                    {_.orderBy(
                        costRecords,
                        [sort.column],
                        sort.direction === 'ascending' ? ['asc'] : ['desc']
                    ).map((p, index, array) => (
                        <React.Fragment key={p.topic}>
                            <SUITable.Row>
                                <SUITable.Cell collapsing>
                                    <Checkbox
                                        checked={openRows.includes(p.topic)}
                                        slider
                                        onChange={() => handleToggle(p.topic)}
                                    />
                                </SUITable.Cell>
                                {HEADER_FIELDS.map((k) => {
                                    switch (k.category) {
                                        case 'topic':
                                            return <SUITable.Cell>{p[k.category]}</SUITable.Cell>
                                        case 'monthly_percent':
                                            return (
                                                <SUITable.Cell>
                                                    {percFormat(p[k.category])}
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
                            </SUITable.Row>
                            {typeof p === 'object' &&
                                'details' in p &&
                                _.orderBy(p?.details, ['monthly_cost'], ['desc']).map((dk) => (
                                    <SUITable.Row
                                        style={{
                                            display: openRows.includes(p.topic)
                                                ? 'table-row'
                                                : 'none',
                                        }}
                                        key={p.topic}
                                    >
                                        <SUITable.Cell style={{ border: 'none' }} />
                                        <SUITable.Cell>
                                            <code>
                                                {dk.cost_category} ({dk.cost_group})
                                            </code>
                                        </SUITable.Cell>

                                        {dk.cost_group === 'C' ? (
                                            <React.Fragment>
                                                <SUITable.Cell>
                                                    <code>{currencyFormat(dk.daily_cost)}</code>
                                                </SUITable.Cell>

                                                <SUITable.Cell colspan="2" />

                                                <SUITable.Cell>
                                                    <code>{currencyFormat(dk.monthly_cost)}</code>
                                                </SUITable.Cell>
                                                <SUITable.Cell colspan="2" />
                                            </React.Fragment>
                                        ) : (
                                            <React.Fragment>
                                                <SUITable.Cell />
                                                <SUITable.Cell>
                                                    <code>{currencyFormat(dk.daily_cost)}</code>
                                                </SUITable.Cell>

                                                <SUITable.Cell colspan="2" />
                                                <SUITable.Cell>
                                                    <code>{currencyFormat(dk.monthly_cost)}</code>
                                                </SUITable.Cell>
                                                <SUITable.Cell />
                                            </React.Fragment>
                                        )}
                                    </SUITable.Row>
                                ))}
                        </React.Fragment>
                    ))}
                </SUITable.Body>
            </Table>
        </>
    )
}

export default CurrentCost
