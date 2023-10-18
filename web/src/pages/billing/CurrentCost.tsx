/* eslint-disable */
// Since this is for admin only

import * as React from 'react'
import { Table as SUITable, Message, Button, Checkbox, Input, InputProps } from 'semantic-ui-react'
import Table from '../../shared/components/Table'
import { BillingApi, BillingCostBudgetRecord, BillingCostDetailsRecord } from '../../sm-api'
import _ from 'lodash'

const CurrentCost = () => {
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
        { category: 'total_daily', title: '24H' },
        { category: 'total_monthly', title: 'Monthly Acc' },
        { category: 'budget', title: 'Budget' },
        { category: 'monthly_percent', title: '%' },
    ]

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
                            >{k.title}</SUITable.HeaderCell>
                        ))}
                    </SUITable.Row>
                </SUITable.Header>
                <SUITable.Body>

                {
                    _.orderBy(
                        costRecords,
                                  [sort.column],
                                  sort.direction === 'ascending' ? ['asc'] : ['desc']
                              ).map( (p) =>
                                { return (
                                <SUITable.Row>
                                {
                                    HEADER_FIELDS.map((k) => {
                                        switch (k.category) {
                                            case 'topic':
                                                return <SUITable.Cell>{p[k.category]}</SUITable.Cell>;
                                            case 'monthly_percent':
                                                return <SUITable.Cell>{percFormat(p[k.category])}</SUITable.Cell>;
                                            default:
                                                return <SUITable.Cell>{currencyFormat(p[k.category])}</SUITable.Cell>;
                                        }
                                    })
                                }
                                </SUITable.Row>
                            )}
                        )
                }
                </SUITable.Body>
            </Table>
        </>
    )
}

export default CurrentCost
