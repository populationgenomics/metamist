import React from 'react'
import { Checkbox, Header, Table as SUITable } from 'semantic-ui-react'
import { IStackedAreaByDateChartData } from '../../../shared/components/Graphs/StackedAreaByDateChart'
import LoadingDucks from '../../../shared/components/LoadingDucks/LoadingDucks'
import Table from '../../../shared/components/Table'
import { convertFieldName } from '../../../shared/utilities/fieldName'
import formatMoney from '../../../shared/utilities/formatMoney'

interface IBillingCostByTimeTableProps {
    heading: string
    start: string
    end: string
    groups: string[]
    isLoading: boolean
    data: IStackedAreaByDateChartData[]
}

const BillingCostByTimeTable: React.FC<IBillingCostByTimeTableProps> = ({
    heading,
    start,
    end,
    groups,
    isLoading,
    data,
}) => {
    const [internalData, setInternalData] = React.useState<IStackedAreaByDateChartData[]>([])
    const [internalGroups, setInternalGroups] = React.useState<string[]>([])

    // Format data
    React.useEffect(() => {
        setInternalData(
            data.map((p) => {
                const newP = { ...p }
                const total = Object.values(p.values).reduce((acc, cur) => acc + cur, 0)
                newP.values['Daily Total'] = total
                newP.values['Compute Cost'] = total - p.values['Cloud Storage']
                return newP
            })
        )

        setInternalGroups(groups.concat(['Daily Total', 'Compute Cost']))
    }, [data, groups])

    // Properties
    const [expandCompute, setExpandCompute] = React.useState<boolean>(false)
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: null,
        direction: null,
    })

    // Header sort
    const priorityColumns = ['Daily Total', 'Cloud Storage', 'Compute Cost']
    const headerSort = (a: string, b: string) => {
        if (priorityColumns.includes(a) && priorityColumns.includes(b)) {
            return priorityColumns.indexOf(a) < priorityColumns.indexOf(b) ? -1 : 1
        } else if (priorityColumns.includes(a)) {
            return -1
        } else if (priorityColumns.includes(b)) {
            return 1
        }
        return a < b ? -1 : 1
    }

    const headerFields = () => {
        if (expandCompute) {
            return internalGroups
                .sort(headerSort)
                .filter((group) => group != 'Compute Cost')
                .map((group: string) => ({
                    category: group,
                    title: group,
                }))
        }
        return [
            {
                category: 'Daily Total',
                title: 'Daily Total',
            },
            {
                category: 'Cloud Storage',
                title: 'Cloud Storage',
            },
            {
                category: 'Compute Cost',
                title: 'Compute Cost',
            },
        ]
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

    if (isLoading) {
        return (
            <div>
                <LoadingDucks />
            </div>
        )
    }

    const dataSort = (
        data: IStackedAreaByDateChartData[],
        props: string[],
        orders?: ('asc' | 'desc')[]
    ) =>
        [...data].sort(
            (a, b) =>
                props.reduce((acc, prop, i) => {
                    if (acc === 0) {
                        const [p1, p2] =
                            orders && orders[i] === 'desc'
                                ? [
                                      b.values[prop as keyof typeof b],
                                      a.values[prop as keyof typeof a],
                                  ]
                                : [
                                      a.values[prop as keyof typeof a],
                                      b.values[prop as keyof typeof b],
                                  ]
                        acc = p1 > p2 ? 1 : p1 < p2 ? -1 : 0
                    }
                    return acc
                }, 0) as number // explicitly cast the result to a number
        )

    const dataToBody = (data: IStackedAreaByDateChartData[]) => (
        <>
            {dataSort(
                data,
                sort.column ? [sort.column] : [],
                sort.direction === 'ascending' ? ['asc'] : ['desc']
            ).map((p) => (
                <React.Fragment key={p.date.toISOString()}>
                    <SUITable.Row>
                        <SUITable.Cell collapsing key={`Date - ${p.date.toISOString()}`}>
                            <b>{p.date.toLocaleDateString()}</b>
                        </SUITable.Cell>
                        {headerFields().map((k) => (
                            <SUITable.Cell key={`${p.date.toISOString()} - ${k.category}`}>
                                {formatMoney(p.values[k.category])}
                            </SUITable.Cell>
                        ))}
                    </SUITable.Row>
                </React.Fragment>
            ))}
        </>
    )

    return (
        <>
            <Header as="h3">
                {convertFieldName(heading)} costs from {start} to {end}
            </Header>
            <Table celled compact sortable selectable>
                <SUITable.Header>
                    <SUITable.Row>
                        <SUITable.HeaderCell colSpan={2} textAlign="center">
                            <Checkbox
                                label="Expand"
                                fitted
                                toggle
                                checked={expandCompute}
                                onChange={() => setExpandCompute(!expandCompute)}
                            />
                        </SUITable.HeaderCell>
                        <SUITable.HeaderCell>Storage Cost</SUITable.HeaderCell>
                        <SUITable.HeaderCell colSpan={headerFields().length - 1}>
                            Compute Cost
                        </SUITable.HeaderCell>
                    </SUITable.Row>
                    <SUITable.Row>
                        <SUITable.HeaderCell
                            style={{
                                borderBottom: 'none',
                            }}
                        >
                            Date
                        </SUITable.HeaderCell>
                        {headerFields().map((k) => (
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
                        ))}
                    </SUITable.Row>
                </SUITable.Header>
                <SUITable.Body>
                    {dataToBody(internalData)}
                    <SUITable.Row>
                        <SUITable.Cell collapsing>
                            <b>All Time Total</b>
                        </SUITable.Cell>
                        {headerFields().map((k) => (
                            <SUITable.Cell key={`Total ${k.category}`}>
                                <b>
                                    {formatMoney(
                                        internalData.reduce(
                                            (acc, cur) => acc + cur.values[k.category],
                                            0
                                        )
                                    )}
                                </b>
                            </SUITable.Cell>
                        ))}
                    </SUITable.Row>
                </SUITable.Body>
            </Table>
        </>
    )
}

export default BillingCostByTimeTable
