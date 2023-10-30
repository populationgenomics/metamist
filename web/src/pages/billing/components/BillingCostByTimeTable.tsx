import { Checkbox, Table as SUITable } from "semantic-ui-react"
import Table from "../../../shared/components/Table"
import React from "react"
import { BillingColumn } from "../../../sm-api"
import { convertFieldName } from "../../../shared/utilities/fieldName"
import LoadingDucks from "../../../shared/components/LoadingDucks/LoadingDucks"
import { IStackedAreaByDateChartData } from "../../../shared/components/Graphs/StackedAreaByDateChart"
import orderBy from "../../../shared/utilities/orderBy"
import { ErrorBarDataPointFormatter } from "recharts/types/cartesian/ErrorBar"

interface IBillingCostByTimeTableProps {
    start: string
    end: string
    groups: string[]
    isLoading: boolean
    data: IStackedAreaByDateChartData[]
}

const BillingCostByTimeTable: React.FC<IBillingCostByTimeTableProps> = ({
    start,
    end,
    groups,
    isLoading,
    data
}) => {
    // Properties
    const [openRows, setOpenRows] = React.useState<string[]>([])
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: null,
        direction: null,
    })

    // Header sort
    const headerSort = (a: string, b: string) => {
        if (a === 'Cloud Storage') {
            return -1
        }
        if (b === 'Cloud Storage') {
            return 1
        }
        return a < b ? -1 : 1
    }

    const HEADER_FIELDS = groups.sort(headerSort).map((group: string) => {
        return {
            category: group,
            title: group,
        }
    })

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

    const currencyFormat = (num: number): string => {
        if (num === undefined || num === null) {
            return ''
        }

        return `$${num.toFixed(2).replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,')}`
    }


    const percFormat = (num: number): string => {
        if (num === undefined || num === null) {
            return ''
        }

        return `${num.toFixed(0).toString()} % `
    }

    // const handleToggle = (field: string) => {
    //     if (!openRows.includes(field)) {
    //         setOpenRows([...openRows, field])
    //     } else {
    //         setOpenRows(openRows.filter((i) => i !== field))
    //     }
    // }

    // const linkTo = (data: string) => {
    //     return `/billing/costByTime?groupBy=${groupBy}&selectedData=${data}`
    // }

    if (isLoading) {
        return (
            <div>
                <LoadingDucks />
                <p style={{ textAlign: 'center', marginTop: '5px' }}>
                    <em>This query takes a while...</em>
                </p>
            </div>
        )
    }

    const dataSort = (
        data: IStackedAreaByDateChartData[],
        props: string[],
        orders?: ("asc" | "desc")[]
    ) => (
        [...data].sort((a, b) =>
            props.reduce((acc, prop, i) => {
                if (acc === 0) {
                    const [p1, p2] =
                        orders && orders[i] === "desc"
                            ? [b.values[prop as keyof typeof b], a.values[prop as keyof typeof a]]
                            : [a.values[prop as keyof typeof a], b.values[prop as keyof typeof b]];
                    acc = p1 > p2 ? 1 : p1 < p2 ? -1 : 0;
                }
                return acc;
            }, 0) as number // explicitly cast the result to a number
        )
    );

    const dataToBody = (data: IStackedAreaByDateChartData[]) => (
        <>
            {dataSort(
                data,
                sort.column ? [sort.column] : [],
                sort.direction === 'ascending' ? ['asc'] : ['desc']
            ).map((p) => (
                <React.Fragment key={p.date.toISOString()}>
                    <SUITable.Row>
                        <SUITable.Cell collapsing>
                            <b>{p.date.toLocaleDateString()}</b>
                        </SUITable.Cell>
                        <SUITable.Cell collapsing>
                            <b>{currencyFormat(Object.values(p.values).reduce((acc, cur) => acc + cur, 0))}</b>
                        </SUITable.Cell>
                        {HEADER_FIELDS.map((k) => (
                            <SUITable.Cell>
                                {currencyFormat(p.values[k.category])}
                            </SUITable.Cell>
                        ))}
                    </SUITable.Row>
                </React.Fragment>
            ))}
        </>
    )

    return (
        <Table celled compact sortable selectable>
            <SUITable.Header>
                <SUITable.Row>
                    <SUITable.HeaderCell colSpan={2}></SUITable.HeaderCell>
                    <SUITable.HeaderCell colSpan={HEADER_FIELDS.length}>Cost Type</SUITable.HeaderCell>
                </SUITable.Row>
                <SUITable.Row>
                    <SUITable.HeaderCell>Date</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Daily Total</SUITable.HeaderCell>

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
                            {convertFieldName(k.title)}
                        </SUITable.HeaderCell>
                    ))}
                </SUITable.Row>
            </SUITable.Header>
            <SUITable.Body>
                {dataToBody(data)}
                <SUITable.Row>
                    <SUITable.Cell collapsing>
                        <b>All Time Total</b>
                    </SUITable.Cell>
                    {HEADER_FIELDS.map((k) => (
                        <SUITable.Cell>
                            <b>{currencyFormat(data.reduce((acc, cur) => acc + cur.values[k.category], 0))}</b>
                        </SUITable.Cell>
                    ))}
                </SUITable.Row>
            </SUITable.Body>
        </Table>
    )
}

export default BillingCostByTimeTable;
