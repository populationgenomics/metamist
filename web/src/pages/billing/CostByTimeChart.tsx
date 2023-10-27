import * as React from 'react'
import _ from 'lodash'

import Container from 'react-bootstrap/Container'
import Row from 'react-bootstrap/Row'
import Col from 'react-bootstrap/Col'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import {
    BillingApi,
    BillingColumn,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
} from '../../sm-api'
import { Message } from 'semantic-ui-react'
import {
    IStackedAreaByDateChartData,
    StackedAreaByDateChart,
} from '../../shared/components/Graphs/StackedAreaByDateChart'

import { BarChart, IData } from '../../shared/components/Graphs/BarChart'
import DonutChart from '../../shared/components/Graphs/DonutChart'

interface ICostByTimeChartProps {
    start: string
    end: string
    groupBy: string
    selectedGroup: string
}

const CostByTimeChart: React.FunctionComponent<ICostByTimeChartProps> = ({
    start,
    end,
    groupBy,
    selectedGroup,
}) => {
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()

    const [data, setData] = React.useState<IStackedAreaByDateChartData[]>([])
    const [groups, setGroups] = React.useState<string[]>([])

    const [barChartData, setBarChartData] = React.useState<IData[]>([])
    const [donutChartData, setDonutChartData] = React.useState<[]>([])

    const getData = (query: BillingTotalCostQueryModel) => {
        setIsLoading(true)
        setError(undefined)
        new BillingApi()
            .getTotalCost(query)
            .then((response) => {
                setIsLoading(false)
                const recGrps = _.uniq(
                    response.data.map((item: BillingTotalCostRecord) => item.cost_category)
                )
                const records = response.data.reduce(
                    (
                        acc: { [key: string]: { [key: string]: number } },
                        item: BillingTotalCostRecord
                    ) => {
                        const { day, cost_category, cost } = item
                        if (day !== undefined) {
                            if (!acc[day]) {
                                acc[day] = {}
                                recGrps.forEach((k) => {
                                    acc[day][k] = 0
                                })
                            }
                            acc[day][cost_category] = cost
                        }
                        return acc
                    },
                    {}
                )

                // calc totals per cost_category
                const recTotals = response.data.reduce(
                    (
                        acc: { [key: string]: { [key: string]: number } },
                        item: BillingTotalCostRecord
                    ) => {
                        const { cost_category, cost } = item
                        if (!acc[cost_category]) {
                            acc[cost_category] = 0
                        }
                        acc[cost_category] += cost
                        return acc
                    },
                    {}
                )
                const sortedRecTotals: { [key: string]: number } = Object.fromEntries(
                    Object.entries(recTotals).sort(([, a], [, b]) => b - a)
                )

                const recGrpsSorted: string[] = Object.keys(sortedRecTotals)

                const barData: IData[] = Object.entries(sortedRecTotals)
                    .map(([label, value]) => ({ label, value }))
                    .reduce((acc: IData[], curr: IData, index: number, arr: IData[]) => {
                        if (index < 5) {
                            acc.push(curr)
                        } else {
                            const restValue = arr
                                .slice(index)
                                .reduce((sum, { value }) => sum + value, 0)
                            if (acc.length === 5) {
                                acc.push({ label: 'Rest', value: restValue })
                            } else {
                                acc[5].value += restValue
                            }
                        }
                        return acc
                    }, [])

                setGroups(recGrpsSorted)
                setData(
                    Object.keys(records).map((key) => ({
                        date: new Date(key),
                        values: records[key],
                    }))
                )
                setBarChartData(barData)
                setDonutChartData(barData)
            })
            .catch((er) => setError(er.message))
    }

    // on first load
    React.useEffect(() => {
        if (selectedGroup !== undefined && selectedGroup !== '' && selectedGroup !== null) {
            if (selectedGroup.startsWith('All ')) {
                getData({
                    fields: [BillingColumn.Day, BillingColumn.CostCategory],
                    start_date: start,
                    end_date: end,
                    order_by: { day: false },
                })
            } else {
                getData({
                    fields: [BillingColumn.Day, BillingColumn.CostCategory],
                    start_date: start,
                    end_date: end,
                    filters: { [groupBy.replace('-', '_').toLowerCase()]: selectedGroup },
                    order_by: { day: false },
                })
            }
        }
    }, [start, end, groupBy, selectedGroup])

    if (isLoading)
        return (
            <div>
                <LoadingDucks />
                <p style={{ textAlign: 'center', marginTop: '5px' }}>
                    <em>This query takes a while...</em>
                </p>
            </div>
        )

    return (
        <Container>
            <Row>
                <Col>
                    <BarChart data={barChartData} />
                </Col>
                <Col>
                    <DonutChart data={donutChartData} />
                </Col>
            </Row>
            <Row>
                <Col colspan="2">
                    <StackedAreaByDateChart
                        keys={groups}
                        data={data}
                        start={new Date(start)}
                        end={new Date(end)}
                        isPercentage={false}
                        xLabel=""
                        yLabel="Cost (AUD)"
                        seriesLabel="Service"
                        extended={false}
                        showDate={true}
                    />
                </Col>
            </Row>
        </Container>
    )
}

export default CostByTimeChart
