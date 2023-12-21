import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Button, Card, Grid, Input, Message, Table as SUITable } from 'semantic-ui-react'
import CostByTimeChart from './components/CostByTimeChart'
import FieldSelector from './components/FieldSelector'
import {
    BillingApi,
    BillingColumn,
    BillingSource,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
} from '../../sm-api'

import { convertFieldName } from '../../shared/utilities/fieldName'
import { IStackedAreaByDateChartData } from '../../shared/components/Graphs/StackedAreaByDateChart'
import BillingCostByTimeTable from './components/BillingCostByTimeTable'
import { BarChart, IData } from '../../shared/components/Graphs/BarChart'
import { DonutChart } from '../../shared/components/Graphs/DonutChart'

const BillingCostByTime: React.FunctionComponent = () => {
    const now = new Date()

    const [searchParams] = useSearchParams()

    const inputGroupBy: string | undefined = searchParams.get('groupBy') ?? undefined
    const fixedGroupBy: BillingColumn = inputGroupBy
        ? (inputGroupBy as BillingColumn)
        : BillingColumn.GcpProject
    const inputSelectedData: string | undefined = searchParams.get('selectedData') ?? undefined

    const [start, setStart] = React.useState<string>(
        searchParams.get('start') ??
            `${now.getFullYear()}-${(now.getMonth() + 1).toString().padStart(2, '0')}-01`
    )
    const [end, setEnd] = React.useState<string>(
        searchParams.get('end') ??
            `${now.getFullYear()}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now
                .getDate()
                .toString()
                .padStart(2, '0')}`
    )
    const [groupBy, setGroupBy] = React.useState<BillingColumn>(
        fixedGroupBy ?? BillingColumn.GcpProject
    )
    const [selectedData, setSelectedData] = React.useState<string | undefined>(inputSelectedData)

    // Max Aggregated Data Points, rest will be aggregated into "Rest"
    const maxDataPoints = 7

    // Data loading
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [groups, setGroups] = React.useState<string[]>([])
    const [data, setData] = React.useState<IStackedAreaByDateChartData[]>([])
    const [aggregatedData, setAggregatedData] = React.useState<IData[]>([])

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (
        grp: string | undefined,
        data: string | undefined,
        start: string,
        end: string
    ) => {
        let url = `${location.pathname}`
        if (grp || data) url += '?'

        let params: string[] = []
        if (grp) params.push(`groupBy=${grp}`)
        if (data) params.push(`selectedData=${data}`)
        if (start) params.push(`start=${start}`)
        if (end) params.push(`end=${end}`)

        url += params.join('&')
        navigate(url)
    }

    const onGroupBySelect = (event: any, data: any) => {
        setGroupBy(data.value)
        setSelectedData(undefined)
        updateNav(data.value, undefined, start, end)
    }

    const onSelect = (event: any, data: any) => {
        setSelectedData(data.value)
        updateNav(groupBy, data.value, start, end)
    }

    const changeDate = (name: string, value: string) => {
        let start_update = start
        let end_update = end
        if (name === 'start') start_update = value
        if (name === 'end') end_update = value
        setStart(start_update)
        setEnd(end_update)
        updateNav(groupBy, selectedData, start_update, end_update)
    }

    const getData = (query: BillingTotalCostQueryModel) => {
        setIsLoading(true)
        setError(undefined)
        new BillingApi()
            .getTotalCost(query)
            .then((response) => {
                setIsLoading(false)

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
                const rec_grps = Object.keys(sortedRecTotals)
                const records = response.data.reduce(
                    (
                        acc: { [key: string]: { [key: string]: number } },
                        item: BillingTotalCostRecord
                    ) => {
                        const { day, cost_category, cost } = item
                        if (day !== undefined) {
                            if (!acc[day]) {
                                // initialise day structure
                                acc[day] = {}
                                rec_grps.forEach((k) => {
                                    acc[day][k] = 0
                                })
                            }
                            acc[day][cost_category] = cost
                        }
                        return acc
                    },
                    {}
                )
                const no_undefined: string[] = rec_grps.filter(
                    (item): item is string => item !== undefined
                )
                setGroups(no_undefined)
                setData(
                    Object.keys(records).map((key) => ({
                        date: new Date(key),
                        values: records[key],
                    }))
                )
                const aggData: IData[] = Object.entries(sortedRecTotals)
                    .map(([label, value]) => ({ label, value }))
                    .reduce((acc: IData[], curr: IData, index: number, arr: IData[]) => {
                        if (index < maxDataPoints) {
                            acc.push(curr)
                        } else {
                            const restValue = arr
                                .slice(index)
                                .reduce((sum, { value }) => sum + value, 0)

                            if (acc.length == maxDataPoints) {
                                acc.push({ label: 'Rest*', value: restValue })
                            } else {
                                acc[maxDataPoints].value += restValue
                            }
                        }
                        return acc
                    }, [])

                setAggregatedData(aggData)
            })
            .catch((er) => setError(er.message))
    }

    React.useEffect(() => {
        if (selectedData !== undefined && selectedData !== '' && selectedData !== null) {
            let source = BillingSource.Aggregate
            if (groupBy === BillingColumn.GcpProject) {
                source = BillingSource.GcpBilling
            }
            if (selectedData.startsWith('All ')) {
                getData({
                    fields: [BillingColumn.Day, BillingColumn.CostCategory],
                    start_date: start,
                    end_date: end,
                    order_by: { day: false },
                    source: source,
                })
            } else {
                getData({
                    fields: [BillingColumn.Day, BillingColumn.CostCategory],
                    start_date: start,
                    end_date: end,
                    filters: { [groupBy.replace('-', '_').toLowerCase()]: selectedData },
                    order_by: { day: false },
                    source: source,
                })
            }
        }
    }, [start, end, groupBy, selectedData])

    if (error) {
        return (
            <Message negative onDismiss={() => setError(undefined)}>
                {error}
                <br />
                <Button color="red" onClick={() => setStart(start)}>
                    Retry
                </Button>
            </Message>
        )
    }

    return (
        <>
            <Card fluid style={{ padding: '20px' }} id="billing-container">
                <h1
                    style={{
                        fontSize: 40,
                    }}
                >
                    Billing Cost By Time
                </h1>

                <Grid columns="equal" stackable doubling>
                    <Grid.Column>
                        <FieldSelector
                            label="Group By"
                            fieldName="Group"
                            onClickFunction={onGroupBySelect}
                            selected={groupBy}
                            autoSelect={false}
                        />
                    </Grid.Column>

                    <Grid.Column>
                        <FieldSelector
                            label={convertFieldName(groupBy)}
                            fieldName={groupBy}
                            onClickFunction={onSelect}
                            selected={selectedData}
                            includeAll={true}
                            autoSelect={true}
                        />
                    </Grid.Column>
                </Grid>

                <Grid columns="equal" stackable doubling>
                    <Grid.Column className="field-selector-label">
                        <Input
                            label="Start"
                            fluid
                            type="date"
                            onChange={(e) => changeDate('start', e.target.value)}
                            value={start}
                        />
                    </Grid.Column>

                    <Grid.Column className="field-selector-label">
                        <Input
                            label="Finish"
                            fluid
                            type="date"
                            onChange={(e) => changeDate('end', e.target.value)}
                            value={end}
                        />
                    </Grid.Column>
                </Grid>

                <Grid columns={2} stackable>
                    <Grid.Column width={10} className="chart-card">
                        <BarChart
                            data={aggregatedData}
                            maxSlices={groups.length}
                            isLoading={isLoading}
                        />
                    </Grid.Column>

                    <Grid.Column width={6} className="chart-card">
                        <DonutChart
                            data={aggregatedData}
                            maxSlices={groups.length}
                            isLoading={isLoading}
                        />
                    </Grid.Column>
                </Grid>

                <Grid>
                    <Grid.Column width={16} className="chart-card">
                        <CostByTimeChart
                            start={start}
                            end={end}
                            groups={groups}
                            isLoading={isLoading}
                            data={data}
                        />
                    </Grid.Column>
                </Grid>
            </Card>
            <Card
                fluid
                style={{ padding: '20px', overflowX: 'scroll' }}
                id="billing-container-data"
            >
                <BillingCostByTimeTable
                    heading={selectedData ?? 'All'}
                    start={start}
                    end={end}
                    groups={groups}
                    isLoading={isLoading}
                    data={data}
                />
            </Card>
        </>
    )
}

export default BillingCostByTime
