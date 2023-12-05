import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Button, Card, Grid, Input, Message, Table as SUITable } from 'semantic-ui-react'
import CostByTimeChart from './components/CostByTimeChart'
import FieldSelector from './components/FieldSelector'
import {
    BillingApi,
    BillingColumn,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
} from '../../sm-api'

import { convertFieldName } from '../../shared/utilities/fieldName'
import { IStackedAreaByDateChartData } from '../../shared/components/Graphs/StackedAreaByDateChart'
import BillingCostByTimeTable from './components/BillingCostByTimeTable'
import { BarChart, IData } from '../../shared/components/Graphs/BarChart'
import { DonutChart } from '../../shared/components/Graphs/DonutChart'

const BillingCostByCategory: React.FunctionComponent = () => {
    const now = new Date()

    const [searchParams] = useSearchParams()

    const inputCostCategory: string | undefined = searchParams.get('costCategory') ?? undefined

    const [start, setStart] = React.useState<string>(
        searchParams.get('start') ??
            `${now.getFullYear() - 1}-${now.getMonth().toString().padStart(2, '0')}-01`
    )
    const [end, setEnd] = React.useState<string>(
        searchParams.get('end') ??
            `${now.getFullYear()}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now
                .getDate()
                .toString()
                .padStart(2, '0')}`
    )
    const [selectedData, setCostCategory] = React.useState<string | undefined>(inputCostCategory)

    const [selectedPeriod, setPeriod] = React.useState<string | undefined>(undefined)

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
        category: string | undefined,
        period: string | undefined,
        start: string,
        end: string
    ) => {
        let url = `${location.pathname}`

        if (category && period) {
            url += '?'

            let params: string[] = []
            if (category) params.push(`costCategory=${category}`)
            if (period) params.push(`period=${period}`)
            if (start) params.push(`start=${start}`)
            if (end) params.push(`end=${end}`)

            url += params.join('&')
            navigate(url)
        }
    }

    const onSelect = (event: any, recs: any) => {
        setCostCategory(recs.value)
        updateNav(recs.value, selectedPeriod, start, end)
    }

    const onSelectPeriod = (event: any, recs: any) => {
        setPeriod(recs.value)
        updateNav(selectedData, recs.value, start, end)
    }

    const changeDate = (name: string, value: string) => {
        let start_update = start
        let end_update = end
        if (name === 'start') start_update = value
        if (name === 'end') end_update = value
        setStart(start_update)
        setEnd(end_update)
        updateNav(selectedData, selectedPeriod, start_update, end_update)
    }

    const getData = (query: BillingTotalCostQueryModel) => {
        setIsLoading(true)
        setError(undefined)
        new BillingApi()
            .getTotalCost(query)
            .then((response) => {
                setIsLoading(false)

                // calc totals per sku
                const recTotals = response.data.reduce(
                    (
                        acc: { [key: string]: { [key: string]: number } },
                        item: BillingTotalCostRecord
                    ) => {
                        const { sku, cost } = item
                        if (!acc[sku]) {
                            acc[sku] = 0
                        }
                        acc[sku] += cost
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
                        const { day, sku, cost } = item
                        if (day !== undefined) {
                            if (!acc[day]) {
                                // initialise day structure
                                acc[day] = {}
                                rec_grps.forEach((k) => {
                                    acc[day][k] = 0
                                })
                            }
                            acc[day][sku] = cost
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
            })
            .catch((er) => setError(er.message))
    }

    React.useEffect(() => {
        if (
            selectedData !== undefined &&
            selectedData !== '' &&
            selectedData !== null &&
            selectedPeriod !== undefined &&
            selectedPeriod !== '' &&
            selectedPeriod !== null
        ) {
            getData({
                fields: [BillingColumn.Sku],
                start_date: start,
                end_date: end,
                filters: { cost_category: selectedData },
                order_by: { day: false },
                time_periods: selectedPeriod,
            })
        }
    }, [start, end, selectedData, selectedPeriod])

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
                    Billing Cost By Category
                </h1>

                <Grid columns="equal">
                    <Grid.Column>
                        <FieldSelector
                            label="Cost Category"
                            fieldName="cost_category"
                            onClickFunction={onSelect}
                            selected={selectedData}
                            includeAll={false}
                            autoSelect={false}
                        />
                    </Grid.Column>
                </Grid>

                <Grid columns="equal">
                    <Grid.Column>
                        <FieldSelector
                            label="Time Period"
                            fieldName="Period"
                            onClickFunction={onSelectPeriod}
                            selected={selectedPeriod}
                            includeAll={false}
                            autoSelect={false}
                        />
                    </Grid.Column>
                </Grid>

                <Grid columns="equal">
                    <Grid.Column className="field-selector-label">
                        <Input
                            label="Since"
                            fluid
                            type="date"
                            onChange={(e) => changeDate('start', e.target.value)}
                            value={start}
                        />
                    </Grid.Column>
                </Grid>

                <Grid>
                    <Grid.Column width={16}>
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
        </>
    )
}

export default BillingCostByCategory
