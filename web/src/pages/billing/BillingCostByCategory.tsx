import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Button, Card, Checkbox, Grid, Input, Message } from 'semantic-ui-react'
import CostByTimeBarChart from './components/CostByTimeBarChart'
import FieldSelector from './components/FieldSelector'
import {
    BillingApi,
    BillingColumn,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
    BillingTimePeriods,
} from '../../sm-api'

import { convertFieldName } from '../../shared/utilities/fieldName'
import { IStackedAreaByDateChartData } from '../../shared/components/Graphs/StackedAreaByDateChart'

const BillingCostByCategory: React.FunctionComponent = () => {
    const now = new Date()

    const [searchParams] = useSearchParams()

    const inputGroupBy: string | undefined = searchParams.get('groupBy') ?? undefined
    const fixedGroupBy: BillingColumn = inputGroupBy
        ? (inputGroupBy as BillingColumn)
        : BillingColumn.GcpProject

    const inputSelectedGroup: string | undefined = searchParams.get('group') ?? undefined
    const inputCostCategory: string | undefined = searchParams.get('costCategory') ?? undefined
    const inputPeriod: string | undefined = searchParams.get('period') ?? BillingTimePeriods.Month

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

    const [selectedGroup, setSelectedGroup] = React.useState<string | undefined>(inputSelectedGroup)
    const [selectedCostCategory, setCostCategory] = React.useState<string | undefined>(
        inputCostCategory
    )

    const [selectedPeriod, setPeriod] = React.useState<string | undefined>(inputPeriod)

    // Data loading
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [data, setData] = React.useState<IStackedAreaByDateChartData[]>([])

    const [groupBy, setGroupBy] = React.useState<BillingColumn>(
        fixedGroupBy ?? BillingColumn.GcpProject
    )

    const [accumulate, setAccumulate] = React.useState<boolean>(true)

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (
        grpBy: BillingColumn,
        grp: string | undefined,
        category: string | undefined,
        period: string | undefined,
        st: string,
        ed: string
    ) => {
        let url = `${location.pathname}`
        url += '?'
        let params: string[] = []
        if (grpBy) params.push(`groupBy=${grpBy}`)
        if (grp) params.push(`group=${grp}`)
        if (category) params.push(`costCategory=${category}`)
        if (period) params.push(`period=${period}`)
        if (st) params.push(`start=${st}`)
        if (ed) params.push(`end=${ed}`)
        url += params.join('&')
        navigate(url)
    }

    const onGroupBySelect = (event: any, recs: any) => {
        setGroupBy(recs.value)
        setSelectedGroup(undefined)
        updateNav(recs.value, undefined, selectedCostCategory, selectedPeriod, start, end)
    }

    const onSelectGroup = (event: any, recs: any) => {
        setSelectedGroup(recs.value)
        updateNav(groupBy, recs.value, selectedCostCategory, selectedPeriod, start, end)
    }

    const onSelectCategory = (event: any, recs: any) => {
        setCostCategory(recs.value)
        updateNav(groupBy, selectedGroup, recs.value, selectedPeriod, start, end)
    }

    const onSelectPeriod = (event: any, recs: any) => {
        setPeriod(recs.value)
        updateNav(groupBy, selectedGroup, selectedCostCategory, recs.value, start, end)
    }

    const changeDate = (name: string, value: string) => {
        let start_update = start
        let end_update = end
        if (name === 'start') start_update = value
        if (name === 'end') end_update = value
        setStart(start_update)
        setEnd(end_update)
        updateNav(
            groupBy,
            selectedGroup,
            selectedCostCategory,
            selectedPeriod,
            start_update,
            end_update
        )
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
        // if selectedCostCategory is all
        const selFilters: { [key: string]: string } = {}

        if (groupBy && selectedGroup && !selectedGroup.startsWith('All ')) {
            selFilters[groupBy] = selectedGroup
        }
        if (selectedCostCategory && !selectedCostCategory.startsWith('All ')) {
            selFilters.cost_category = selectedCostCategory
        }

        if (selectedPeriod !== undefined && selectedPeriod !== '' && selectedPeriod !== null) {
            getData({
                fields: [BillingColumn.Sku],
                start_date: start,
                end_date: end,
                filters: selFilters,
                order_by: { day: false },
                time_periods: selectedPeriod,
            })
        }
    }, [groupBy, selectedGroup, selectedCostCategory, selectedPeriod, start, end])

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

                <Grid stackable doubling>
                    <Grid.Column width={4}>
                        <FieldSelector
                            label="Group By"
                            fieldName="Group"
                            onClickFunction={onGroupBySelect}
                            selected={groupBy}
                            autoSelect={true}
                        />
                    </Grid.Column>

                    <Grid.Column width={6}>
                        <FieldSelector
                            label={convertFieldName(groupBy)}
                            fieldName={groupBy}
                            onClickFunction={onSelectGroup}
                            selected={selectedGroup}
                            includeAll={true}
                            autoSelect={true}
                        />
                    </Grid.Column>

                    <Grid.Column width={6}>
                        <FieldSelector
                            label="Cost Category"
                            fieldName="cost_category"
                            onClickFunction={onSelectCategory}
                            selected={selectedCostCategory}
                            includeAll={true}
                            autoSelect={true}
                        />
                    </Grid.Column>
                </Grid>

                <Grid columns="equal" stackable doubling>
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
                    <Grid.Column width={12}></Grid.Column>
                    <Grid.Column width={4}>
                        <Checkbox
                            label="Accumulate ON/OFF"
                            fitted
                            toggle
                            checked={accumulate}
                            onChange={() => setAccumulate(!accumulate)}
                        />
                    </Grid.Column>
                </Grid>

                <Grid>
                    <Grid.Column width={16}>
                        <CostByTimeBarChart
                            isLoading={isLoading}
                            accumulate={accumulate}
                            data={data}
                        />
                    </Grid.Column>
                </Grid>
            </Card>
        </>
    )
}

export default BillingCostByCategory
