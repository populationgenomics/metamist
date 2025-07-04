import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Button, Card, Checkbox, Dropdown, Grid, Input, Message } from 'semantic-ui-react'
import { IStackedAreaByDateChartData } from '../../shared/components/Graphs/StackedAreaByDateChart'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import { exportTable } from '../../shared/utilities/exportTable'
import { convertFieldName } from '../../shared/utilities/fieldName'
import generateUrl from '../../shared/utilities/generateUrl'
import { getMonthEndDate, getMonthStartDate } from '../../shared/utilities/monthStartEndDate'
import {
    BillingApi,
    BillingColumn,
    BillingTimePeriods,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
} from '../../sm-api'
import CostByTimeBarChart from './components/CostByTimeBarChart'
import FieldSelector from './components/FieldSelector'

/* eslint-disable @typescript-eslint/no-explicit-any  -- too many anys in the file to fix right now but would be good to sort out when we can */
const BillingCostByCategory: React.FunctionComponent = () => {
    const [searchParams] = useSearchParams()

    const inputGroupBy: string | undefined = searchParams.get('groupBy') ?? undefined
    const fixedGroupBy: BillingColumn = inputGroupBy
        ? (inputGroupBy as BillingColumn)
        : BillingColumn.GcpProject

    const inputSelectedGroup: string | undefined = searchParams.get('group') ?? undefined
    const inputCostCategory: string | undefined = searchParams.get('costCategory') ?? undefined
    const inputPeriod: string | undefined = searchParams.get('period') ?? BillingTimePeriods.Month

    const [start, setStart] = React.useState<string>(
        searchParams.get('start') ?? getMonthStartDate()
    )
    const [end, setEnd] = React.useState<string>(searchParams.get('end') ?? getMonthEndDate())

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
        const url = generateUrl(location, {
            groupBy: grpBy,
            group: grp,
            costCategory: category,
            period: period,
            start: st,
            end: ed,
        })
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
                const recTotals: { [key: string]: number } = {}
                response.data.forEach((item: BillingTotalCostRecord) => {
                    const { sku, cost } = item
                    // if sku is not a string, set to unknown
                    const _sku = typeof sku === 'string' ? sku : 'unknown'
                    if (!(_sku in recTotals)) {
                        recTotals[_sku] = 0
                    }
                    recTotals[_sku] += cost
                })
                const sortedRecTotals: { [key: string]: number } = Object.fromEntries(
                    Object.entries(recTotals).sort(([, a], [, b]) => b - a)
                )
                const rec_grps = Object.keys(sortedRecTotals)
                const records: { [key: string]: { [key: string]: number } } = {}

                response.data.forEach((item: any) => {
                    const { day, sku, cost } = item
                    if (day !== undefined) {
                        if (!records[day]) {
                            // initialise day structure
                            records[day] = {}
                            rec_grps.forEach((k: string) => {
                                records[day][k] = 0
                            })
                        }
                        records[day][sku] = cost
                    }
                })
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
                time_periods: selectedPeriod as BillingTimePeriods,
                // show only records with cost > 0.01
                min_cost: 0.01,
            })
        }
    }, [groupBy, selectedGroup, selectedCostCategory, selectedPeriod, start, end])

    if (error) {
        return (
            <Message negative onDismiss={() => setError(undefined)}>
                {error}
                <br />
                <Button negative onClick={() => window.location.reload()}>
                    Retry
                </Button>
            </Message>
        )
    }

    const exportToFile = (format: 'csv' | 'tsv') => {
        const dateSet = new Set<string>()
        const skuSet = new Set<string>()

        const dateToValues = new Map<string, Record<string, number>>()

        data.forEach((row) => {
            const dateStr = row.date.toISOString().slice(0, 10)
            dateSet.add(dateStr)
            dateToValues.set(dateStr, row.values)
            Object.keys(row.values).forEach((sku) => skuSet.add(sku))
        })

        const dates = [...dateSet].sort()
        const skus = [...skuSet].sort()

        const headerFields = ['SKU', ...dates]

        const matrix = skus.map((sku) => {
            const rowCells = dates.map((date) => {
                const cost = dateToValues.get(date)?.[sku]
                return typeof cost === 'number' && cost !== 0 ? cost.toFixed(2) : ''
            })
            return [sku, ...rowCells]
        })

        exportTable({ headerFields, matrix }, format, 'billing_cost_by_category')
    }

    return (
        <>
            <Card fluid style={{ padding: '20px' }} id="billing-container">
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <h1
                        style={{
                            fontSize: 40,
                        }}
                    >
                        Billing Cost By Category
                    </h1>
                    <div style={{ textAlign: 'right' }}>
                        <Dropdown
                            button
                            className="icon"
                            floating
                            labeled
                            icon="download"
                            text="Export"
                        >
                            <Dropdown.Menu>
                                <Dropdown.Item
                                    key="csv"
                                    text="Export to CSV"
                                    icon="file excel"
                                    onClick={() => exportToFile('csv')}
                                />
                                <Dropdown.Item
                                    key="tsv"
                                    text="Export to TSV"
                                    icon="file text outline"
                                    onClick={() => exportToFile('tsv')}
                                />
                            </Dropdown.Menu>
                        </Dropdown>
                    </div>
                </div>

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

export default function BillingCostByCategoryPage() {
    return (
        <PaddedPage>
            <BillingCostByCategory />
        </PaddedPage>
    )
}

/* eslint-enable @typescript-eslint/no-explicit-any   */
