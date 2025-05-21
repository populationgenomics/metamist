import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Button, Card, Dropdown, DropdownProps, Grid, Input, Message } from 'semantic-ui-react'
import { BarChart, IData } from '../../shared/components/Graphs/BarChart'
import { DonutChart } from '../../shared/components/Graphs/DonutChart'
import { IStackedAreaByDateChartData } from '../../shared/components/Graphs/StackedAreaByDateChart'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { convertFieldName } from '../../shared/utilities/fieldName'
import generateUrl from '../../shared/utilities/generateUrl'
import { getMonthEndDate, getMonthStartDate } from '../../shared/utilities/monthStartEndDate'
import {
    BillingApi,
    BillingColumn,
    BillingSource,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
} from '../../sm-api'
import BillingCostByTimeTable from './components/BillingCostByTimeTable'
import CostByTimeChart from './components/CostByTimeChart'
import FieldSelector from './components/FieldSelector'

const BillingCostByTime: React.FunctionComponent = () => {
    const [searchParams] = useSearchParams()

    const inputGroupBy: string | undefined = searchParams.get('groupBy') ?? undefined
    const fixedGroupBy: BillingColumn = inputGroupBy
        ? (inputGroupBy as BillingColumn)
        : BillingColumn.GcpProject
    const inputSelectedData: string | undefined = searchParams.get('selectedData') ?? undefined

    const [start, setStart] = React.useState<string>(
        searchParams.get('start') ?? getMonthStartDate()
    )
    const [end, setEnd] = React.useState<string>(searchParams.get('end') ?? getMonthEndDate())
    const [groupBy, setGroupBy] = React.useState<BillingColumn>(
        fixedGroupBy ?? BillingColumn.GcpProject
    )
    const [selectedData, setSelectedData] = React.useState<string | undefined>(inputSelectedData)

    // Max Aggregated Data Points, rest will be aggregated into "Rest"
    const maxDataPoints = 7

    // Data loading
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [message, setMessage] = React.useState<string | undefined>()
    const [groups, setGroups] = React.useState<string[]>([])
    const [data, setData] = React.useState<IStackedAreaByDateChartData[]>([])
    const [aggregatedData, setAggregatedData] = React.useState<IData[]>([])

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (
        grp: string | undefined,
        selData: string | undefined,
        st: string,
        ed: string
    ) => {
        const url = generateUrl(location, {
            groupBy: grp,
            selectedData: selData,
            start: st,
            end: ed,
        })
        navigate(url)
    }

    const onGroupBySelect = (event: unknown, data: DropdownProps) => {
        const value = data.value
        if (typeof value == 'string') {
            setGroupBy(value as BillingColumn)
            setSelectedData(undefined)
            updateNav(value, undefined, start, end)
        }
    }

    const onSelect = (event: unknown, data: DropdownProps) => {
        const value = data.value
        if (typeof value == 'string') {
            setSelectedData(value)
            updateNav(groupBy, value, start, end)
        }
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
        setMessage(undefined)
        new BillingApi()
            .getTotalCost(query)
            .then((response) => {
                setIsLoading(false)

                // calc totals per cost_category
                const recTotals: { [key: string]: number } = {}
                response.data.forEach((item: BillingTotalCostRecord) => {
                    const { cost_category, cost } = item
                    if (cost_category !== undefined && cost_category !== null) {
                        if (!recTotals[cost_category]) {
                            recTotals[cost_category] = 0
                        }
                        recTotals[cost_category] += cost
                    }
                })
                const sortedRecTotals: { [key: string]: number } = Object.fromEntries(
                    Object.entries(recTotals).sort(([, a], [, b]) => b - a)
                )
                const rec_grps = Object.keys(sortedRecTotals)
                const records: { [key: string]: { [key: string]: number } } = {}
                response.data.forEach((item: BillingTotalCostRecord) => {
                    const { day, cost_category, cost } = item
                    if (
                        day !== undefined &&
                        day !== null &&
                        cost_category !== undefined &&
                        cost_category !== null
                    ) {
                        if (!records[day]) {
                            // initial day structure
                            records[day] = {}
                            rec_grps.forEach((k) => {
                                records[day][k] = 0
                            })
                        }
                        records[day][cost_category] = cost
                    }
                })
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

                            if (acc.length === maxDataPoints) {
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

    const messageComponent = () => {
        if (message) {
            return (
                <Message negative onDismiss={() => setError(undefined)}>
                    {message}
                </Message>
            )
        }
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
        return null
    }

    const dataComponent = () => {
        if (message || error || isLoading) {
            return null
        }

        if (!message && !error && !isLoading && (!data || data.length === 0)) {
            return (
                <Card
                    fluid
                    style={{ padding: '20px', overflowX: 'scroll' }}
                    id="billing-container-charts"
                >
                    No Data
                </Card>
            )
        }

        return (
            <>
                <Card
                    fluid
                    style={{ padding: '20px', overflowX: 'scroll' }}
                    id="billing-container-charts"
                >
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

    React.useEffect(() => {
        if (
            selectedData !== undefined &&
            selectedData !== '' &&
            selectedData !== null &&
            start !== undefined &&
            start !== '' &&
            start !== null &&
            end !== undefined &&
            end !== '' &&
            end !== null &&
            groupBy !== undefined &&
            groupBy !== null
        ) {
            // valid selection, retrieve data
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
        } else {
            // invalid selection,
            setIsLoading(false)
            setError(undefined)

            if (groupBy === undefined || groupBy === null) {
                // Group By not selected
                setMessage('Please select Group By')
            } else if (selectedData === undefined || selectedData === null || selectedData === '') {
                // Top Level not selected
                setMessage(`Please select ${groupBy}`)
            } else if (start === undefined || start === null || start === '') {
                setMessage('Please select Start date')
            } else if (end === undefined || end === null || end === '') {
                setMessage('Please select End date')
            } else {
                // generic message
                setMessage('Please make selection')
            }
        }
    }, [start, end, groupBy, selectedData])

    const exportOptions = [
            { key: 'csv', text: 'Export to CSV', value: 'csv' },
            { key: 'tsv', text: 'Export to TSV', value: 'tsv' },
        ]

    const exportToFile = (format: 'csv' | 'tsv') => {
        // Create header: Date plus group/column names
        const headerData = ["Date", ...groups];

        // Build rows: for each object in `data` (each date)
        const rowData = data.map((row) => {
            // Each `row` is { date, values }
            const dateStr =
                row.date instanceof Date ? row.date.toISOString().slice(0, 10) : String(row.date);

            // Get values for each group, blank if not found
            const vals = groups.map((group) =>
                row.values[group] !== undefined && row.values[group] !== null
                    ? Number(row.values[group]).toFixed(2)
                    : ""
            );

            return [dateStr, ...vals];
        });

        // Compose file content
        const separator = format === "csv" ? "," : "\t";
        const fileData = [headerData, ...rowData]
            .map((row) => row.join(separator))
            .join("\n");
        const blob = new Blob([fileData], { type: `text/${format}` });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        const fileName = `billing_cost_by_time.${format}`;
        link.setAttribute("download", fileName);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }

    return (
        <PaddedPage>
            <Card fluid style={{ padding: '20px' }} id="billing-container">
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <h1
                        style={{
                            fontSize: 40,
                        }}
                    >
                        Billing Cost By Time
                    </h1>
                    <div style={{ textAlign: 'right' }}>
                        <Dropdown
                            button
                            className="icon"
                            floating
                            labeled
                            icon="download"
                            options={exportOptions}
                            text="Export"
                            onChange={(_, data) => exportToFile(data.value as 'csv' | 'tsv')}
                        />
                    </div>
                </div>

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
            </Card>

            {messageComponent()}

            {dataComponent()}
        </PaddedPage>
    )
}

export default BillingCostByTime
