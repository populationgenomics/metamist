import { SelectChangeEvent } from '@mui/material/Select'
import { debounce } from 'lodash'
import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Button, Card, DropdownProps, Grid, Input, Message } from 'semantic-ui-react'
import { BarChart, IData } from '../../shared/components/Graphs/BarChart'
import { DonutChart } from '../../shared/components/Graphs/DonutChart'
import { IStackedAreaByDateChartData } from '../../shared/components/Graphs/StackedAreaByDateChart'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { exportTable } from '../../shared/utilities/exportTable'
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
import MultiFieldSelector from './components/MultiFieldSelector'

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

    // State for multiple project selection
    const [selectedProjects, setSelectedProjects] = React.useState<string[]>([])

    // Max Aggregated Data Points, rest will be aggregated into "Rest"
    const maxDataPoints = 7

    // Data loading
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [message, setMessage] = React.useState<string | undefined>()
    const [groups, setGroups] = React.useState<string[]>([])
    const [data, setData] = React.useState<IStackedAreaByDateChartData[]>([])
    const [aggregatedData, setAggregatedData] = React.useState<IData[]>([])
    const [visibleColumns, setVisibleColumns] = React.useState<Set<string>>(new Set())

    // Add expand state management
    const [expandCompute, setExpandCompute] = React.useState<boolean>(
        searchParams.get('expand') === 'true'
    )

    // Create a debounced version of getData for project selections
    const debouncedGetData = React.useMemo(
        () =>
            debounce((query: BillingTotalCostQueryModel) => {
                getData(query)
            }, 1000), // 1000ms delay
        // eslint-disable-next-line react-hooks/exhaustive-deps
        []
    )

    // Cleanup debounced function on unmount
    React.useEffect(() => {
        return () => {
            debouncedGetData.cancel()
        }
    }, [debouncedGetData])

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (
        grp: string | undefined,
        selData: string | undefined,
        st: string,
        ed: string,
        expand?: boolean,
        columns?: Set<string>
    ) => {
        const searchParams = new URLSearchParams(location.search)

        // Handle expand parameter
        if (expand !== undefined) {
            if (expand) {
                searchParams.set('expand', 'true')
            } else {
                searchParams.delete('expand')
            }
        }

        // Handle columns parameter
        if (columns !== undefined && columns.size > 0) {
            const columnsArray = Array.from(columns).sort()
            searchParams.set('columns', columnsArray.join(','))
        }

        const url = generateUrl(location, {
            groupBy: grp,
            selectedData: selData,
            start: st,
            end: ed,
            expand: searchParams.get('expand') || undefined,
            columns: searchParams.get('columns') || undefined,
        })
        navigate(url)
    }

    const onGroupBySelect = (event: unknown, data: DropdownProps) => {
        const value = data.value
        if (typeof value == 'string') {
            setGroupBy(value as BillingColumn)
            setSelectedData(undefined)
            // Clear project selections when switching away from GCP Project grouping
            if (value !== BillingColumn.GcpProject) {
                setSelectedProjects([])
            }
            updateNav(value, undefined, start, end, expandCompute, visibleColumns)
        }
    }

    const onSelect = (event: unknown, data: DropdownProps) => {
        const value = data.value
        if (typeof value == 'string') {
            setSelectedData(value)
            updateNav(groupBy, value, start, end, expandCompute, visibleColumns)
        }
    }

    const onProjectsSelect = (
        event: SelectChangeEvent<string[]> | undefined,
        data: { value: string[] }
    ) => {
        // Update UI state immediately for responsive feedback
        setSelectedProjects(data.value)
        // Use debounced version for API calls to prevent excessive requests during rapid selections
        if (Boolean(start) && Boolean(end) && groupBy === BillingColumn.GcpProject) {
            const queryModel = buildProjectQueryModel(data.value)
            debouncedGetData(queryModel)
        }
    }

    const buildProjectQueryModel = React.useCallback(
        (projects: string[]): BillingTotalCostQueryModel => {
            const baseQuery: BillingTotalCostQueryModel = {
                fields: [BillingColumn.Day, BillingColumn.CostCategory],
                start_date: start,
                end_date: end,
                order_by: { day: false },
                source: BillingSource.GcpBilling,
            }

            // Add project filter if projects are selected
            if (projects.length > 0) {
                baseQuery.filters = {
                    gcp_project: projects,
                }
            }

            return baseQuery
        },
        [start, end]
    )

    const changeDate = (name: string, value: string) => {
        let start_update = start
        let end_update = end
        if (name === 'start') start_update = value
        if (name === 'end') end_update = value
        setStart(start_update)
        setEnd(end_update)
        updateNav(groupBy, selectedData, start_update, end_update, expandCompute, visibleColumns)
    }

    // Custom handlers that update URL
    const handleExpandChange = (expand: boolean) => {
        setExpandCompute(expand)
        updateNav(groupBy, selectedData, start, end, expand, visibleColumns)
    }

    const handleColumnsChange = (columns: Set<string>) => {
        setVisibleColumns(columns)
        updateNav(groupBy, selectedData, start, end, expandCompute, columns)
    }

    const getData = React.useCallback(
        (query: BillingTotalCostQueryModel) => {
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

                    // Include additional columns that will be added by the table component
                    const allColumns = [
                        ...no_undefined,
                        'Daily Total',
                        'Cloud Storage',
                        'Compute Cost',
                    ]

                    // Check for URL parameters first
                    const urlColumns = searchParams.get('columns')
                    if (urlColumns) {
                        const columnsFromUrl = urlColumns.split(',').filter(Boolean)
                        const validColumns = columnsFromUrl.filter((col) =>
                            allColumns.includes(col)
                        )
                        if (validColumns.length > 0) {
                            setVisibleColumns(new Set(validColumns))
                        } else {
                            setVisibleColumns(new Set(allColumns))
                        }
                    } else {
                        setVisibleColumns(new Set(allColumns))
                    }
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
        },
        [
            searchParams,
            setIsLoading,
            setError,
            setMessage,
            setGroups,
            setVisibleColumns,
            setData,
            setAggregatedData,
        ]
    )

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
                        visibleColumns={visibleColumns}
                        setVisibleColumns={handleColumnsChange}
                        expandCompute={expandCompute}
                        setExpandCompute={handleExpandChange}
                        exportToFile={exportToFile}
                        groupBy={groupBy}
                        selectedProjects={selectedProjects}
                    />
                </Card>
            </>
        )
    }

    // Separate useEffect for initial load and date/groupBy changes
    React.useEffect(() => {
        // Check if we have valid date and groupBy selections
        if (
            start !== undefined &&
            start !== '' &&
            start !== null &&
            end !== undefined &&
            end !== '' &&
            end !== null &&
            groupBy !== undefined &&
            groupBy !== null
        ) {
            // For GCP Project grouping, use project selector logic
            if (groupBy === BillingColumn.GcpProject) {
                const queryModel = buildProjectQueryModel(selectedProjects)
                getData(queryModel)
            }
            // For other groupings, use the existing selectedData logic
            else if (selectedData !== undefined && selectedData !== '' && selectedData !== null) {
                const source = BillingSource.Aggregate
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
                // For non-GCP groupings, require selectedData
                setIsLoading(false)
                setError(undefined)
                setMessage(`Please select ${groupBy}`)
            }
        } else {
            // invalid selection,
            setIsLoading(false)
            setError(undefined)

            if (groupBy === undefined || groupBy === null) {
                // Group By not selected
                setMessage('Please select Group By')
            } else if (start === undefined || start === null || start === '') {
                setMessage('Please select Start date')
            } else if (end === undefined || end === null || end === '') {
                setMessage('Please select End date')
            } else {
                // generic message
                setMessage('Please make selection')
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [start, end, groupBy, selectedData])

    const exportToFile = (format: 'csv' | 'tsv') => {
        // Use the same priority columns and sorting logic as the table
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

        // Create the full list of possible columns in the correct order
        const allPossibleColumns = [...groups, 'Daily Total', 'Cloud Storage', 'Compute Cost']
            .filter((column, index, arr) => arr.indexOf(column) === index) // Remove duplicates
            .sort(headerSort)

        // Filter to only visible columns
        const visibleGroups = allPossibleColumns.filter((group) => visibleColumns.has(group))

        const headerFields = ['Date', ...visibleGroups]
        const matrix = data.map((row) => {
            const dateStr = row.date.toISOString().slice(0, 10)
            const total = Object.values(row.values).reduce((acc, cur) => acc + cur, 0)
            const computeCost = total - row.values['Cloud Storage']

            const vals = visibleGroups.map((group) => {
                let val: number
                if (group === 'Daily Total') {
                    val = total
                } else if (group === 'Compute Cost') {
                    val = computeCost
                } else {
                    val = row.values[group]
                }

                if (typeof val === 'number') {
                    // leave blank if value is exactly 0
                    return val === 0 ? '' : Number(val).toFixed(2)
                }
                return ''
            })
            return [dateStr, ...vals]
        })
        exportTable(
            {
                headerFields,
                matrix,
            },
            format,
            'billing_cost_by_time'
        )
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
                        {groupBy === BillingColumn.GcpProject ? (
                            <MultiFieldSelector
                                label="Filter GCP Projects"
                                fieldName={BillingColumn.GcpProject}
                                selected={selectedProjects}
                                isApiLoading={isLoading}
                                onClickFunction={onProjectsSelect}
                            />
                        ) : (
                            <FieldSelector
                                label={convertFieldName(groupBy)}
                                fieldName={groupBy}
                                onClickFunction={onSelect}
                                selected={selectedData}
                                includeAll={true}
                                autoSelect={true}
                            />
                        )}
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
