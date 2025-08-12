import { SelectChangeEvent } from '@mui/material/Select'
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

    // Get initial projects and topics from URL parameters
    const inputProjects = searchParams.get('projects')
    const initialProjects = inputProjects
        ? inputProjects.split(',').filter((p) => p.trim() !== '')
        : []
    const inputTopics = searchParams.get('topics')
    const initialTopics = inputTopics ? inputTopics.split(',').filter((t) => t.trim() !== '') : []

    // State for multiple project selection
    const [selectedProjects, setSelectedProjects] = React.useState<string[]>(initialProjects)

    // State for multiple topic selection
    const [selectedTopics, setSelectedTopics] = React.useState<string[]>(initialTopics)

    // Pre-fetched data state for MultiFieldSelector components
    const [availableGcpProjects, setAvailableGcpProjects] = React.useState<string[]>([])
    const [availableTopics, setAvailableTopics] = React.useState<string[]>([])
    const [_isPreFetching, setIsPreFetching] = React.useState<boolean>(true)

    // Max Aggregated Data Points, rest will be aggregated into "Rest"
    const maxDataPoints = 7

    // Data loading
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [message, setMessage] = React.useState<string | undefined>()
    const [groups, setGroups] = React.useState<string[]>([])
    const [data, setData] = React.useState<IStackedAreaByDateChartData[]>([])
    const [allData, setAllData] = React.useState<BillingTotalCostRecord[]>([]) // Store complete unfiltered dataset
    const [aggregatedData, setAggregatedData] = React.useState<IData[]>([])
    const [visibleColumns, setVisibleColumns] = React.useState<Set<string>>(new Set())

    // Add expand state management
    const [expandCompute, setExpandCompute] = React.useState<boolean>(
        searchParams.get('expand') === 'true'
    )

    // Pre-fetch GCP projects and topics data on component mount
    React.useEffect(() => {
        const preFetchData = async () => {
            setIsPreFetching(true)
            try {
                // Pre-fetch GCP projects and topics in parallel
                const [gcpProjectsResponse, topicsResponse] = await Promise.all([
                    new BillingApi().getGcpProjects(),
                    new BillingApi().getTopics(),
                ])

                setAvailableGcpProjects(gcpProjectsResponse.data || [])
                setAvailableTopics(topicsResponse.data || [])
            } catch (error) {
                console.error('Error pre-fetching data:', error)
                // Fallback: let MultiFieldSelector handle individual fetching
                setAvailableGcpProjects([])
                setAvailableTopics([])
            } finally {
                setIsPreFetching(false)
            }
        }

        preFetchData()
    }, [])

    // Client-side filtering function
    const applyClientSideFilters = React.useCallback(
        (records: BillingTotalCostRecord[], gcpProjects: string[], topics: string[]) => {
            if (gcpProjects.length === 0 && topics.length === 0) {
                return records
            }

            return records.filter((record) => {
                // For GCP Project filtering (only when we have the gcp_project field)
                if (gcpProjects.length > 0 && record.gcp_project) {
                    if (!gcpProjects.includes(record.gcp_project)) {
                        return false
                    }
                }

                // For Topic filtering (only when we have the topic field)
                if (topics.length > 0 && record.topic) {
                    if (!topics.includes(record.topic)) {
                        return false
                    }
                }

                return true
            })
        },
        []
    )

    // Process filtered data into chart format
    const processFilteredData = React.useCallback(
        (filteredRecords: BillingTotalCostRecord[]) => {
            // calc totals per cost_category
            const recTotals: { [key: string]: number } = {}
            filteredRecords.forEach((item: BillingTotalCostRecord) => {
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
            filteredRecords.forEach((item: BillingTotalCostRecord) => {
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
            const allColumns = [...no_undefined, 'Daily Total', 'Cloud Storage', 'Compute Cost']

            // Check for URL parameters first
            const urlColumns = searchParams.get('columns')
            if (urlColumns) {
                const columnsFromUrl = urlColumns.split(',').filter(Boolean)
                const validColumns = columnsFromUrl.filter((col) => allColumns.includes(col))
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
        },
        [searchParams]
    )

    // Update filtered results whenever filters or data change
    React.useEffect(() => {
        const filteredRecords = applyClientSideFilters(allData, selectedProjects, selectedTopics)
        processFilteredData(filteredRecords)
    }, [allData, selectedProjects, selectedTopics, applyClientSideFilters, processFilteredData])

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (
        grp: string | undefined,
        selData: string | undefined,
        st: string,
        ed: string,
        expand?: boolean,
        columns?: Set<string>,
        projects?: string[],
        topics?: string[]
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

        // Handle projects parameter
        if (projects !== undefined) {
            if (projects.length > 0) {
                searchParams.set('projects', projects.join(','))
            } else {
                searchParams.delete('projects')
            }
        }

        // Handle topics parameter
        if (topics !== undefined) {
            if (topics.length > 0) {
                searchParams.set('topics', topics.join(','))
            } else {
                searchParams.delete('topics')
            }
        }

        const url = generateUrl(location, {
            groupBy: grp,
            selectedData: selData,
            start: st,
            end: ed,
            expand: searchParams.get('expand') || undefined,
            columns: searchParams.get('columns') || undefined,
            projects: searchParams.get('projects') || undefined,
            topics: searchParams.get('topics') || undefined,
        })
        navigate(url)
    }

    const onGroupBySelect = (event: unknown, data: DropdownProps) => {
        const value = data.value
        if (typeof value == 'string') {
            setGroupBy(value as BillingColumn)
            setSelectedData(undefined)
            // Clear filters when switching groupBy
            if (value !== BillingColumn.GcpProject) {
                setSelectedProjects([])
            }
            if (value !== BillingColumn.Topic) {
                setSelectedTopics([])
            }
            updateNav(value, undefined, start, end, expandCompute, visibleColumns, [], [])
        }
    }

    const onSelect = (event: unknown, data: DropdownProps) => {
        const value = data.value
        if (typeof value == 'string') {
            setSelectedData(value)
            updateNav(
                groupBy,
                value,
                start,
                end,
                expandCompute,
                visibleColumns,
                selectedProjects,
                selectedTopics
            )
        }
    }

    const onProjectsSelect = (
        event: SelectChangeEvent<string[]> | undefined,
        data: { value: string[] }
    ) => {
        setSelectedProjects(data.value)
        updateNav(
            groupBy,
            selectedData,
            start,
            end,
            expandCompute,
            visibleColumns,
            data.value,
            selectedTopics
        )
    }

    const onTopicsSelect = (
        event: SelectChangeEvent<string[]> | undefined,
        data: { value: string[] }
    ) => {
        setSelectedTopics(data.value)
        updateNav(
            groupBy,
            selectedData,
            start,
            end,
            expandCompute,
            visibleColumns,
            selectedProjects,
            data.value
        )
    }

    const buildFilteredQueryModel = React.useCallback((): BillingTotalCostQueryModel => {
        // Build fields array based on what we need for client-side filtering
        const baseFields = [BillingColumn.Day, BillingColumn.CostCategory]

        // Add fields based on the data source capabilities
        if (groupBy === BillingColumn.GcpProject) {
            // GcpBilling source supports gcp_project field
            baseFields.push(BillingColumn.GcpProject)
        } else if (groupBy === BillingColumn.Topic) {
            // Aggregate source supports topic field
            baseFields.push(BillingColumn.Topic)
        }

        const baseQuery: BillingTotalCostQueryModel = {
            fields: baseFields,
            start_date: start,
            end_date: end,
            order_by: { day: false },
            source:
                groupBy === BillingColumn.GcpProject
                    ? BillingSource.GcpBilling
                    : BillingSource.Aggregate,
        }
        return baseQuery
    }, [start, end, groupBy])

    const changeDate = (name: string, value: string) => {
        let start_update = start
        let end_update = end
        if (name === 'start') start_update = value
        if (name === 'end') end_update = value
        setStart(start_update)
        setEnd(end_update)
        updateNav(
            groupBy,
            selectedData,
            start_update,
            end_update,
            expandCompute,
            visibleColumns,
            selectedProjects,
            selectedTopics
        )
    }

    const handleExpandChange = (expand: boolean) => {
        setExpandCompute(expand)
        updateNav(
            groupBy,
            selectedData,
            start,
            end,
            expand,
            visibleColumns,
            selectedProjects,
            selectedTopics
        )
    }

    const handleColumnsChange = (columns: Set<string>) => {
        setVisibleColumns(columns)
        updateNav(
            groupBy,
            selectedData,
            start,
            end,
            expandCompute,
            columns,
            selectedProjects,
            selectedTopics
        )
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
                    setAllData(response.data)
                })
                .catch((er) => setError(er.message))
        },
        [setIsLoading, setError, setMessage, setAllData]
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
            // For GCP Project and Topic grouping, fetch complete dataset for client-side filtering
            if (groupBy === BillingColumn.GcpProject || groupBy === BillingColumn.Topic) {
                const queryModel = buildFilteredQueryModel()
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
                // For non-GCP and non-Topic groupings, require selectedData
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
                                preloadedData={availableGcpProjects}
                            />
                        ) : groupBy === BillingColumn.Topic ? (
                            <MultiFieldSelector
                                label="Filter Topics"
                                fieldName={BillingColumn.Topic}
                                selected={selectedTopics}
                                isApiLoading={isLoading}
                                onClickFunction={onTopicsSelect}
                                preloadedData={availableTopics}
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
