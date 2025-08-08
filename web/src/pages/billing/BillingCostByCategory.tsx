import { SelectChangeEvent } from '@mui/material/Select'
import { debounce } from 'lodash'
import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Button, Card, Checkbox, Dropdown, Grid, Input, Message } from 'semantic-ui-react'
import {
    ColumnConfig,
    ColumnGroup,
    ColumnVisibilityDropdown,
    useColumnVisibility,
} from '../../shared/components/ColumnVisibilityDropdown'
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
import './components/BillingCostByTimeTable.css'
import CostByTimeBarChart from './components/CostByTimeBarChart'
import FieldSelector from './components/FieldSelector'
import MultiFieldSelector from './components/MultiFieldSelector'

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

    // GCP projects are stored as comma-separated values in URL: gcpProjects=project1,project2,project3
    const inputGcpProjects = searchParams.get('gcpProjects')
    // Only use GCP projects from URL if we're grouping by GCP Project
    const initialGcpProjects =
        fixedGroupBy === BillingColumn.GcpProject && inputGcpProjects
            ? inputGcpProjects.split(',').filter((p) => p.trim() !== '')
            : []

    // Topics are stored as comma-separated values in URL: topics=topic1,topic2,topic3
    const inputTopics = searchParams.get('topics')
    // Only use topics from URL if we're grouping by Topic
    const initialTopics =
        fixedGroupBy === BillingColumn.Topic && inputTopics
            ? inputTopics.split(',').filter((t) => t.trim() !== '')
            : []

    const [start, setStart] = React.useState<string>(
        searchParams.get('start') ?? getMonthStartDate()
    )
    const [end, setEnd] = React.useState<string>(searchParams.get('end') ?? getMonthEndDate())

    const [selectedGroup, setSelectedGroup] = React.useState<string | undefined>(inputSelectedGroup)
    const [selectedCostCategory, setCostCategory] = React.useState<string | undefined>(
        inputCostCategory
    )

    const [selectedPeriod, setPeriod] = React.useState<string | undefined>(inputPeriod)

    // State for multiple project selection
    const [selectedProjects, setSelectedProjects] = React.useState<string[]>(initialGcpProjects)

    // State for multiple topic selection
    const [selectedTopics, setSelectedTopics] = React.useState<string[]>(initialTopics)

    // Pre-fetched data state for MultiFieldSelector components
    const [availableGcpProjects, setAvailableGcpProjects] = React.useState<string[]>([])
    const [availableTopics, setAvailableTopics] = React.useState<string[]>([])
    const [_isPreFetching, setIsPreFetching] = React.useState<boolean>(true)

    // Data loading
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [data, setData] = React.useState<IStackedAreaByDateChartData[]>([])

    const [groupBy, setGroupBy] = React.useState<BillingColumn>(
        fixedGroupBy ?? BillingColumn.GcpProject
    )

    const [accumulate, setAccumulate] = React.useState<boolean>(true)
    const [visibleColumns, setVisibleColumns] = React.useState<Set<string>>(new Set())
    const [urlInitialized, setUrlInitialized] = React.useState(false)

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

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (
        grpBy: BillingColumn,
        grp: string | undefined,
        category: string | undefined,
        period: string | undefined,
        st: string,
        ed: string,
        gcpProjects?: string[],
        topics?: string[]
    ) => {
        const searchParams = new URLSearchParams(location.search)
        const columnsParam = searchParams.get('columns')

        const url = generateUrl(location, {
            groupBy: grpBy,
            group: grp,
            costCategory: category,
            period: period,
            start: st,
            end: ed,
            // Only include gcpProjects in URL if grouping by GCP Project and there are selected projects
            gcpProjects:
                grpBy === BillingColumn.GcpProject && gcpProjects && gcpProjects.length > 0
                    ? gcpProjects.join(',')
                    : undefined,
            // Only include topics in URL if grouping by Topic and there are selected topics
            topics:
                grpBy === BillingColumn.Topic && topics && topics.length > 0
                    ? topics.join(',')
                    : undefined,
            // Preserve existing columns parameter if it exists
            ...(columnsParam && { columns: columnsParam }),
        })
        navigate(url)
    }

    const onGroupBySelect = (event: any, recs: any) => {
        setGroupBy(recs.value)
        setSelectedGroup(undefined)
        // Clear filters when switching groupBy
        if (recs.value !== BillingColumn.GcpProject) {
            setSelectedProjects([])
        }
        if (recs.value !== BillingColumn.Topic) {
            setSelectedTopics([])
        }
        updateNav(
            recs.value,
            undefined,
            selectedCostCategory,
            selectedPeriod,
            start,
            end,
            recs.value === BillingColumn.GcpProject ? selectedProjects : [],
            recs.value === BillingColumn.Topic ? selectedTopics : []
        )
    }

    const onSelectGroup = (event: any, recs: any) => {
        setSelectedGroup(recs.value)
        updateNav(
            groupBy,
            recs.value,
            selectedCostCategory,
            selectedPeriod,
            start,
            end,
            selectedProjects,
            selectedTopics
        )
    }

    const onSelectCategory = (event: any, recs: any) => {
        setCostCategory(recs.value)
        updateNav(
            groupBy,
            selectedGroup,
            recs.value,
            selectedPeriod,
            start,
            end,
            selectedProjects,
            selectedTopics
        )
    }

    const onSelectPeriod = (event: any, recs: any) => {
        setPeriod(recs.value)
        updateNav(
            groupBy,
            selectedGroup,
            selectedCostCategory,
            recs.value,
            start,
            end,
            selectedProjects,
            selectedTopics
        )
    }

    const onProjectsSelect = (
        event: SelectChangeEvent<string[]> | undefined,
        data: { value: string[] }
    ) => {
        // Update UI state immediately for responsive feedback
        setSelectedProjects(data.value)
        // Update URL parameters to persist selection
        updateNav(
            groupBy,
            selectedGroup,
            selectedCostCategory,
            selectedPeriod,
            start,
            end,
            data.value,
            selectedTopics
        )
        // Use debounced version for API calls to prevent excessive requests during rapid selections
        if (Boolean(start) && Boolean(end) && groupBy === BillingColumn.GcpProject) {
            const query = buildFilteredQueryModel(data.value, [])
            debouncedGetData(query)
        }
    }

    const onTopicsSelect = (
        event: SelectChangeEvent<string[]> | undefined,
        data: { value: string[] }
    ) => {
        // Update UI state immediately for responsive feedback
        setSelectedTopics(data.value)
        // Update URL parameters to persist selection
        updateNav(
            groupBy,
            selectedGroup,
            selectedCostCategory,
            selectedPeriod,
            start,
            end,
            selectedProjects,
            data.value
        )
        // Use debounced version for API calls to prevent excessive requests during rapid selections
        if (Boolean(start) && Boolean(end) && groupBy === BillingColumn.Topic) {
            const query = buildFilteredQueryModel([], data.value)
            debouncedGetData(query)
        }
    }

    const buildFilteredQueryModel = React.useCallback(
        (projects: string[], topics: string[]): BillingTotalCostQueryModel => {
            const selFilters: { [key: string]: string | string[] } = {}

            // Add project filter if projects are selected
            if (projects.length > 0) {
                selFilters[BillingColumn.GcpProject] = projects
            }

            // Add topic filter if topics are selected
            if (topics.length > 0) {
                selFilters[BillingColumn.Topic] = topics
            }

            // Add other filters
            if (selectedCostCategory && !selectedCostCategory.startsWith('All ')) {
                selFilters.cost_category = selectedCostCategory
            }

            return {
                fields: [BillingColumn.Sku],
                start_date: start,
                end_date: end,
                filters: selFilters,
                order_by: { day: false },
                time_periods: selectedPeriod as BillingTimePeriods,
                // show only records with cost > 0.01
                min_cost: 0.01,
            }
        },
        [start, end, selectedCostCategory, selectedPeriod]
    )

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
            end_update,
            selectedProjects,
            selectedTopics
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
        const selFilters: { [key: string]: string | string[] } = {}

        // For GCP Project grouping, use project filtering instead of selectedGroup
        if (groupBy === BillingColumn.GcpProject) {
            if (selectedProjects.length > 0) {
                selFilters[BillingColumn.GcpProject] = selectedProjects
            }
        }
        // For Topic grouping, use topic filtering instead of selectedGroup
        else if (groupBy === BillingColumn.Topic) {
            if (selectedTopics.length > 0) {
                selFilters[BillingColumn.Topic] = selectedTopics
            }
        }
        // For other groupings, use selectedGroup
        else if (groupBy && selectedGroup && !selectedGroup.startsWith('All ')) {
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
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [
        groupBy,
        selectedGroup,
        selectedCostCategory,
        selectedPeriod,
        start,
        end,
        selectedProjects,
        selectedTopics,
    ])

    // Generate column configurations for the dropdown
    const getColumnConfigs = React.useCallback((): ColumnConfig[] => {
        const configs: ColumnConfig[] = []

        // Add SKU columns based on available data
        const skuSet = new Set<string>()
        data.forEach((row) => {
            Object.keys(row.values).forEach((sku) => skuSet.add(sku))
        })

        const skus = [...skuSet].sort()
        skus.forEach((sku) => {
            configs.push({ id: sku, label: sku, group: 'skus' })
        })

        return configs
    }, [data])

    // Generate column groups for the dropdown
    const getColumnGroups = React.useCallback((): ColumnGroup[] => {
        const skuColumns = getColumnConfigs()
            .filter((config) => config.group === 'skus')
            .map((config) => config.id)

        return [{ id: 'skus', label: 'SKUs', columns: skuColumns }]
    }, [getColumnConfigs])

    // Use the column visibility hook
    const { isColumnVisible } = useColumnVisibility(getColumnConfigs(), visibleColumns)

    // Initialize visible columns when data changes
    React.useEffect(() => {
        if (data.length > 0 && !urlInitialized) {
            const skuSet = new Set<string>()
            data.forEach((row) => {
                Object.keys(row.values).forEach((sku) => skuSet.add(sku))
            })
            const skus = [...skuSet].sort()

            // Check for URL parameters first
            const urlColumns = searchParams.get('columns')
            if (urlColumns) {
                const columnsFromUrl = urlColumns.split(',').filter(Boolean)
                const validColumns = columnsFromUrl.filter((sku) => skus.includes(sku))
                if (validColumns.length > 0) {
                    setVisibleColumns(new Set(validColumns))
                    setUrlInitialized(true)
                    return
                }
            }

            // No valid URL parameters, set defaults (all SKUs visible)
            setVisibleColumns(new Set(skus))
            setUrlInitialized(true)
        }
    }, [data, urlInitialized, searchParams])

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

        // Filter SKUs by visible columns
        const visibleSkus = skus.filter((sku) => isColumnVisible(sku))
        const headerFields = ['Date', ...visibleSkus]

        const matrix = dates.map((date) => {
            const values = dateToValues.get(date) || {}
            const rowCells = visibleSkus.map((sku) => {
                const cost = values[sku]
                return typeof cost === 'number' && cost !== 0 ? cost.toFixed(2) : ''
            })
            return [date, ...rowCells]
        })

        exportTable({ headerFields, matrix }, format, 'billing_cost_by_category')
    }

    return (
        <>
            <Card fluid style={{ padding: '20px' }} id="billing-container">
                <div
                    className="header-container"
                    style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'flex-start',
                        flexWrap: 'wrap',
                        gap: '10px',
                    }}
                >
                    <h1
                        style={{
                            fontSize: 40,
                            margin: 0,
                            flex: '1 1 200px',
                        }}
                    >
                        Billing Cost By Category
                    </h1>
                    <div
                        className="button-container"
                        style={{
                            display: 'flex',
                            gap: '10px',
                            alignItems: 'stretch',
                            flex: '0 0 auto',
                            minWidth: '240px',
                        }}
                    >
                        <ColumnVisibilityDropdown
                            columns={getColumnConfigs()}
                            groups={getColumnGroups()}
                            visibleColumns={visibleColumns}
                            onVisibilityChange={setVisibleColumns}
                            buttonStyle={{
                                marginRight: '0px',
                                minWidth: '115px',
                                height: '36px',
                            }}
                            searchThreshold={8}
                            searchPlaceholder="Search SKUs..."
                            enableUrlPersistence={true}
                            urlParamName="columns"
                        />
                        <Dropdown
                            button
                            className="icon"
                            floating
                            labeled
                            icon="download"
                            text="Export"
                            style={{
                                minWidth: '115px',
                                height: '36px',
                            }}
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
                                onClickFunction={onSelectGroup}
                                selected={selectedGroup}
                                includeAll={true}
                                autoSelect={true}
                            />
                        )}
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
                            data={data.map((row) => ({
                                ...row,
                                values: Object.fromEntries(
                                    Object.entries(row.values).filter(([sku]) =>
                                        isColumnVisible(sku)
                                    )
                                ),
                            }))}
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
