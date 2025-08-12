import { SelectChangeEvent } from '@mui/material/Select'
import orderBy from 'lodash/orderBy'
import * as React from 'react'
import { Link, useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import {
    Button,
    Checkbox,
    Dropdown,
    DropdownProps,
    Grid,
    Message,
    Table as SUITable,
} from 'semantic-ui-react'
import {
    ColumnConfig,
    ColumnGroup,
    ColumnVisibilityDropdown,
    useColumnVisibility,
} from '../../shared/components/ColumnVisibilityDropdown'
import { HorizontalStackedBarChart } from '../../shared/components/Graphs/HorizontalStackedBarChart'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import Table from '../../shared/components/Table'
import { exportTable } from '../../shared/utilities/exportTable'
import { convertFieldName } from '../../shared/utilities/fieldName'
import formatMoney from '../../shared/utilities/formatMoney'
import generateUrl from '../../shared/utilities/generateUrl'
import { BillingApi, BillingColumn, BillingCostBudgetRecord, BillingSource } from '../../sm-api'
import './components/BillingCostByTimeTable.css'
import FieldSelector from './components/FieldSelector'
import MultiFieldSelector from './components/MultiFieldSelector'

const BillingCurrentCost = () => {
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [openRows, setOpenRows] = React.useState<string[]>([])

    const [costRecords, setCosts] = React.useState<BillingCostBudgetRecord[]>([])
    const [allCostRecords, setAllCostRecords] = React.useState<BillingCostBudgetRecord[]>([]) // Store complete unfiltered dataset
    const [error, setError] = React.useState<string | undefined>()
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: 'undefined',
        direction: 'undefined',
    })

    const [showAsChart, setShowAsChart] = React.useState<boolean>(true)

    // State for column visibility
    const [visibleColumns, setVisibleColumns] = React.useState<Set<string>>(
        new Set([
            'field',
            'compute_daily',
            'storage_daily',
            'total_daily',
            'compute_monthly',
            'storage_monthly',
            'total_monthly',
            'budget_spent',
        ])
    )

    // Pull search params for use in the component
    const [searchParams] = useSearchParams()
    const inputGroupBy: string | null = searchParams.get('groupBy')
    const fixedGroupBy: BillingColumn = inputGroupBy
        ? (inputGroupBy as BillingColumn)
        : BillingColumn.GcpProject
    const inputInvoiceMonth = searchParams.get('invoiceMonth')
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

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = React.useCallback(
        (
            grp: BillingColumn,
            invoiceMonth: string | undefined,
            gcpProjects?: string[],
            topics?: string[]
        ) => {
            const url = generateUrl(location, {
                groupBy: grp,
                invoiceMonth: invoiceMonth,
                // Only include gcpProjects in URL if grouping by GCP Project and there are selected projects
                gcpProjects:
                    grp === BillingColumn.GcpProject && gcpProjects && gcpProjects.length > 0
                        ? gcpProjects.join(',')
                        : undefined,
                // Only include topics in URL if grouping by Topic and there are selected topics
                topics:
                    grp === BillingColumn.Topic && topics && topics.length > 0
                        ? topics.join(',')
                        : undefined,
            })
            navigate(url)
        },
        [location, navigate]
    )

    // toISOString() will give you YYYY-MM-DDTHH:mm:ss.sssZ
    // toISOString().substring(0, 7) will give you YYYY-MM
    // .replace('-', '') will give you YYYYMM
    const currentDate = new Date()
    const dayOfMonth = new Date().toISOString().substring(8, 10)
    if (parseInt(dayOfMonth, 10) < 3) {
        // first 2 days of the month, show previous month as default
        currentDate.setMonth(currentDate.getMonth() - 1)
    }
    const thisMonth = currentDate.toISOString().substring(0, 7).replace('-', '')

    const [groupBy, setGroupBy] = React.useState<BillingColumn>(
        fixedGroupBy ?? BillingColumn.GcpProject
    )
    const [invoiceMonth, setInvoiceMonth] = React.useState<string>(inputInvoiceMonth ?? thisMonth)
    const [selectedGcpProjects, setSelectedGcpProjects] =
        React.useState<string[]>(initialGcpProjects)
    const [selectedTopics, setSelectedTopics] = React.useState<string[]>(initialTopics)

    const [lastLoadedDay, setLastLoadedDay] = React.useState<string>()

    // Pre-fetched data state for MultiFieldSelector components
    const [availableGcpProjects, setAvailableGcpProjects] = React.useState<string[]>([])
    const [availableTopics, setAvailableTopics] = React.useState<string[]>([])
    const [_isPreFetching, setIsPreFetching] = React.useState<boolean>(true)

    // Pre-fetch GCP projects and topics data on component mount
    React.useEffect(() => {
        const preFetchData = async () => {
            setIsPreFetching(true)
            try {
                const [gcpProjectsResponse, topicsResponse] = await Promise.all([
                    new BillingApi().getGcpProjects(),
                    new BillingApi().getTopics(),
                ])

                setAvailableGcpProjects(gcpProjectsResponse.data || [])
                setAvailableTopics(topicsResponse.data || [])
            } catch (error) {
                console.error('Error pre-fetching data:', error)
                setAvailableGcpProjects([])
                setAvailableTopics([])
            } finally {
                setIsPreFetching(false)
            }
        }

        preFetchData()
    }, [])

    const buildCurrentFilters = React.useCallback(
        (grp: BillingColumn, gcpProjectFilters?: string[], topicFilters?: string[]) => {
            const gcpFiltersToUse =
                grp === BillingColumn.GcpProject
                    ? gcpProjectFilters !== undefined
                        ? gcpProjectFilters
                        : selectedGcpProjects
                    : []

            const topicFiltersToUse =
                grp === BillingColumn.Topic
                    ? topicFilters !== undefined
                        ? topicFilters
                        : selectedTopics
                    : []

            return { gcpFiltersToUse, topicFiltersToUse }
        },
        [selectedGcpProjects, selectedTopics]
    )

    const applyClientSideFilters = React.useCallback(
        (records: BillingCostBudgetRecord[], gcpFilters: string[], topicFilters: string[]) => {
            if (gcpFilters.length === 0 && topicFilters.length === 0) {
                return records
            }

            return records.filter((record) => {
                // For GCP Project grouping, filter by gcp_project field
                if (groupBy === BillingColumn.GcpProject && gcpFilters.length > 0) {
                    return gcpFilters.includes(record.field as string)
                }

                // For Topic grouping, filter by topic field
                if (groupBy === BillingColumn.Topic && topicFilters.length > 0) {
                    return topicFilters.includes(record.field as string)
                }

                return true
            })
        },
        [groupBy]
    )

    // Update filtered results whenever filters or data changes
    React.useEffect(() => {
        const { gcpFiltersToUse, topicFiltersToUse } = buildCurrentFilters(groupBy)
        const filteredRecords = applyClientSideFilters(
            allCostRecords,
            gcpFiltersToUse,
            topicFiltersToUse
        )
        setCosts(filteredRecords)
    }, [
        allCostRecords,
        selectedGcpProjects,
        selectedTopics,
        groupBy,
        buildCurrentFilters,
        applyClientSideFilters,
    ])

    const buildQueryModel = (
        grp: BillingColumn,
        invoiceMth: string | undefined,
        gcpFiltersToUse: string[],
        topicFiltersToUse: string[]
    ) => {
        let source = BillingSource.Aggregate
        if (grp === BillingColumn.GcpProject) {
            source = BillingSource.GcpBilling
        }

        return {
            field: grp,
            invoice_month: invoiceMth,
            source: source,
            filters:
                (grp === BillingColumn.GcpProject && gcpFiltersToUse.length > 0) ||
                (grp === BillingColumn.Topic && topicFiltersToUse.length > 0)
                    ? {
                          ...(grp === BillingColumn.GcpProject && gcpFiltersToUse.length > 0
                              ? { gcp_project: gcpFiltersToUse }
                              : {}),
                          ...(grp === BillingColumn.Topic && topicFiltersToUse.length > 0
                              ? { topic: topicFiltersToUse }
                              : {}),
                      }
                    : undefined,
        }
    }

    const getData = (grp: BillingColumn, invoiceMth: string | undefined) => {
        setIsLoading(true)
        setError(undefined)

        const queryModel = buildQueryModel(grp, invoiceMth, [], [])

        new BillingApi()
            .getRunningCost(queryModel)
            .then((response) => {
                setIsLoading(false)
                if (response.data.length > 0) {
                    setAllCostRecords(response.data)
                    setLastLoadedDay(response.data[0].last_loaded_day || '')
                } else {
                    setAllCostRecords([])
                }
            })
            .catch((er) => {
                setIsLoading(false)
                setError(er.message)
            })
    }

    const onGroupBySelect = (event: unknown, data: DropdownProps) => {
        const value = data.value
        if (typeof value == 'string') {
            setIsLoading(true)
            setGroupBy(value as BillingColumn)
            if (value !== BillingColumn.GcpProject) {
                setSelectedGcpProjects([])
            }
            if (value !== BillingColumn.Topic) {
                setSelectedTopics([])
            }
            updateNav(value as BillingColumn, invoiceMonth, [], [])
            getData(value as BillingColumn, invoiceMonth)
        }
    }

    const onInvoiceMonthSelect = (event: unknown, data: DropdownProps) => {
        const value = data.value
        if (typeof value == 'string') {
            setIsLoading(true)
            setInvoiceMonth(value)
            const currentGcpFilters =
                groupBy === BillingColumn.GcpProject ? selectedGcpProjects : []
            const currentTopicFilters = groupBy === BillingColumn.Topic ? selectedTopics : []
            updateNav(groupBy, value, currentGcpFilters, currentTopicFilters)
            getData(groupBy, value)
        }
    }

    const onGcpProjectsSelect = (
        event: SelectChangeEvent<string[]> | undefined,
        data: { value: string[] }
    ) => {
        setSelectedGcpProjects(data.value)
        updateNav(groupBy, invoiceMonth, data.value, selectedTopics)
    }

    const onTopicsSelect = (
        event: SelectChangeEvent<string[]> | undefined,
        data: { value: string[] }
    ) => {
        setSelectedTopics(data.value)
        updateNav(groupBy, invoiceMonth, selectedGcpProjects, data.value)
    }

    React.useEffect(() => {
        getData(groupBy, invoiceMonth)
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [])

    // Define column groups for easier management
    const DAILY_COLUMNS = ['compute_daily', 'storage_daily', 'total_daily']
    const MONTHLY_COLUMNS = ['compute_monthly', 'storage_monthly', 'total_monthly']
    const BUDGET_COLUMNS = ['budget_spent']

    // Define header fields for table rendering
    const HEADER_FIELDS = [
        { category: 'field', title: groupBy.toUpperCase() },
        // Daily columns (only if current month)
        ...(invoiceMonth === thisMonth
            ? [
                  { category: 'compute_daily', title: 'COMPUTE_DAILY' },
                  { category: 'storage_daily', title: 'STORAGE_DAILY' },
                  { category: 'total_daily', title: 'TOTAL_DAILY' },
              ]
            : []),
        // Monthly columns
        { category: 'compute_monthly', title: 'COMPUTE_MONTHLY' },
        { category: 'storage_monthly', title: 'STORAGE_MONTHLY' },
        { category: 'total_monthly', title: 'TOTAL_MONTHLY' },
    ]

    // Generate column configurations for the dropdown
    const getColumnConfigs = (): ColumnConfig[] => {
        const configs: ColumnConfig[] = [
            { id: 'field', label: convertFieldName(groupBy.toUpperCase()), isRequired: true },
        ]

        // Add daily columns if current month
        if (invoiceMonth === thisMonth) {
            configs.push(
                { id: 'compute_daily', label: 'Compute (Daily)', group: 'daily' },
                { id: 'storage_daily', label: 'Storage (Daily)', group: 'daily' },
                { id: 'total_daily', label: 'Total (Daily)', group: 'daily' }
            )
        }

        // Add monthly columns
        configs.push(
            { id: 'compute_monthly', label: 'Compute (Monthly)', group: 'monthly' },
            { id: 'storage_monthly', label: 'Storage (Monthly)', group: 'monthly' },
            { id: 'total_monthly', label: 'Total (Monthly)', group: 'monthly' }
        )

        // Add budget column if applicable
        if (
            groupBy === BillingColumn.GcpProject &&
            costRecords.length > 0 &&
            costRecords[0].budget_spent !== null
        ) {
            configs.push({ id: 'budget_spent', label: 'Budget Spent %', group: 'budget' })
        }

        return configs
    }

    // Generate column groups for the dropdown
    const getColumnGroups = (): ColumnGroup[] => {
        const groups: ColumnGroup[] = [
            { id: 'monthly', label: 'Monthly Costs', columns: MONTHLY_COLUMNS },
        ]

        if (invoiceMonth === thisMonth) {
            groups.unshift({ id: 'daily', label: 'Daily Costs', columns: DAILY_COLUMNS })
        }

        // Add budget group if budget data is available
        if (
            groupBy === BillingColumn.GcpProject &&
            costRecords.length > 0 &&
            costRecords[0].budget_spent !== null
        ) {
            groups.push({ id: 'budget', label: 'Budget', columns: BUDGET_COLUMNS })
        }

        return groups
    }

    // Use the column visibility hook for easier export handling
    const { isColumnVisible } = useColumnVisibility(getColumnConfigs(), visibleColumns)

    const handleToggle = (field: string) => {
        if (!openRows.includes(field)) {
            setOpenRows([...openRows, field])
        } else {
            setOpenRows(openRows.filter((i) => i !== field))
        }
    }

    function percFormat(num: number | undefined | null): string {
        if (num === undefined || num === null) {
            return ''
        }

        return `${num.toFixed(0).toString()} % `
    }

    if (error)
        return (
            <Message negative>
                {error}
                <br />
                <Button negative onClick={() => getData(groupBy, invoiceMonth)}>
                    Retry
                </Button>
            </Message>
        )

    const rowColor = (p: BillingCostBudgetRecord) => {
        if (p.budget_spent === undefined || p.budget_spent === null) {
            return ''
        }
        if (p.budget_spent > 90) {
            return 'billing-over-budget'
        }
        if (p.budget_spent > 50) {
            return 'billing-half-budget'
        }
        return 'billing-under-budget'
    }

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

    const linkTo = (data: string) => {
        // convert invoice month to start and end dates
        const year = invoiceMonth.substring(0, 4)
        const month = invoiceMonth.substring(4, 6)
        let nextYear = year
        let nextMonth = (parseInt(month, 10) + 1).toString().padStart(2, '0')
        if (month === '12') {
            nextYear = (parseInt(year, 10) + 1).toString()
            nextMonth = '01'
        }
        const startDate = `${year}-${month}-01`
        const nextMth = new Date(`${nextYear}-${nextMonth}-01`)
        nextMth.setDate(-0.01)
        const endDate = nextMth.toISOString().substring(0, 10)
        return `/billing/costByTime?groupBy=${groupBy}&selectedData=${data}&start=${startDate}&end=${endDate}`
    }

    const exportToFile = (format: 'csv' | 'tsv') => {
        // Filter based on user-selected visible columns
        const visibleFields = HEADER_FIELDS.filter((k) => isColumnVisible(k.category))
        const headerFields: string[] = visibleFields.map((k) => convertFieldName(k.category))

        // Add Budget % spend if it should be shown and is selected
        const budgetSpendVisible =
            groupBy === BillingColumn.GcpProject &&
            invoiceMonth === thisMonth &&
            isColumnVisible('budget_spent')

        if (budgetSpendVisible) headerFields.push('Budget Spend %')

        // Prepare 2D matrix of data strings
        const matrix = costRecords.map((rec) => {
            const row = visibleFields.map((k) =>
                k.category === 'field'
                    ? String(rec[k.category] ?? '')
                    : formatMoney(
                          (rec as unknown as Record<string, number | undefined>)[k.category] ?? 0
                      )
            )
            if (budgetSpendVisible) {
                row.push(percFormat(rec.budget_spent))
            }
            return row
        })

        exportTable({ headerFields, matrix }, format, 'billing-cost-by-invoice-month')
    }

    // Prepare header cells for daily and monthly columns (simplified, no block scope)
    const visibleDailyCount = ['compute_daily', 'storage_daily', 'total_daily'].filter((col) =>
        isColumnVisible(col)
    ).length
    const dailyHeaderCell =
        invoiceMonth === thisMonth && visibleDailyCount > 0 ? (
            <SUITable.HeaderCell colSpan={visibleDailyCount}>
                24H (day UTC {lastLoadedDay})
            </SUITable.HeaderCell>
        ) : null

    const baseMonthlyColumns = ['compute_monthly', 'storage_monthly', 'total_monthly']
    const budgetColumnVisible =
        groupBy === BillingColumn.GcpProject &&
        invoiceMonth === thisMonth &&
        isColumnVisible('budget_spent')
    const visibleMonthlyCount =
        baseMonthlyColumns.filter((col) => isColumnVisible(col)).length +
        (budgetColumnVisible ? 1 : 0)
    const monthlyHeaderCell =
        visibleMonthlyCount > 0 ? (
            <SUITable.HeaderCell colSpan={visibleMonthlyCount}>
                Invoice Month (Acc)
            </SUITable.HeaderCell>
        ) : null

    return (
        <>
            <h1>Cost By Invoice Month</h1>

            <Grid columns="equal" stackable doubling>
                <Grid.Column>
                    <FieldSelector
                        label="Group By"
                        fieldName="Group"
                        onClickFunction={onGroupBySelect}
                        selected={groupBy}
                    />
                </Grid.Column>
                <Grid.Column>
                    <FieldSelector
                        label="Invoice Month"
                        fieldName={BillingColumn.InvoiceMonth}
                        onClickFunction={onInvoiceMonthSelect}
                        selected={invoiceMonth}
                    />
                </Grid.Column>
                <Grid.Column>
                    <Checkbox
                        label="Show as Chart / Table"
                        fitted
                        toggle
                        checked={showAsChart}
                        onChange={() => setShowAsChart(!showAsChart)}
                    />
                </Grid.Column>

                {groupBy === BillingColumn.GcpProject && (
                    <Grid.Column>
                        <MultiFieldSelector
                            label="Filter GCP Projects"
                            fieldName={BillingColumn.GcpProject}
                            selected={selectedGcpProjects}
                            isApiLoading={isLoading}
                            preloadedData={availableGcpProjects}
                            onClickFunction={onGcpProjectsSelect}
                        />
                    </Grid.Column>
                )}

                {groupBy === BillingColumn.Topic && (
                    <Grid.Column>
                        <MultiFieldSelector
                            label="Filter Topics"
                            fieldName={BillingColumn.Topic}
                            selected={selectedTopics}
                            isApiLoading={isLoading}
                            preloadedData={availableTopics}
                            onClickFunction={onTopicsSelect}
                        />
                    </Grid.Column>
                )}

                <Grid.Column textAlign="right">
                    <div
                        className="button-container"
                        style={{
                            display: 'flex',
                            gap: '10px',
                            alignItems: 'stretch',
                            justifyContent: 'flex-end',
                            flex: '0 0 auto',
                            minWidth: '240px',
                        }}
                    >
                        <ColumnVisibilityDropdown
                            columns={getColumnConfigs()}
                            groups={getColumnGroups()}
                            visibleColumns={visibleColumns}
                            onVisibilityChange={setVisibleColumns}
                            searchThreshold={8}
                            searchPlaceholder="Search topics and months..."
                            enableUrlPersistence={true}
                            urlParamName="columns"
                            buttonStyle={{
                                minWidth: '115px',
                                height: '36px',
                            }}
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
                </Grid.Column>
            </Grid>

            {(() => {
                if (!showAsChart) return null
                if (String(invoiceMonth) === String(thisMonth)) {
                    return (
                        <Grid columns={2} stackable doubling>
                            <Grid.Column width={8} className="chart-card">
                                <HorizontalStackedBarChart
                                    data={costRecords}
                                    title={`24H (day UTC ${lastLoadedDay})`}
                                    series={['compute_daily', 'storage_daily']}
                                    labels={['Compute', 'Storage']}
                                    total_series="total_daily"
                                    threshold_values={[90, 50]}
                                    threshold_series="budget_spent"
                                    sorted_by="total_monthly"
                                    isLoading={isLoading}
                                    showLegend={false}
                                />
                            </Grid.Column>
                            <Grid.Column width={8} className="chart-card donut-chart">
                                <HorizontalStackedBarChart
                                    data={costRecords}
                                    title="Invoice Month (Acc)"
                                    series={['compute_monthly', 'storage_monthly']}
                                    labels={['Compute', 'Storage']}
                                    total_series="total_monthly"
                                    threshold_values={[90, 50]}
                                    threshold_series="budget_spent"
                                    sorted_by="total_monthly"
                                    isLoading={isLoading}
                                    showLegend={true}
                                />
                            </Grid.Column>
                        </Grid>
                    )
                }
                return (
                    <Grid>
                        <Grid.Column width={12}>
                            <HorizontalStackedBarChart
                                data={costRecords}
                                title="Invoice Month (Acc)"
                                series={['compute_monthly', 'storage_monthly']}
                                labels={['Compute', 'Storage']}
                                total_series="total_monthly"
                                sorted_by="total_monthly"
                                isLoading={isLoading}
                                showLegend={true}
                                // @ts-ignore
                                threshold_values={undefined}
                            />
                        </Grid.Column>
                    </Grid>
                )
            })()}

            {!showAsChart ? (
                <Table celled compact sortable>
                    <SUITable.Header>
                        <SUITable.Row>
                            <SUITable.HeaderCell></SUITable.HeaderCell>

                            <SUITable.HeaderCell></SUITable.HeaderCell>

                            {dailyHeaderCell}
                            {monthlyHeaderCell}
                        </SUITable.Row>
                        <SUITable.Row>
                            <SUITable.HeaderCell></SUITable.HeaderCell>

                            {HEADER_FIELDS.map((k) => {
                                // Only show columns that are visible
                                if (isColumnVisible(k.category)) {
                                    return (
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
                                    )
                                }
                                return null
                            })}

                            {groupBy === BillingColumn.GcpProject &&
                            invoiceMonth === thisMonth &&
                            isColumnVisible('budget_spent') ? (
                                <SUITable.HeaderCell
                                    key={'budget_spent'}
                                    sorted={checkDirection('budget_spent')}
                                    onClick={() => handleSort('budget_spent')}
                                    style={{
                                        borderBottom: 'none',
                                        position: 'sticky',
                                        resize: 'horizontal',
                                    }}
                                >
                                    Budget Spend %
                                </SUITable.HeaderCell>
                            ) : null}
                        </SUITable.Row>
                    </SUITable.Header>
                    <SUITable.Body>
                        {orderBy(
                            costRecords,
                            [sort.column],
                            sort.direction === 'ascending' ? ['asc'] : ['desc']
                        ).map((p) => (
                            // @ts-ignore
                            <React.Fragment key={`total - ${p.field}`}>
                                <SUITable.Row
                                    // @ts-ignore
                                    className={`${rowColor(p)}`}
                                    // @ts-ignore
                                    key={`total-row-${p.field}`}
                                    // @ts-ignore
                                    id={`total-row-${p.field}`}
                                >
                                    <SUITable.Cell collapsing>
                                        <Checkbox
                                            // @ts-ignore
                                            checked={openRows.includes(p.field)}
                                            slider
                                            // @ts-ignore
                                            onChange={() => handleToggle(p.field)}
                                        />
                                    </SUITable.Cell>
                                    {HEADER_FIELDS.map((k) => {
                                        // Skip columns that are not visible
                                        if (!isColumnVisible(k.category)) {
                                            return null
                                        }

                                        switch (k.category) {
                                            case 'field':
                                                return (
                                                    <SUITable.Cell
                                                        key={k.category}
                                                        className="billing-href"
                                                    >
                                                        <b>
                                                            <Link
                                                                to={
                                                                    // @ts-ignore
                                                                    linkTo(p[k.category])
                                                                }
                                                            >
                                                                {
                                                                    // @ts-ignore
                                                                    p[k.category]
                                                                }
                                                            </Link>
                                                        </b>
                                                    </SUITable.Cell>
                                                )
                                            default:
                                                // We already checked visibility above, so just render
                                                return (
                                                    <SUITable.Cell key={k.category}>
                                                        {
                                                            // @ts-ignore
                                                            formatMoney(p[k.category])
                                                        }
                                                    </SUITable.Cell>
                                                )
                                        }
                                    })}

                                    {groupBy === BillingColumn.GcpProject &&
                                    invoiceMonth === thisMonth &&
                                    isColumnVisible('budget_spent') ? (
                                        <SUITable.Cell>
                                            {
                                                // @ts-ignore
                                                percFormat(p.budget_spent)
                                            }
                                        </SUITable.Cell>
                                    ) : null}
                                </SUITable.Row>
                                {typeof p === 'object' &&
                                    'details' in p &&
                                    orderBy(p?.details, ['monthly_cost'], ['desc']).map((dk) => (
                                        <SUITable.Row
                                            style={{
                                                // @ts-ignore
                                                display: openRows.includes(p.field)
                                                    ? 'table-row'
                                                    : 'none',
                                                backgroundColor: 'var(--color-bg)',
                                            }}
                                            key={`${dk.cost_category} - ${p.field}`}
                                            id={`${dk.cost_category} - ${p.field}`}
                                        >
                                            <SUITable.Cell style={{ border: 'none' }} />
                                            <SUITable.Cell>{dk.cost_category}</SUITable.Cell>

                                            {dk.cost_group === 'C' ? (
                                                <React.Fragment>
                                                    {invoiceMonth === thisMonth &&
                                                    isColumnVisible('compute_daily') ? (
                                                        <SUITable.Cell>
                                                            {formatMoney(dk.daily_cost)}
                                                        </SUITable.Cell>
                                                    ) : null}

                                                    {/* Calculate colspan dynamically based on visible columns */}
                                                    {(() => {
                                                        // For the daily section, we need to check visibility of storage_daily and total_daily
                                                        if (invoiceMonth === thisMonth) {
                                                            const visibleCount =
                                                                (isColumnVisible('storage_daily')
                                                                    ? 1
                                                                    : 0) +
                                                                (isColumnVisible('total_daily')
                                                                    ? 1
                                                                    : 0)

                                                            return visibleCount > 0 ? (
                                                                <SUITable.Cell
                                                                    colSpan={visibleCount}
                                                                />
                                                            ) : null
                                                        }
                                                        return null
                                                    })()}

                                                    {isColumnVisible('compute_monthly') ? (
                                                        <SUITable.Cell>
                                                            {formatMoney(dk.monthly_cost)}
                                                        </SUITable.Cell>
                                                    ) : null}

                                                    {/* Calculate colspan dynamically based on visible columns */}
                                                    {(() => {
                                                        // For monthly section, check visibility of storage_monthly and total_monthly
                                                        const visibleCount =
                                                            (isColumnVisible('storage_monthly')
                                                                ? 1
                                                                : 0) +
                                                            (isColumnVisible('total_monthly')
                                                                ? 1
                                                                : 0)

                                                        return visibleCount > 0 ? (
                                                            <SUITable.Cell colSpan={visibleCount} />
                                                        ) : null
                                                    })()}
                                                </React.Fragment>
                                            ) : (
                                                <React.Fragment>
                                                    {isColumnVisible('compute_daily') ? (
                                                        <SUITable.Cell />
                                                    ) : null}

                                                    {invoiceMonth === thisMonth &&
                                                    isColumnVisible('storage_daily') ? (
                                                        <SUITable.Cell>
                                                            {formatMoney(dk.daily_cost)}
                                                        </SUITable.Cell>
                                                    ) : null}

                                                    {/* Calculate colspan for total_daily */}
                                                    {invoiceMonth === thisMonth &&
                                                    isColumnVisible('total_daily') ? (
                                                        <SUITable.Cell />
                                                    ) : null}

                                                    {isColumnVisible('compute_monthly') ? (
                                                        <SUITable.Cell />
                                                    ) : null}

                                                    {isColumnVisible('storage_monthly') ? (
                                                        <SUITable.Cell>
                                                            {formatMoney(dk.monthly_cost)}
                                                        </SUITable.Cell>
                                                    ) : null}

                                                    {isColumnVisible('total_monthly') ? (
                                                        <SUITable.Cell />
                                                    ) : null}
                                                </React.Fragment>
                                            )}

                                            {groupBy === BillingColumn.GcpProject &&
                                            invoiceMonth === thisMonth &&
                                            isColumnVisible('budget_spent') ? (
                                                <SUITable.Cell />
                                            ) : null}
                                        </SUITable.Row>
                                    ))}
                            </React.Fragment>
                        ))}
                    </SUITable.Body>
                </Table>
            ) : null}
        </>
    )
}

export default function BillingCurrentCostPage() {
    return (
        <PaddedPage>
            <BillingCurrentCost />
        </PaddedPage>
    )
}
