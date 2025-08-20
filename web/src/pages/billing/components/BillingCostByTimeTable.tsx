import { Switch, ToggleButton, ToggleButtonGroup } from '@mui/material'
import React from 'react'
import { Dropdown, Header, Table as SUITable } from 'semantic-ui-react'
import {
    ColumnConfig,
    ColumnGroup,
    ColumnVisibilityDropdown,
} from '../../../shared/components/ColumnVisibilityDropdown'
import { IStackedAreaByDateChartData } from '../../../shared/components/Graphs/StackedAreaByDateChart'

import Table from '../../../shared/components/Table'
import { convertFieldName } from '../../../shared/utilities/fieldName'
import formatMoney from '../../../shared/utilities/formatMoney'
import { BillingColumn } from '../../../sm-api'

// Virtual scrolling constants
const ROW_HEIGHT = 40 // Approximate height of each table row in pixels
const VISIBLE_ROWS = 50 // Number of rows to render at once
const BUFFER_ROWS = 10 // Extra rows to render above/below for smooth scrolling

type ViewMode = 'summary' | 'breakdown'

export interface ExportData {
    headerFields: Array<{ category: string; title: string }>
    summaryData: IStackedAreaByDateChartData[]
    breakdownRows: Array<{
        date: Date
        projectOrTopic: string
        values: { [key: string]: number }
    }>
    viewMode: ViewMode
    expandCompute: boolean
    groupBy?: BillingColumn
}

interface IBillingCostByTimeTableProps {
    heading: string
    start: string
    end: string
    groups: string[]
    isLoading: boolean
    data: IStackedAreaByDateChartData[]
    visibleColumns: Set<string>
    setVisibleColumns: (columns: Set<string>) => void
    expandCompute?: boolean
    setExpandCompute?: (expand: boolean) => void
    exportToFile: (format: 'csv' | 'tsv', exportData?: ExportData) => void
    groupBy?: BillingColumn
    selectedProjects?: string[]
    breakdownData?: { [date: string]: { [field: string]: { [category: string]: number } } }
    openRows?: string[]
    handleToggle?: (date: string) => void
    onViewModeChange?: (viewMode: 'summary' | 'breakdown') => void
    onExportRequest?: (
        viewMode: 'summary' | 'breakdown',
        format: 'csv' | 'tsv',
        exportData?: ExportData
    ) => void
    currentViewMode?: 'summary' | 'breakdown'
}

const BillingCostByTimeTable: React.FC<IBillingCostByTimeTableProps> = ({
    heading,
    start,
    end,
    groups,
    isLoading,
    data,
    visibleColumns,
    setVisibleColumns,
    expandCompute: externalExpandCompute,
    setExpandCompute: externalSetExpandCompute,
    exportToFile,
    groupBy,
    breakdownData,
    openRows: _openRows = [],
    handleToggle: _handleToggle,
    onViewModeChange,
    onExportRequest,
    currentViewMode: externalCurrentViewMode,
}) => {
    const renderStartTime = performance.now()
    console.log(`[${new Date().toISOString()}] TABLE RENDER START`)
    const [internalData, setInternalData] = React.useState<IStackedAreaByDateChartData[]>([])
    const [internalGroups, setInternalGroups] = React.useState<string[]>([])
    const [viewMode, setViewMode] = React.useState<ViewMode>(externalCurrentViewMode || 'summary')

    // Virtual scrolling state
    const [scrollTop, setScrollTop] = React.useState(0)
    const tableContainerRef = React.useRef<HTMLDivElement>(null)

    // Handle scroll events for virtual scrolling
    const handleScroll = React.useCallback((e: React.UIEvent<HTMLDivElement>) => {
        const scrollTop = e.currentTarget.scrollTop
        setScrollTop(scrollTop)
    }, [])

    // Sync internal viewMode with external currentViewMode
    React.useEffect(() => {
        const startTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] VIEW MODE SYNC START - external: ${externalCurrentViewMode}, internal: ${viewMode}`
        )

        if (externalCurrentViewMode && externalCurrentViewMode !== viewMode) {
            setViewMode(externalCurrentViewMode)
        }

        const endTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] VIEW MODE SYNC END: ${(endTime - startTime).toFixed(2)}ms`
        )
    }, [externalCurrentViewMode, viewMode])

    // No longer need expand toggling state since updates are immediate

    // Use external expand state if provided, otherwise use internal state
    const [internalExpandCompute, setInternalExpandCompute] = React.useState<boolean>(false)
    const expandCompute = externalExpandCompute ?? internalExpandCompute
    const setExpandCompute = externalSetExpandCompute ?? setInternalExpandCompute

    // Properties
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: null,
        direction: null,
    })

    // Memoize column configurations for performance
    const columnConfigs = React.useMemo((): ColumnConfig[] => {
        const startTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] COLUMN CONFIGS START - expand: ${expandCompute}, groups: ${groups.length}`
        )
        const configs: ColumnConfig[] = [
            { id: 'Daily Total', label: 'Daily Total', group: 'summary' },
            { id: 'Cloud Storage', label: 'Cloud Storage', group: 'storage' },
        ]

        if (expandCompute) {
            // When expanded, add individual compute categories (sorted alphabetically)
            const computeGroups = groups
                .filter((group) => group !== 'Cloud Storage')
                .sort((a, b) => a.localeCompare(b))
            computeGroups.forEach((group) => {
                configs.push({ id: group, label: group, group: 'compute' })
            })
        } else {
            // When collapsed, add summary compute cost
            configs.push({ id: 'Compute Cost', label: 'Compute Cost', group: 'compute' })
        }

        const endTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] COLUMN CONFIGS END: ${(endTime - startTime).toFixed(2)}ms`
        )

        return configs
    }, [expandCompute, groups])

    // Memoize column groups for performance
    const columnGroups = React.useMemo((): ColumnGroup[] => {
        const startTime = performance.now()
        console.log(`[${new Date().toISOString()}] COLUMN GROUPS START`)
        const groups: ColumnGroup[] = [
            { id: 'summary', label: 'Summary', columns: ['Daily Total'] },
            { id: 'storage', label: 'Storage Cost', columns: ['Cloud Storage'] },
        ]

        if (expandCompute) {
            const computeColumns = columnConfigs
                .filter((config) => config.group === 'compute')
                .map((config) => config.id)
                .sort((a, b) => a.localeCompare(b))
            if (computeColumns.length > 0) {
                groups.push({ id: 'compute', label: 'Compute Categories', columns: computeColumns })
            }
        } else {
            // Add compute cost to its own group instead of summary
            groups.push({ id: 'compute', label: 'Compute Cost', columns: ['Compute Cost'] })
        }

        const endTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] COLUMN GROUPS END: ${(endTime - startTime).toFixed(2)}ms`
        )

        return groups
    }, [expandCompute, columnConfigs])

    // Create CSS classes for column visibility based on expand state
    const getColumnDisplayStyle = React.useCallback(
        (group: string) => {
            // Always show core columns
            if (['Daily Total', 'Cloud Storage'].includes(group)) {
                return { display: 'table-cell' }
            }

            if (expandCompute) {
                // In expanded mode: show individual compute categories, hide 'Compute Cost' summary
                return { display: group !== 'Compute Cost' ? 'table-cell' : 'none' }
            } else {
                // In collapsed mode: show 'Compute Cost' summary, hide individual compute categories
                if (group === 'Compute Cost') {
                    return { display: 'table-cell' }
                }
                // Hide individual compute categories
                const isComputeCategory = ![
                    'Daily Total',
                    'Cloud Storage',
                    'Compute Cost',
                ].includes(group)
                return { display: isComputeCategory ? 'none' : 'table-cell' }
            }
        },
        [expandCompute]
    )

    console.log(
        `[${new Date().toISOString()}] ZERO FUNCTION CALLS - Using CSS display for ${internalGroups.length} columns`
    )

    // Properly ordered header fields to match table header structure
    const headerFields = React.useMemo(() => {
        const startTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] HEADER FIELDS START - expand: ${expandCompute}, groups: ${internalGroups.length}`
        )

        // Order columns correctly: Daily Total, Cloud Storage, then compute columns
        const orderedColumns = []

        // Always add Daily Total first (under Expand header)
        orderedColumns.push('Daily Total')

        // Always add Cloud Storage second (under Storage Cost header)
        orderedColumns.push('Cloud Storage')

        // Add compute columns in order
        if (expandCompute) {
            // In expanded mode: show individual compute categories (excluding summary columns)
            const computeCategories = internalGroups
                .filter(
                    (group) => !['Daily Total', 'Cloud Storage', 'Compute Cost'].includes(group)
                )
                .sort()
            orderedColumns.push(...computeCategories)
        } else {
            // In collapsed mode: show Compute Cost summary
            orderedColumns.push('Compute Cost')
        }

        const result = orderedColumns.map((group) => ({
            category: group,
            title: group,
        }))

        const endTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] HEADER FIELDS END: ${(endTime - startTime).toFixed(2)}ms`
        )

        return result
    }, [expandCompute, internalGroups])

    // Memoize breakdown rows processing for performance
    const allBreakdownRows = React.useMemo(() => {
        const startTime = performance.now()
        console.log(`[${new Date().toISOString()}] BREAKDOWN ROWS START - mode: ${viewMode}`)

        if (!breakdownData || viewMode !== 'breakdown') {
            console.log(`[${new Date().toISOString()}] BREAKDOWN ROWS SKIPPED`)
            return []
        }

        const rows: Array<{
            date: Date
            projectOrTopic: string
            values: { [key: string]: number }
        }> = []

        Object.entries(breakdownData).forEach(([dateStr, fieldData]) => {
            const date = new Date(dateStr)
            Object.entries(fieldData).forEach(([projectOrTopic, categories]) => {
                rows.push({
                    date,
                    projectOrTopic,
                    values: categories,
                })
            })
        })

        // Sort breakdown rows by date then by project/topic
        rows.sort((a, b) => {
            const dateCompare = a.date.getTime() - b.date.getTime()
            if (dateCompare !== 0) return dateCompare
            return a.projectOrTopic.localeCompare(b.projectOrTopic)
        })

        const endTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] BREAKDOWN ROWS END: ${(endTime - startTime).toFixed(2)}ms - rows: ${rows.length}`
        )

        return rows
    }, [breakdownData, viewMode])

    // Memoize sorted summary data for performance - don't recalculate on expand toggle
    const sortedSummaryData = React.useMemo(() => {
        const startTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] SORTED SUMMARY START - mode: ${viewMode}, data: ${internalData.length}`
        )

        if (viewMode !== 'summary') {
            console.log(`[${new Date().toISOString()}] SORTED SUMMARY SKIPPED`)
            return []
        }

        const result = [...internalData].sort((a, b) => {
            if (!sort.column) return 0
            const props = [sort.column]
            const orders = sort.direction === 'ascending' ? ['asc'] : ['desc']

            return props.reduce((acc, prop, i) => {
                if (acc === 0) {
                    const [p1, p2] =
                        orders && orders[i] === 'desc'
                            ? [b.values[prop as keyof typeof b], a.values[prop as keyof typeof a]]
                            : [a.values[prop as keyof typeof a], b.values[prop as keyof typeof b]]
                    acc = p1 > p2 ? 1 : p1 < p2 ? -1 : 0
                }
                return acc
            }, 0) as number
        })

        const endTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] SORTED SUMMARY END: ${(endTime - startTime).toFixed(2)}ms`
        )

        return result
    }, [viewMode, internalData, sort.column, sort.direction])

    // Memoized breakdown row component for better performance
    const BreakdownRow = React.memo<{
        row: { date: Date; projectOrTopic: string; values: { [key: string]: number } }
        index: number
        headerFields: Array<{ category: string; title: string }>
    }>(({ row, index }) => {
        const rowRenderStart = performance.now()
        if (index === 0) {
            console.log(`[${new Date().toISOString()}] FIRST BREAKDOWN ROW RENDER START`)
        }

        const result = (
            <SUITable.Row>
                <SUITable.Cell collapsing>
                    <b>{row.date.toLocaleDateString()}</b>
                </SUITable.Cell>
                <SUITable.Cell collapsing>
                    <b>{row.projectOrTopic}</b>
                </SUITable.Cell>
                {headerFields.map((field) => (
                    <SUITable.Cell
                        key={`${index}-${field.category}`}
                        style={getColumnDisplayStyle(field.category)}
                    >
                        {formatMoney(row.values[field.category] || 0)}
                    </SUITable.Cell>
                ))}
            </SUITable.Row>
        )

        if (index === 0) {
            const rowRenderEnd = performance.now()
            console.log(
                `[${new Date().toISOString()}] FIRST BREAKDOWN ROW RENDER END: ${(rowRenderEnd - rowRenderStart).toFixed(2)}ms`
            )
        }

        return result
    })
    BreakdownRow.displayName = 'BreakdownRow'

    // Calculate which rows to render for virtual scrolling
    const virtualizedBreakdownRows = React.useMemo(() => {
        if (viewMode !== 'breakdown' || allBreakdownRows.length === 0) {
            return { visibleRows: [], startIndex: 0, endIndex: 0, totalHeight: 0 }
        }

        const startIndex = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - BUFFER_ROWS)
        const endIndex = Math.min(
            allBreakdownRows.length,
            startIndex + VISIBLE_ROWS + BUFFER_ROWS * 2
        )
        const visibleRows = allBreakdownRows.slice(startIndex, endIndex)
        const totalHeight = allBreakdownRows.length * ROW_HEIGHT

        console.log(
            `[${new Date().toISOString()}] VIRTUALIZED BREAKDOWN - Rendering ${visibleRows.length} rows (${startIndex}-${endIndex}) of ${allBreakdownRows.length} total`
        )

        return { visibleRows, startIndex, endIndex, totalHeight }
    }, [viewMode, allBreakdownRows, scrollTop])

    // Split data rendering into separate memoized components for better performance
    const breakdownTableBody = React.useMemo(() => {
        const startTime = performance.now()
        console.log(`[${new Date().toISOString()}] BREAKDOWN TABLE BODY START - Virtual rendering`)

        if (viewMode !== 'breakdown' || virtualizedBreakdownRows.visibleRows.length === 0) {
            console.log(`[${new Date().toISOString()}] BREAKDOWN TABLE BODY SKIPPED`)
            return null
        }

        const { visibleRows, startIndex } = virtualizedBreakdownRows

        const result = (
            <>
                {/* Spacer for rows above viewport */}
                {startIndex > 0 && (
                    <tr style={{ height: startIndex * ROW_HEIGHT }}>
                        <td colSpan={internalGroups.length + 2}></td>
                    </tr>
                )}

                {/* Render visible rows */}
                {visibleRows.map((row, index) => (
                    <BreakdownRow
                        key={`${row.date.toISOString()}-${row.projectOrTopic}`}
                        row={row}
                        index={startIndex + index}
                        headerFields={headerFields}
                    />
                ))}

                {/* Spacer for rows below viewport */}
                {startIndex + visibleRows.length < allBreakdownRows.length && (
                    <tr
                        style={{
                            height:
                                (allBreakdownRows.length - startIndex - visibleRows.length) *
                                ROW_HEIGHT,
                        }}
                    >
                        <td colSpan={internalGroups.length + 2}></td>
                    </tr>
                )}
            </>
        )

        const endTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] BREAKDOWN TABLE BODY END: ${(endTime - startTime).toFixed(2)}ms`
        )

        return result
    }, [
        viewMode,
        virtualizedBreakdownRows,
        headerFields,
        BreakdownRow,
        internalGroups.length,
        allBreakdownRows.length,
    ])

    const summaryTableBody = React.useMemo(() => {
        const startTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] SUMMARY TABLE BODY START - ${sortedSummaryData.length} rows`
        )

        if (viewMode !== 'summary') {
            console.log(`[${new Date().toISOString()}] SUMMARY TABLE BODY SKIPPED`)
            return null
        }

        const result = (
            <>
                {sortedSummaryData.map((p) => (
                    <SUITable.Row key={p.date.toISOString()}>
                        <SUITable.Cell collapsing key={`Date - ${p.date.toISOString()}`}>
                            <b>{p.date.toLocaleDateString()}</b>
                        </SUITable.Cell>
                        {headerFields.map((field) => (
                            <SUITable.Cell
                                key={`${p.date.toISOString()} - ${field.category}`}
                                style={getColumnDisplayStyle(field.category)}
                            >
                                {formatMoney(p.values[field.category] || 0)}
                            </SUITable.Cell>
                        ))}
                    </SUITable.Row>
                ))}
            </>
        )

        const endTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] SUMMARY TABLE BODY END: ${(endTime - startTime).toFixed(2)}ms`
        )

        return result
    }, [viewMode, sortedSummaryData, headerFields])

    // Format data
    React.useEffect(() => {
        const startTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] FORMAT DATA START - data: ${data.length}, groups: ${groups.length}, triggered by data/groups change`
        )
        setInternalData(
            data.map((p) => {
                const newP = { ...p }
                const total = Object.values(p.values).reduce((acc, cur) => acc + cur, 0)
                newP.values['Daily Total'] = total
                newP.values['Compute Cost'] = total - p.values['Cloud Storage']
                return newP
            })
        )

        setInternalGroups(groups.concat(['Daily Total', 'Compute Cost']))

        const endTime = performance.now()
        console.log(
            `[${new Date().toISOString()}] FORMAT DATA END: ${(endTime - startTime).toFixed(2)}ms`
        )
    }, [data, groups])

    // DISABLED: Problematic visibility effect that was causing delayed re-renders
    // Column visibility is now handled entirely by parent component
    console.log(
        `[${new Date().toISOString()}] VISIBILITY MANAGEMENT DISABLED - columns handled by parent`
    )

    // Early return for loading - must be after all hooks
    if (isLoading) {
        return (
            <div>
                <p style={{ textAlign: 'center', marginTop: '20px' }}>
                    <em>Loading table...</em>
                </p>
            </div>
        )
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

    const renderEndTime = performance.now()
    console.log(
        `[${new Date().toISOString()}] TABLE RENDER END: ${(renderEndTime - renderStartTime).toFixed(2)}ms`
    )

    return (
        <>
            <div
                style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'flex-start',
                    marginBottom: '15px',
                    flexWrap: 'wrap',
                    gap: '10px',
                }}
            >
                <Header as="h3" style={{ margin: 0, flex: '1 1 200px' }}>
                    {convertFieldName(heading)} costs from {start} to {end}
                </Header>
                <div
                    style={{
                        display: 'flex',
                        gap: '10px',
                        flex: '0 0 auto',
                        minWidth: '340px',
                    }}
                >
                    <ToggleButtonGroup
                        value={viewMode}
                        exclusive
                        color="primary"
                        onChange={(event, newMode) => {
                            if (newMode !== null) {
                                setViewMode(newMode)
                                onViewModeChange?.(newMode)
                            }
                        }}
                        aria-label="view mode toggle"
                        style={{ height: '36px' }}
                        disabled={false}
                    >
                        <ToggleButton value="summary" aria-label="summary view">
                            Summary
                        </ToggleButton>
                        <ToggleButton value="breakdown" aria-label="breakdown view">
                            Breakdown
                        </ToggleButton>
                    </ToggleButtonGroup>
                    <ColumnVisibilityDropdown
                        columns={columnConfigs}
                        groups={columnGroups}
                        visibleColumns={visibleColumns}
                        onVisibilityChange={setVisibleColumns}
                        searchThreshold={8}
                        searchPlaceholder="Search columns..."
                        enableUrlPersistence={false}
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
                                onClick={() => {
                                    const exportData: ExportData = {
                                        headerFields,
                                        summaryData: sortedSummaryData,
                                        breakdownRows: allBreakdownRows,
                                        viewMode,
                                        expandCompute,
                                        groupBy,
                                    }
                                    if (onExportRequest) {
                                        onExportRequest(viewMode, 'csv', exportData)
                                    } else {
                                        exportToFile('csv', exportData)
                                    }
                                }}
                            />
                            <Dropdown.Item
                                key="tsv"
                                text="Export to TSV"
                                icon="file text outline"
                                onClick={() => {
                                    const exportData: ExportData = {
                                        headerFields,
                                        summaryData: sortedSummaryData,
                                        breakdownRows: allBreakdownRows,
                                        viewMode,
                                        expandCompute,
                                        groupBy,
                                    }
                                    if (onExportRequest) {
                                        onExportRequest(viewMode, 'tsv', exportData)
                                    } else {
                                        exportToFile('tsv', exportData)
                                    }
                                }}
                            />
                        </Dropdown.Menu>
                    </Dropdown>
                </div>
            </div>
            <div
                ref={tableContainerRef}
                onScroll={viewMode === 'breakdown' ? handleScroll : undefined}
                style={{
                    maxHeight: viewMode === 'breakdown' ? '600px' : 'none',
                    overflowY: viewMode === 'breakdown' ? 'auto' : 'visible',
                }}
            >
                <Table celled compact sortable selectable style={{ width: '100%' }}>
                    <SUITable.Header
                        style={{ position: 'sticky', top: 0, zIndex: 10, backgroundColor: 'white' }}
                    >
                        <SUITable.Row>
                            <SUITable.HeaderCell
                                colSpan={viewMode === 'breakdown' ? 3 : 2}
                                textAlign="center"
                            >
                                <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                    <Switch
                                        checked={expandCompute}
                                        onChange={(e) => {
                                            console.log(
                                                `[${new Date().toISOString()}] MUI SWITCH TOGGLE: ${e.target.checked}`
                                            )
                                            setExpandCompute(e.target.checked)
                                        }}
                                        size="small"
                                        color="primary"
                                    />
                                    <span>Expand</span>
                                </span>
                            </SUITable.HeaderCell>
                            <SUITable.HeaderCell
                                colSpan={1}
                                style={getColumnDisplayStyle('Cloud Storage')}
                            >
                                Storage Cost
                            </SUITable.HeaderCell>
                            <SUITable.HeaderCell
                                colSpan={headerFields.length - 2}
                                style={{
                                    display: headerFields.some(
                                        (f) =>
                                            f.category !== 'Daily Total' &&
                                            f.category !== 'Cloud Storage'
                                    )
                                        ? 'table-cell'
                                        : 'none',
                                }}
                            >
                                Compute Cost
                            </SUITable.HeaderCell>
                        </SUITable.Row>
                        <SUITable.Row>
                            <SUITable.HeaderCell
                                style={{
                                    borderBottom: 'none',
                                    minWidth: '140px',
                                }}
                            >
                                Date
                            </SUITable.HeaderCell>
                            {viewMode === 'breakdown' && (
                                <SUITable.HeaderCell
                                    style={{
                                        borderBottom: 'none',
                                        minWidth: '250px',
                                    }}
                                >
                                    {groupBy === BillingColumn.GcpProject ? 'Project' : 'Topic'}
                                </SUITable.HeaderCell>
                            )}
                            {headerFields.map((field) => (
                                <SUITable.HeaderCell
                                    key={field.category}
                                    sorted={checkDirection(field.category)}
                                    onClick={() => handleSort(field.category)}
                                    style={{
                                        borderBottom: 'none',
                                        position: 'sticky',
                                        minWidth: '180px',
                                        ...getColumnDisplayStyle(field.category),
                                    }}
                                >
                                    {convertFieldName(field.title)}
                                </SUITable.HeaderCell>
                            ))}
                        </SUITable.Row>
                    </SUITable.Header>
                    <SUITable.Body>
                        <div style={{ display: viewMode === 'breakdown' ? 'contents' : 'none' }}>
                            {breakdownTableBody}
                        </div>
                        <div style={{ display: viewMode === 'summary' ? 'contents' : 'none' }}>
                            {summaryTableBody}
                        </div>
                    </SUITable.Body>
                </Table>
            </div>
            {/* Always visible totals footer */}
            <div
                style={{
                    backgroundColor: 'white',
                    borderTop: '1px solid rgba(34,36,38,.15)',
                    position: 'sticky',
                    bottom: 0,
                    zIndex: 5,
                }}
            >
                <Table celled compact style={{ width: '100%', margin: 0 }}>
                    <SUITable.Body>
                        <SUITable.Row
                            style={{ display: viewMode === 'summary' ? 'table-row' : 'none' }}
                        >
                            <SUITable.Cell collapsing style={{ minWidth: '140px' }}>
                                <b>All Time Total</b>
                            </SUITable.Cell>
                            {headerFields.map((field) => {
                                const total = internalData.reduce(
                                    (acc, cur) => acc + (cur.values[field.category] || 0),
                                    0
                                )
                                return (
                                    <SUITable.Cell
                                        key={`Total ${field.category}`}
                                        style={{
                                            minWidth: '180px',
                                            ...getColumnDisplayStyle(field.category),
                                        }}
                                    >
                                        <b>{formatMoney(total)}</b>
                                    </SUITable.Cell>
                                )
                            })}
                        </SUITable.Row>
                        <SUITable.Row
                            style={{
                                display:
                                    viewMode === 'breakdown' && breakdownData
                                        ? 'table-row'
                                        : 'none',
                            }}
                        >
                            <SUITable.Cell collapsing style={{ minWidth: '140px' }}>
                                <b>All Time Total</b>
                            </SUITable.Cell>
                            <SUITable.Cell collapsing style={{ minWidth: '250px' }}>
                                <b>
                                    All{' '}
                                    {groupBy === BillingColumn.GcpProject ? 'Projects' : 'Topics'}
                                </b>
                            </SUITable.Cell>
                            {headerFields.map((field) => {
                                const total = breakdownData
                                    ? Object.values(breakdownData).reduce((dateSum, fieldData) => {
                                          return (
                                              dateSum +
                                              Object.values(fieldData).reduce(
                                                  (fieldSum, categories) => {
                                                      return (
                                                          fieldSum +
                                                          (categories[field.category] || 0)
                                                      )
                                                  },
                                                  0
                                              )
                                          )
                                      }, 0)
                                    : 0
                                return (
                                    <SUITable.Cell
                                        key={`Total ${field.category}`}
                                        style={{
                                            minWidth: '180px',
                                            ...getColumnDisplayStyle(field.category),
                                        }}
                                    >
                                        <b>{formatMoney(total)}</b>
                                    </SUITable.Cell>
                                )
                            })}
                        </SUITable.Row>
                    </SUITable.Body>
                </Table>
            </div>
        </>
    )
}

export default React.memo(BillingCostByTimeTable)
