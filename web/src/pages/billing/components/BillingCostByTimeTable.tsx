import { Switch, ToggleButton, ToggleButtonGroup } from '@mui/material'
import Paper from '@mui/material/Paper'
import TableBody from '@mui/material/TableBody'
import TableContainer from '@mui/material/TableContainer'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import React from 'react'
import { TableComponents, TableVirtuoso } from 'react-virtuoso'
import { Dropdown, Header, Table as SUITable, TableBodyProps, TableProps } from 'semantic-ui-react'
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

// Type for breakdown row data in virtuoso
type BreakdownRowData = {
    date: Date | null // null for total row
    projectOrTopic: string
    values: { [key: string]: number }
    headerFields: Array<{ category: string; title: string }>
    getColumnDisplayStyle: (category: string) => React.CSSProperties
    isTotal?: boolean
}

// Type for summary row data in virtuoso
type SummaryRowData = {
    date: Date
    values: { [key: string]: number }
    headerFields: Array<{ category: string; title: string }>
    getColumnDisplayStyle: (category: string) => React.CSSProperties
}

// Breakdown row component for react-virtuoso
const BreakdownTableRow: React.FC<{ item: BreakdownRowData }> = ({ item: row, ...props }) => {
    return (
        <TableRow
            {...props}
            className="ui table row"
            style={row.isTotal ? { backgroundColor: '#f5f5f5', fontWeight: 'bold' } : {}}
        >
            <SUITable.Cell collapsing>
                <b>{row.date ? row.date.toLocaleDateString() : 'All Time Total'}</b>
            </SUITable.Cell>
            <SUITable.Cell collapsing>
                <b>{row.projectOrTopic}</b>
            </SUITable.Cell>
            {row.headerFields.map((field) => (
                <SUITable.Cell
                    key={`${row.date ? row.date.toISOString() : 'total'}-${field.category}`}
                    style={row.getColumnDisplayStyle(field.category)}
                >
                    <span style={row.isTotal ? { fontWeight: 'bold' } : {}}>
                        {formatMoney(row.values[field.category] || 0)}
                    </span>
                </SUITable.Cell>
            ))}
        </TableRow>
    )
}

// Summary row component for react-virtuoso
const SummaryTableRow: React.FC<{ item: SummaryRowData }> = ({ item: row, ...props }) => {
    return (
        <TableRow {...props} className="ui table row">
            <SUITable.Cell collapsing>
                <b>{row.date.toLocaleDateString()}</b>
            </SUITable.Cell>
            {row.headerFields.map((field) => (
                <SUITable.Cell
                    key={`${row.date.toISOString()}-${field.category}`}
                    style={row.getColumnDisplayStyle(field.category)}
                >
                    {formatMoney(row.values[field.category] || 0)}
                </SUITable.Cell>
            ))}
        </TableRow>
    )
}

// Table components for react-virtuoso breakdown view
const VirtuosoBreakdownTableComponents: TableComponents<BreakdownRowData> = {
    Scroller: React.forwardRef(function TableContainerWrapper(props, ref) {
        return (
            <TableContainer
                component={Paper}
                // @ts-ignore
                ref={ref}
                {...props}
            />
        )
    }),
    Table: (props: TableProps) => (
        <Table
            {...props}
            celled
            compact
            selectable
            style={{ borderCollapse: 'separate', width: '100%' }}
        />
    ),
    TableHead: TableHead,
    // @ts-ignore
    TableRow: BreakdownTableRow,
    // @ts-ignore
    TableBody: React.forwardRef(function TableBodyWrapper(props: TableBodyProps, ref) {
        return <TableBody ref={ref} {...props} />
    }),
}

// Table components for react-virtuoso summary view
const VirtuosoSummaryTableComponents: TableComponents<SummaryRowData> = {
    Scroller: React.forwardRef(function TableContainerWrapper(props, ref) {
        return (
            <TableContainer
                component={Paper}
                // @ts-ignore
                ref={ref}
                {...props}
            />
        )
    }),
    Table: (props: TableProps) => (
        <Table
            {...props}
            celled
            compact
            sortable
            selectable
            style={{ borderCollapse: 'separate', width: '100%' }}
        />
    ),
    TableHead: TableHead,
    // @ts-ignore
    TableRow: SummaryTableRow,
    // @ts-ignore
    TableBody: React.forwardRef(function TableBodyWrapper(props: TableBodyProps, ref) {
        return <TableBody ref={ref} {...props} />
    }),
}

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
    onViewModeChange,
    onExportRequest,
    currentViewMode: externalCurrentViewMode,
}) => {
    const [internalData, setInternalData] = React.useState<IStackedAreaByDateChartData[]>([])
    const [internalGroups, setInternalGroups] = React.useState<string[]>([])
    const [viewMode, setViewMode] = React.useState<ViewMode>(externalCurrentViewMode || 'summary')

    // Sync internal viewMode with external currentViewMode
    React.useEffect(() => {
        if (externalCurrentViewMode && externalCurrentViewMode !== viewMode) {
            setViewMode(externalCurrentViewMode)
        }
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

        return configs
    }, [expandCompute, groups])

    // Memoize column groups for performance
    const columnGroups = React.useMemo((): ColumnGroup[] => {
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

    // Properly ordered header fields to match table header structure
    const headerFields = React.useMemo(() => {
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

        return result
    }, [expandCompute, internalGroups])

    // Memoize breakdown rows processing for performance
    const allBreakdownRows = React.useMemo(() => {
        if (!breakdownData || viewMode !== 'breakdown') {
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

        return rows
    }, [breakdownData, viewMode])

    // Memoize sorted summary data for performance - don't recalculate on expand toggle
    const sortedSummaryData = React.useMemo(() => {
        if (viewMode !== 'summary') {
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

        return result
    }, [viewMode, internalData, sort.column, sort.direction])

    // Prepare data for react-virtuoso breakdown table
    const virtuosoBreakdownData = React.useMemo(() => {
        if (viewMode !== 'breakdown') return []

        // Add regular breakdown rows only
        return allBreakdownRows.map((row) => ({
            ...row,
            headerFields,
            getColumnDisplayStyle,
            isTotal: false,
        }))
    }, [viewMode, allBreakdownRows, headerFields, getColumnDisplayStyle])

    // Prepare data for react-virtuoso summary table
    const virtuosoSummaryData = React.useMemo(() => {
        if (viewMode !== 'summary') return []

        return sortedSummaryData.map((row) => ({
            ...row,
            headerFields,
            getColumnDisplayStyle,
        }))
    }, [viewMode, sortedSummaryData, headerFields, getColumnDisplayStyle])

    // Format data
    React.useEffect(() => {
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
    }, [data, groups])

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

    const handleExport = React.useCallback(
        (format: 'csv' | 'tsv') => {
            const exportData: ExportData = {
                headerFields,
                summaryData: sortedSummaryData,
                breakdownRows: allBreakdownRows,
                viewMode,
                expandCompute,
                groupBy,
            }
            if (onExportRequest) {
                onExportRequest(viewMode, format, exportData)
            } else {
                exportToFile(format, exportData)
            }
        },
        [
            headerFields,
            sortedSummaryData,
            allBreakdownRows,
            viewMode,
            expandCompute,
            groupBy,
            onExportRequest,
            exportToFile,
        ]
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
                                onClick={() => handleExport('csv')}
                            />
                            <Dropdown.Item
                                key="tsv"
                                text="Export to TSV"
                                icon="file text outline"
                                onClick={() => handleExport('tsv')}
                            />
                        </Dropdown.Menu>
                    </Dropdown>
                </div>
            </div>
            {viewMode === 'breakdown' ? (
                <TableVirtuoso
                    style={{
                        height: Math.min(
                            600,
                            Math.max(200, virtuosoBreakdownData.length * 45 + 100)
                        ),
                        backgroundColor: 'white',
                    }}
                    data={virtuosoBreakdownData}
                    components={VirtuosoBreakdownTableComponents}
                    fixedHeaderContent={() => (
                        <>
                            <TableRow className="ui table row">
                                <SUITable.HeaderCell colSpan={3} textAlign="center">
                                    <span
                                        style={{
                                            display: 'flex',
                                            alignItems: 'center',
                                            gap: '8px',
                                        }}
                                    >
                                        <Switch
                                            checked={expandCompute}
                                            onChange={(e) => {
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
                            </TableRow>
                            <TableRow className="ui table row">
                                <SUITable.HeaderCell
                                    style={{
                                        borderBottom: 'none',
                                        minWidth: '140px',
                                    }}
                                >
                                    Date
                                </SUITable.HeaderCell>
                                <SUITable.HeaderCell
                                    style={{
                                        borderBottom: 'none',
                                        minWidth: '250px',
                                    }}
                                >
                                    {groupBy === BillingColumn.GcpProject ? 'Project' : 'Topic'}
                                </SUITable.HeaderCell>
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
                            </TableRow>
                        </>
                    )}
                    fixedFooterContent={() => (
                        <TableRow
                            className="ui table row"
                            style={{ backgroundColor: '#f5f5f5', fontWeight: 'bold' }}
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
                        </TableRow>
                    )}
                />
            ) : (
                <TableVirtuoso
                    style={{
                        height: Math.min(600, Math.max(200, virtuosoSummaryData.length * 45 + 100)),
                        backgroundColor: 'white',
                    }}
                    data={virtuosoSummaryData}
                    components={VirtuosoSummaryTableComponents}
                    fixedHeaderContent={() => (
                        <>
                            <TableRow className="ui table row">
                                <SUITable.HeaderCell colSpan={2} textAlign="center">
                                    <span
                                        style={{
                                            display: 'flex',
                                            alignItems: 'center',
                                            gap: '8px',
                                        }}
                                    >
                                        <Switch
                                            checked={expandCompute}
                                            onChange={(e) => {
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
                            </TableRow>
                            <TableRow className="ui table row">
                                <SUITable.HeaderCell
                                    style={{
                                        borderBottom: 'none',
                                        minWidth: '140px',
                                    }}
                                >
                                    Date
                                </SUITable.HeaderCell>
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
                            </TableRow>
                        </>
                    )}
                    fixedFooterContent={() => (
                        <TableRow
                            className="ui table row"
                            style={{ backgroundColor: '#f5f5f5', fontWeight: 'bold' }}
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
                        </TableRow>
                    )}
                />
            )}
        </>
    )
}

export default React.memo(BillingCostByTimeTable)
