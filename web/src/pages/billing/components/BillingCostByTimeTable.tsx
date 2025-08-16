import { Pagination, ToggleButton, ToggleButtonGroup } from '@mui/material'
import React from 'react'
import { Checkbox, Dropdown, Header, Table as SUITable } from 'semantic-ui-react'
import {
    ColumnConfig,
    ColumnGroup,
    ColumnVisibilityDropdown,
    useColumnVisibility,
} from '../../../shared/components/ColumnVisibilityDropdown'
import { IStackedAreaByDateChartData } from '../../../shared/components/Graphs/StackedAreaByDateChart'
import LoadingDucks from '../../../shared/components/LoadingDucks/LoadingDucks'
import Table from '../../../shared/components/Table'
import { convertFieldName } from '../../../shared/utilities/fieldName'
import formatMoney from '../../../shared/utilities/formatMoney'
import { BillingColumn } from '../../../sm-api'

type ViewMode = 'summary' | 'breakdown'

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
    exportToFile: (format: 'csv' | 'tsv') => void
    groupBy?: BillingColumn
    selectedProjects?: string[]
    breakdownData?: { [date: string]: { [field: string]: { [category: string]: number } } }
    openRows?: string[]
    handleToggle?: (date: string) => void
    onViewModeChange?: (viewMode: 'summary' | 'breakdown') => void
    onExportRequest?: (viewMode: 'summary' | 'breakdown', format: 'csv' | 'tsv') => void
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
}) => {
    const [internalData, setInternalData] = React.useState<IStackedAreaByDateChartData[]>([])
    const [internalGroups, setInternalGroups] = React.useState<string[]>([])
    const [viewMode, setViewMode] = React.useState<ViewMode>('summary')
    const [currentPage, setCurrentPage] = React.useState<number>(1)
    const [availableDates, setAvailableDates] = React.useState<string[]>([])

    // Use external expand state if provided, otherwise use internal state
    const [internalExpandCompute, setInternalExpandCompute] = React.useState<boolean>(false)
    const expandCompute = externalExpandCompute ?? internalExpandCompute
    const setExpandCompute = externalSetExpandCompute ?? setInternalExpandCompute

    // Properties
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: null,
        direction: null,
    })

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

    // Calculate total rows in breakdown view and determine if pagination is needed
    const getTotalBreakdownRows = React.useCallback(() => {
        if (!breakdownData) return 0
        return Object.values(breakdownData).reduce(
            (total, fieldData) => total + Object.keys(fieldData).length,
            0
        )
    }, [breakdownData])

    const shouldUsePagination = React.useMemo(() => {
        return viewMode === 'breakdown' && getTotalBreakdownRows() > 100
    }, [viewMode, getTotalBreakdownRows])

    // Update available dates for pagination when breakdown data changes
    React.useEffect(() => {
        if (breakdownData && viewMode === 'breakdown') {
            const dates = Object.keys(breakdownData).sort()
            setAvailableDates(dates)
            if (currentPage > dates.length) {
                setCurrentPage(1)
            }
        }
    }, [breakdownData, viewMode, currentPage])

    // Reset pagination when switching to breakdown view
    React.useEffect(() => {
        if (viewMode === 'breakdown') {
            setCurrentPage(1)
        }
    }, [viewMode])

    // Generate column configurations for the dropdown
    const getColumnConfigs = React.useCallback((): ColumnConfig[] => {
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

    // Generate column groups for the dropdown
    const getColumnGroups = React.useCallback((): ColumnGroup[] => {
        const groups: ColumnGroup[] = [
            { id: 'summary', label: 'Summary', columns: ['Daily Total'] },
            { id: 'storage', label: 'Storage Cost', columns: ['Cloud Storage'] },
        ]

        if (expandCompute) {
            const computeColumns = getColumnConfigs()
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
    }, [expandCompute, getColumnConfigs])

    // Use the column visibility hook
    const { isColumnVisible } = useColumnVisibility(getColumnConfigs(), visibleColumns)

    // Handle expand toggle changes - only modify columns when user explicitly toggles expand
    const [previousExpandState, setPreviousExpandState] = React.useState<boolean | null>(null)

    React.useEffect(() => {
        // Only respond to actual expand state changes, not initial load
        if (previousExpandState !== null && previousExpandState !== expandCompute) {
            const newVisibleColumns = new Set(visibleColumns)
            let hasChanges = false

            // Filter out 'Cloud Storage' from groups since it's always a storage cost, not compute cost
            const computeGroups = groups.filter((group) => group !== 'Cloud Storage')

            if (expandCompute) {
                // Switching to expanded mode - remove 'Compute Cost' summary and add individual compute groups
                if (newVisibleColumns.has('Compute Cost')) {
                    newVisibleColumns.delete('Compute Cost')
                    hasChanges = true
                }
                // Add all compute cost columns (excluding Cloud Storage)
                computeGroups.forEach((group) => {
                    if (!newVisibleColumns.has(group)) {
                        newVisibleColumns.add(group)
                        hasChanges = true
                    }
                })
            } else {
                // Switching to collapsed mode - remove individual compute groups and add 'Compute Cost' summary
                computeGroups.forEach((group) => {
                    if (newVisibleColumns.has(group)) {
                        newVisibleColumns.delete(group)
                        hasChanges = true
                    }
                })
                if (!newVisibleColumns.has('Compute Cost')) {
                    newVisibleColumns.add('Compute Cost')
                    hasChanges = true
                }
            }

            if (hasChanges) {
                setVisibleColumns(newVisibleColumns)
            }
        }

        setPreviousExpandState(expandCompute)
    }, [expandCompute, groups, visibleColumns, setVisibleColumns, previousExpandState])

    // Early return for loading - must be after all hooks
    if (isLoading) {
        return (
            <div>
                <LoadingDucks />
            </div>
        )
    }

    // Header sort
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

    const headerFields = () => {
        const baseFields = expandCompute
            ? internalGroups
                  .sort(headerSort)
                  .filter((group) => group != 'Compute Cost')
                  .map((group: string) => ({
                      category: group,
                      title: group,
                  }))
            : [
                  {
                      category: 'Daily Total',
                      title: 'Daily Total',
                  },
                  {
                      category: 'Cloud Storage',
                      title: 'Cloud Storage',
                  },
                  {
                      category: 'Compute Cost',
                      title: 'Compute Cost',
                  },
              ]

        // Filter by visible columns using our new hook
        return baseFields.filter((field) => isColumnVisible(field.category))
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

    const dataSort = (
        data: IStackedAreaByDateChartData[],
        props: string[],
        orders?: ('asc' | 'desc')[]
    ) =>
        [...data].sort(
            (a, b) =>
                props.reduce((acc, prop, i) => {
                    if (acc === 0) {
                        const [p1, p2] =
                            orders && orders[i] === 'desc'
                                ? [
                                      b.values[prop as keyof typeof b],
                                      a.values[prop as keyof typeof a],
                                  ]
                                : [
                                      a.values[prop as keyof typeof a],
                                      b.values[prop as keyof typeof b],
                                  ]
                        acc = p1 > p2 ? 1 : p1 < p2 ? -1 : 0
                    }
                    return acc
                }, 0) as number // explicitly cast the result to a number
        )

    const dataToBody = (_data: IStackedAreaByDateChartData[]) => {
        if (viewMode === 'breakdown') {
            if (!breakdownData) {
                return <></>
            }

            if (shouldUsePagination) {
                // Paginated breakdown view: Show projects/topics for the current page date only
                if (availableDates.length === 0) {
                    return <></>
                }

                const currentDateStr = availableDates[currentPage - 1]
                if (!currentDateStr || !breakdownData[currentDateStr]) {
                    return <></>
                }

                const currentDate = new Date(currentDateStr)
                const fieldData = breakdownData[currentDateStr]

                const breakdownRows: Array<{
                    date: Date
                    projectOrTopic: string
                    values: { [key: string]: number }
                }> = []

                Object.entries(fieldData).forEach(([projectOrTopic, categories]) => {
                    breakdownRows.push({
                        date: currentDate,
                        projectOrTopic,
                        values: categories,
                    })
                })

                breakdownRows.sort((a, b) => a.projectOrTopic.localeCompare(b.projectOrTopic))

                return (
                    <>
                        {breakdownRows.map((row, index) => (
                            <SUITable.Row key={`${row.date.toISOString()}-${row.projectOrTopic}`}>
                                <SUITable.Cell collapsing>
                                    <b>{row.date.toLocaleDateString()}</b>
                                </SUITable.Cell>
                                <SUITable.Cell collapsing>
                                    <b>{row.projectOrTopic}</b>
                                </SUITable.Cell>
                                {headerFields().map((k) => (
                                    <SUITable.Cell key={`${index}-${k.category}`}>
                                        {formatMoney(row.values[k.category] || 0)}
                                    </SUITable.Cell>
                                ))}
                            </SUITable.Row>
                        ))}
                    </>
                )
            } else {
                // Non-paginated breakdown view: Show all projects/topics for all dates
                const breakdownRows: Array<{
                    date: Date
                    projectOrTopic: string
                    values: { [key: string]: number }
                }> = []

                Object.entries(breakdownData).forEach(([dateStr, fieldData]) => {
                    const date = new Date(dateStr)
                    Object.entries(fieldData).forEach(([projectOrTopic, categories]) => {
                        breakdownRows.push({
                            date,
                            projectOrTopic,
                            values: categories,
                        })
                    })
                })

                // Sort breakdown rows by date then by project/topic
                breakdownRows.sort((a, b) => {
                    const dateCompare = a.date.getTime() - b.date.getTime()
                    if (dateCompare !== 0) return dateCompare
                    return a.projectOrTopic.localeCompare(b.projectOrTopic)
                })

                return (
                    <>
                        {breakdownRows.map((row, index) => (
                            <SUITable.Row key={`${row.date.toISOString()}-${row.projectOrTopic}`}>
                                <SUITable.Cell collapsing>
                                    <b>{row.date.toLocaleDateString()}</b>
                                </SUITable.Cell>
                                <SUITable.Cell collapsing>
                                    <b>{row.projectOrTopic}</b>
                                </SUITable.Cell>
                                {headerFields().map((k) => (
                                    <SUITable.Cell key={`${index}-${k.category}`}>
                                        {formatMoney(row.values[k.category] || 0)}
                                    </SUITable.Cell>
                                ))}
                            </SUITable.Row>
                        ))}
                    </>
                )
            }
        }

        return (
            <>
                {dataSort(
                    internalData,
                    sort.column ? [sort.column] : [],
                    sort.direction === 'ascending' ? ['asc'] : ['desc']
                ).map((p) => (
                    <SUITable.Row key={p.date.toISOString()}>
                        <SUITable.Cell collapsing key={`Date - ${p.date.toISOString()}`}>
                            <b>{p.date.toLocaleDateString()}</b>
                        </SUITable.Cell>
                        {headerFields().map((k) => (
                            <SUITable.Cell key={`${p.date.toISOString()} - ${k.category}`}>
                                {formatMoney(p.values[k.category])}
                            </SUITable.Cell>
                        ))}
                    </SUITable.Row>
                ))}
            </>
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
                        onChange={(event, newMode) => {
                            if (newMode !== null) {
                                setViewMode(newMode)
                                onViewModeChange?.(newMode)
                            }
                        }}
                        aria-label="view mode"
                        size="small"
                        color="primary"
                        style={{ height: '36px' }}
                    >
                        <ToggleButton value="summary" aria-label="summary view">
                            Summary
                        </ToggleButton>
                        <ToggleButton value="breakdown" aria-label="breakdown view">
                            Breakdown
                        </ToggleButton>
                    </ToggleButtonGroup>
                    <ColumnVisibilityDropdown
                        columns={getColumnConfigs()}
                        groups={getColumnGroups()}
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
                                    if (onExportRequest) {
                                        onExportRequest(viewMode, 'csv')
                                    } else {
                                        exportToFile('csv')
                                    }
                                }}
                            />
                            <Dropdown.Item
                                key="tsv"
                                text="Export to TSV"
                                icon="file text outline"
                                onClick={() => {
                                    if (onExportRequest) {
                                        onExportRequest(viewMode, 'tsv')
                                    } else {
                                        exportToFile('tsv')
                                    }
                                }}
                            />
                        </Dropdown.Menu>
                    </Dropdown>
                </div>
            </div>
            <Table celled compact sortable selectable>
                <SUITable.Header>
                    <SUITable.Row>
                        <SUITable.HeaderCell
                            colSpan={
                                viewMode === 'breakdown'
                                    ? isColumnVisible('Daily Total')
                                        ? 3
                                        : 2
                                    : isColumnVisible('Daily Total')
                                      ? 2
                                      : 1
                            }
                            textAlign="center"
                        >
                            <Checkbox
                                label="Expand"
                                fitted
                                toggle
                                checked={expandCompute}
                                onChange={() => setExpandCompute(!expandCompute)}
                            />
                        </SUITable.HeaderCell>
                        <SUITable.HeaderCell
                            colSpan={isColumnVisible('Cloud Storage') ? 1 : 0}
                            style={{
                                display: isColumnVisible('Cloud Storage') ? 'table-cell' : 'none',
                            }}
                        >
                            Storage Cost
                        </SUITable.HeaderCell>
                        <SUITable.HeaderCell
                            colSpan={
                                headerFields().length -
                                (isColumnVisible('Cloud Storage') ? 1 : 0) -
                                (isColumnVisible('Daily Total') ? 1 : 0)
                            }
                            style={{
                                display: headerFields().some(
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
                            }}
                        >
                            Date
                        </SUITable.HeaderCell>
                        {viewMode === 'breakdown' && (
                            <SUITable.HeaderCell
                                style={{
                                    borderBottom: 'none',
                                }}
                            >
                                {groupBy === BillingColumn.GcpProject ? 'Project' : 'Topic'}
                            </SUITable.HeaderCell>
                        )}
                        {headerFields().map((k) => (
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
                        ))}
                    </SUITable.Row>
                </SUITable.Header>
                <SUITable.Body>
                    {dataToBody(internalData)}
                    {viewMode === 'summary' && (
                        <SUITable.Row>
                            <SUITable.Cell collapsing>
                                <b>All Time Total</b>
                            </SUITable.Cell>
                            {headerFields().map((k) => (
                                <SUITable.Cell key={`Total ${k.category}`}>
                                    <b>
                                        {formatMoney(
                                            internalData.reduce(
                                                (acc, cur) => acc + (cur.values[k.category] || 0),
                                                0
                                            )
                                        )}
                                    </b>
                                </SUITable.Cell>
                            ))}
                        </SUITable.Row>
                    )}
                    {viewMode === 'breakdown' && breakdownData && (
                        <SUITable.Row>
                            <SUITable.Cell collapsing>
                                <b>{shouldUsePagination ? 'Date Total' : 'All Time Total'}</b>
                            </SUITable.Cell>
                            <SUITable.Cell collapsing>
                                <b>
                                    All{' '}
                                    {groupBy === BillingColumn.GcpProject ? 'Projects' : 'Topics'}
                                </b>
                            </SUITable.Cell>
                            {headerFields().map((k) => {
                                const total = shouldUsePagination
                                    ? // Paginated view: show total for current date only
                                      (() => {
                                          const currentDateStr = availableDates[currentPage - 1]
                                          return currentDateStr && breakdownData[currentDateStr]
                                              ? Object.values(breakdownData[currentDateStr]).reduce(
                                                    (fieldSum, categories) => {
                                                        return (
                                                            fieldSum + (categories[k.category] || 0)
                                                        )
                                                    },
                                                    0
                                                )
                                              : 0
                                      })()
                                    : // Non-paginated view: show total for all dates
                                      Object.values(breakdownData).reduce((dateSum, fieldData) => {
                                          return (
                                              dateSum +
                                              Object.values(fieldData).reduce(
                                                  (fieldSum, categories) => {
                                                      return (
                                                          fieldSum + (categories[k.category] || 0)
                                                      )
                                                  },
                                                  0
                                              )
                                          )
                                      }, 0)
                                return (
                                    <SUITable.Cell key={`Total ${k.category}`}>
                                        <b>{formatMoney(total)}</b>
                                    </SUITable.Cell>
                                )
                            })}
                        </SUITable.Row>
                    )}
                </SUITable.Body>
            </Table>
            {shouldUsePagination && availableDates.length > 1 && (
                <div
                    style={{
                        display: 'flex',
                        justifyContent: 'center',
                        alignItems: 'center',
                        marginTop: '20px',
                        gap: '10px',
                    }}
                >
                    <Pagination
                        count={availableDates.length}
                        page={currentPage}
                        onChange={(event, page) => setCurrentPage(page)}
                        color="primary"
                        showFirstButton
                        showLastButton
                        size="medium"
                    />
                    <span style={{ fontSize: '14px', color: '#666' }}>
                        Page {currentPage} of {availableDates.length} dates (showing paginated view
                        for {getTotalBreakdownRows()} total rows)
                    </span>
                </div>
            )}
        </>
    )
}

export default BillingCostByTimeTable
