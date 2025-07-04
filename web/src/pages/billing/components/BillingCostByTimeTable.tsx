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

interface IBillingCostByTimeTableProps {
    heading: string
    start: string
    end: string
    groups: string[]
    isLoading: boolean
    data: IStackedAreaByDateChartData[]
    visibleColumns: Set<string>
    setVisibleColumns: (columns: Set<string>) => void
    exportToFile: (format: 'csv' | 'tsv') => void
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
    exportToFile,
}) => {
    const [internalData, setInternalData] = React.useState<IStackedAreaByDateChartData[]>([])
    const [internalGroups, setInternalGroups] = React.useState<string[]>([])

    // Properties
    const [expandCompute, setExpandCompute] = React.useState<boolean>(false)
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

    // Generate column configurations for the dropdown
    const getColumnConfigs = React.useCallback((): ColumnConfig[] => {
        const configs: ColumnConfig[] = [
            { id: 'Daily Total', label: 'Daily Total', group: 'summary' },
            { id: 'Cloud Storage', label: 'Cloud Storage', group: 'storage' },
        ]

        if (expandCompute) {
            // When expanded, add individual compute categories
            const computeGroups = groups.filter((group) => group !== 'Cloud Storage')
            computeGroups.forEach((group) => {
                configs.push({ id: group, label: group, group: 'compute' })
            })
        } else {
            // When collapsed, add summary compute cost
            configs.push({ id: 'Compute Cost', label: 'Compute Cost', group: 'summary' })
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
            if (computeColumns.length > 0) {
                groups.push({ id: 'compute', label: 'Compute Categories', columns: computeColumns })
            }
        } else {
            groups[0].columns.push('Compute Cost') // Add to summary group
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

    const dataToBody = (data: IStackedAreaByDateChartData[]) => (
        <>
            {dataSort(
                data,
                sort.column ? [sort.column] : [],
                sort.direction === 'ascending' ? ['asc'] : ['desc']
            ).map((p) => (
                <React.Fragment key={p.date.toISOString()}>
                    <SUITable.Row>
                        <SUITable.Cell collapsing key={`Date - ${p.date.toISOString()}`}>
                            <b>{p.date.toLocaleDateString()}</b>
                        </SUITable.Cell>
                        {headerFields().map((k) => (
                            <SUITable.Cell key={`${p.date.toISOString()} - ${k.category}`}>
                                {formatMoney(p.values[k.category])}
                            </SUITable.Cell>
                        ))}
                    </SUITable.Row>
                </React.Fragment>
            ))}
        </>
    )

    return (
        <>
            <div
                style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    marginBottom: '15px',
                }}
            >
                <Header as="h3" style={{ margin: 0 }}>
                    {convertFieldName(heading)} costs from {start} to {end}
                </Header>
                <div style={{ display: 'flex', gap: '10px' }}>
                    <ColumnVisibilityDropdown
                        columns={getColumnConfigs()}
                        groups={getColumnGroups()}
                        visibleColumns={visibleColumns}
                        onVisibilityChange={setVisibleColumns}
                        searchThreshold={8}
                        searchPlaceholder="Search time periods..."
                    />
                    <Dropdown
                        button
                        className="icon"
                        floating
                        labeled
                        icon="download"
                        text="Export"
                        style={{
                            minWidth: '100px',
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
            <Table celled compact sortable selectable>
                <SUITable.Header>
                    <SUITable.Row>
                        <SUITable.HeaderCell
                            colSpan={isColumnVisible('Daily Total') ? 2 : 1}
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
                    <SUITable.Row>
                        <SUITable.Cell collapsing>
                            <b>All Time Total</b>
                        </SUITable.Cell>
                        {headerFields().map((k) => (
                            <SUITable.Cell key={`Total ${k.category}`}>
                                <b>
                                    {formatMoney(
                                        internalData.reduce(
                                            (acc, cur) => acc + cur.values[k.category],
                                            0
                                        )
                                    )}
                                </b>
                            </SUITable.Cell>
                        ))}
                    </SUITable.Row>
                </SUITable.Body>
            </Table>
        </>
    )
}

export default BillingCostByTimeTable
