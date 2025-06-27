import React from 'react'
import {
    Button,
    Checkbox,
    CheckboxProps,
    Dropdown,
    Header,
    Table as SUITable,
} from 'semantic-ui-react'
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

    // State to control dropdown menu open/close
    const [isColumnsDropdownOpen, setColumnsDropdownOpen] = React.useState<boolean>(false)

    // Properties
    const [expandCompute, setExpandCompute] = React.useState<boolean>(false)
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: null,
        direction: null,
    })

    // Handle outside clicks and keyboard events for dropdown
    React.useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            const target = event.target as HTMLElement
            const columnsDropdown = document.querySelector('.columns-dropdown')

            // If clicking outside the dropdown and dropdown is open, close it
            if (isColumnsDropdownOpen && columnsDropdown && !columnsDropdown.contains(target)) {
                setColumnsDropdownOpen(false)
            }
        }

        const handleKeyDown = (event: KeyboardEvent) => {
            // Close dropdown on ESC key press
            if (event.key === 'Escape' && isColumnsDropdownOpen) {
                setColumnsDropdownOpen(false)
            }
        }

        document.addEventListener('mousedown', handleClickOutside)
        document.addEventListener('keydown', handleKeyDown)

        return () => {
            document.removeEventListener('mousedown', handleClickOutside)
            document.removeEventListener('keydown', handleKeyDown)
        }
    }, [isColumnsDropdownOpen])

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

    // Define available columns based on expand state
    const getAvailableColumns = React.useCallback(() => {
        // Filter out 'Cloud Storage' from groups since it's always a storage cost, not compute cost
        const computeGroups = groups.filter((group) => group !== 'Cloud Storage')

        if (expandCompute) {
            // When expanded, show all individual compute categories, plus storage and daily total
            return [...computeGroups, 'Daily Total', 'Cloud Storage']
        } else {
            // When collapsed, show only summary columns
            return ['Daily Total', 'Cloud Storage', 'Compute Cost']
        }
    }, [expandCompute, groups])

    // Get available columns dynamically based on current expand state
    const availableColumns = getAvailableColumns()

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

        // Filter by visible columns
        return baseFields.filter((field) => visibleColumns.has(field.category))
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

    // Helper functions for column visibility
    const toggleColumnVisibility = (category: string, event?: React.SyntheticEvent) => {
        // Stop propagation to prevent dropdown from closing
        if (event) {
            event.stopPropagation()
        }

        const newSet = new Set(visibleColumns)
        if (newSet.has(category)) {
            newSet.delete(category)
        } else {
            newSet.add(category)
        }
        setVisibleColumns(newSet)
    }

    // Toggle parent column groups (Storage Cost / Compute Cost)
    const toggleParentColumnGroup = (
        parentGroup: 'Storage Cost' | 'Compute Cost',
        event?: React.SyntheticEvent
    ) => {
        if (event) {
            event.stopPropagation()
        }

        const newSet = new Set(visibleColumns)

        if (parentGroup === 'Storage Cost') {
            // Toggle Cloud Storage
            if (newSet.has('Cloud Storage')) {
                newSet.delete('Cloud Storage')
            } else {
                newSet.add('Cloud Storage')
            }
        } else if (parentGroup === 'Compute Cost') {
            if (expandCompute) {
                // When expanded, toggle all individual compute categories
                const computeGroups = groups.filter((group) => group !== 'Cloud Storage')
                const allComputeVisible = computeGroups.every((group) => newSet.has(group))

                if (allComputeVisible) {
                    // Hide all compute categories
                    computeGroups.forEach((group) => newSet.delete(group))
                } else {
                    // Show all compute categories
                    computeGroups.forEach((group) => newSet.add(group))
                }
            } else {
                // When collapsed, toggle the Compute Cost summary
                if (newSet.has('Compute Cost')) {
                    newSet.delete('Compute Cost')
                } else {
                    newSet.add('Compute Cost')
                }
            }
        }

        setVisibleColumns(newSet)
    }

    // Check if parent group is visible
    const isParentGroupVisible = (parentGroup: 'Storage Cost' | 'Compute Cost'): boolean => {
        if (parentGroup === 'Storage Cost') {
            return visibleColumns.has('Cloud Storage')
        } else if (parentGroup === 'Compute Cost') {
            if (expandCompute) {
                // When expanded, check if any individual compute categories are visible
                const computeGroups = groups.filter((group) => group !== 'Cloud Storage')
                return computeGroups.some((group) => visibleColumns.has(group))
            } else {
                // When collapsed, check if Compute Cost summary is visible
                return visibleColumns.has('Compute Cost')
            }
        }
        return false
    }

    // Toggle all columns in a group
    const toggleColumnGroup = (categoryGroup: string[], visible: boolean) => {
        const newSet = new Set(visibleColumns)
        categoryGroup.forEach((category) => {
            if (visible) {
                newSet.add(category)
            } else {
                newSet.delete(category)
            }
        })
        setVisibleColumns(newSet)
    }

    // Reusable column checkbox component
    const ColumnCheckbox = ({ category, label }: { category: string; label: string }) => {
        const handleItemClick = (e: React.MouseEvent) => {
            e.stopPropagation()
            e.preventDefault()
            toggleColumnVisibility(category, e)
        }

        // Use the correct type for Semantic UI's onChange handler
        const handleChange = (e: React.FormEvent<HTMLInputElement>, _data: CheckboxProps) => {
            e.stopPropagation()
            toggleColumnVisibility(category, e)
        }

        const isVisible = visibleColumns.has(category)

        return (
            <Dropdown.Item
                onClick={handleItemClick}
                onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                role="menuitemcheckbox"
                aria-checked={isVisible}
            >
                <Checkbox
                    label={label}
                    checked={isVisible}
                    onChange={handleChange}
                    onClick={(e: React.MouseEvent) => e.stopPropagation()}
                />
            </Dropdown.Item>
        )
    }

    // Reusable parent column checkbox component
    const ParentColumnCheckbox = ({
        parentGroup,
        label,
    }: {
        parentGroup: 'Storage Cost' | 'Compute Cost'
        label: string
    }) => {
        const handleItemClick = (e: React.MouseEvent) => {
            e.stopPropagation()
            e.preventDefault()
            toggleParentColumnGroup(parentGroup, e)
        }

        // Use the correct type for Semantic UI's onChange handler
        const handleChange = (e: React.FormEvent<HTMLInputElement>, _data: CheckboxProps) => {
            e.stopPropagation()
            toggleParentColumnGroup(parentGroup, e)
        }

        const isVisible = isParentGroupVisible(parentGroup)

        return (
            <Dropdown.Item
                onClick={handleItemClick}
                onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                role="menuitemcheckbox"
                aria-checked={isVisible}
            >
                <Checkbox
                    label={label}
                    checked={isVisible}
                    onChange={handleChange}
                    onClick={(e: React.MouseEvent) => e.stopPropagation()}
                />
            </Dropdown.Item>
        )
    }

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
                    <Dropdown
                        button
                        className="icon columns-dropdown"
                        floating
                        labeled
                        icon="columns"
                        text="Columns"
                        style={{
                            minWidth: '120px',
                            zIndex: 1000,
                        }}
                        open={isColumnsDropdownOpen}
                        onClick={(e) => {
                            // Prevent toggling if clicking on dropdown menu content
                            const target = e.target as HTMLElement
                            if (target && target.closest('.dropdown-menu-content')) {
                                return
                            }
                            setColumnsDropdownOpen(!isColumnsDropdownOpen)
                        }}
                        // Don't use onClose or onOpen, as we want to manually control it
                        closeOnBlur={false}
                        closeOnChange={false}
                        closeOnEscape={true}
                    >
                        <Dropdown.Menu className="dropdown-menu-content" style={{ zIndex: 1001 }}>
                            <Dropdown.Header icon="table" content="Column Visibility" />
                            <Dropdown.Item
                                onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                onClick={(e: React.MouseEvent) => e.stopPropagation()}
                            >
                                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                                    <Button
                                        compact
                                        size="mini"
                                        onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                        onClick={(e: React.MouseEvent) => {
                                            e.stopPropagation()
                                            e.preventDefault()
                                            toggleColumnGroup(availableColumns, true)
                                        }}
                                    >
                                        Select All
                                    </Button>
                                    <Button
                                        compact
                                        size="mini"
                                        onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                        onClick={(e: React.MouseEvent) => {
                                            e.stopPropagation()
                                            e.preventDefault()
                                            toggleColumnGroup(availableColumns, false)
                                        }}
                                    >
                                        Hide All
                                    </Button>
                                </div>
                            </Dropdown.Item>
                            <Dropdown.Divider />

                            {/* Main Column Groups */}
                            <Dropdown.Header content="Column Groups" />
                            <ColumnCheckbox category="Daily Total" label="Daily Total" />
                            <ParentColumnCheckbox parentGroup="Storage Cost" label="Storage Cost" />
                            <ParentColumnCheckbox parentGroup="Compute Cost" label="Compute Cost" />

                            {/* Individual Compute Categories - only show when expanded */}
                            {expandCompute && groups.length > 0 && (
                                <>
                                    <Dropdown.Divider />
                                    <Dropdown.Header content="Individual Compute Categories" />
                                    {groups
                                        .filter((group) => group !== 'Cloud Storage') // Exclude Cloud Storage from compute categories
                                        .sort()
                                        .map((group) => (
                                            <ColumnCheckbox
                                                key={group}
                                                category={group}
                                                label={group}
                                            />
                                        ))}
                                </>
                            )}
                        </Dropdown.Menu>
                    </Dropdown>
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
                            colSpan={visibleColumns.has('Daily Total') ? 2 : 1}
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
                            colSpan={visibleColumns.has('Cloud Storage') ? 1 : 0}
                            style={{
                                display: visibleColumns.has('Cloud Storage')
                                    ? 'table-cell'
                                    : 'none',
                            }}
                        >
                            Storage Cost
                        </SUITable.HeaderCell>
                        <SUITable.HeaderCell
                            colSpan={
                                headerFields().length -
                                (visibleColumns.has('Cloud Storage') ? 1 : 0) -
                                (visibleColumns.has('Daily Total') ? 1 : 0)
                            }
                            style={{
                                display: isParentGroupVisible('Compute Cost')
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
