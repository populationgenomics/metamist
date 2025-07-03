import React from 'react'
import { useClickAway } from 'react-use'
import { Button, Checkbox, CheckboxProps, Dropdown } from 'semantic-ui-react'
import './ColumnVisibilityDropdown.css'

export interface ColumnConfig {
    id: string
    label: string
    isRequired?: boolean
    group?: string
}

export interface ColumnGroup {
    id: string
    label: string
    columns: string[]
}

export interface ColumnVisibilityDropdownProps {
    /** List of column configurations */
    columns: ColumnConfig[]
    /** Optional column groups for organization */
    groups?: ColumnGroup[]
    /** Currently visible columns */
    visibleColumns: Set<string>
    /** Callback when column visibility changes */
    onVisibilityChange: (visibleColumns: Set<string>) => void
    /** Optional custom button style */
    buttonStyle?: React.CSSProperties
    /** Optional custom dropdown class */
    className?: string
    /** Optional label formatter for columns */
    labelFormatter?: (column: ColumnConfig) => string
}

const ColumnVisibilityDropdown: React.FC<ColumnVisibilityDropdownProps> = ({
    columns,
    groups = [],
    visibleColumns,
    onVisibilityChange,
    buttonStyle,
    className,
    labelFormatter,
}) => {
    const [isOpen, setIsOpen] = React.useState(false)
    const dropdownRef = React.useRef<HTMLDivElement>(null)

    // Handle outside clicks
    useClickAway(dropdownRef, () => {
        if (isOpen) {
            setIsOpen(false)
        }
    })

    // Handle keyboard events
    React.useEffect(() => {
        const handleKeyDown = (event: KeyboardEvent) => {
            if (event.key === 'Escape' && isOpen) {
                setIsOpen(false)
            }
        }

        document.addEventListener('keydown', handleKeyDown)
        return () => document.removeEventListener('keydown', handleKeyDown)
    }, [isOpen])

    // Toggle individual column visibility
    const toggleColumn = (columnId: string, event?: React.SyntheticEvent) => {
        if (event) {
            event.stopPropagation()
        }

        const column = columns.find((c) => c.id === columnId)
        if (column?.isRequired) {
            return // Don't allow hiding required columns
        }

        const newVisibleColumns = new Set(visibleColumns)
        if (newVisibleColumns.has(columnId)) {
            newVisibleColumns.delete(columnId)
        } else {
            newVisibleColumns.add(columnId)
        }
        onVisibilityChange(newVisibleColumns)
    }

    // Toggle all columns in a group
    const toggleGroup = (columnIds: string[], visible: boolean) => {
        const newVisibleColumns = new Set(visibleColumns)
        columnIds.forEach((columnId) => {
            const column = columns.find((c) => c.id === columnId)
            if (column && !column.isRequired) {
                if (visible) {
                    newVisibleColumns.add(columnId)
                } else {
                    newVisibleColumns.delete(columnId)
                }
            }
        })
        onVisibilityChange(newVisibleColumns)
    }

    // Toggle all columns
    const toggleAllColumns = (visible: boolean) => {
        const allColumnIds = columns.map((c) => c.id)
        toggleGroup(allColumnIds, visible)
    }

    // Check if column is visible
    const isColumnVisible = (columnId: string): boolean => {
        const column = columns.find((c) => c.id === columnId)
        if (column?.isRequired) {
            return true
        }
        return visibleColumns.has(columnId)
    }

    // Column checkbox component
    const ColumnCheckbox: React.FC<{ column: ColumnConfig }> = ({ column }) => {
        const handleItemClick = (e: React.MouseEvent) => {
            e.stopPropagation()
            e.preventDefault()
            toggleColumn(column.id, e)
        }

        const handleChange = (e: React.FormEvent<HTMLInputElement>, _data: CheckboxProps) => {
            e.stopPropagation()
            toggleColumn(column.id, e)
        }

        const isVisible = isColumnVisible(column.id)
        const isDisabled = column.isRequired

        const label = labelFormatter ? labelFormatter(column) : column.label

        return (
            <Dropdown.Item
                onClick={handleItemClick}
                onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                role="menuitemcheckbox"
                aria-checked={isVisible}
                disabled={isDisabled}
                className={`column-checkbox-item ${isVisible ? 'selected' : ''}`}
            >
                <div className="column-checkbox-content">
                    <Checkbox
                        checked={isVisible}
                        onChange={handleChange}
                        onClick={(e: React.MouseEvent) => e.stopPropagation()}
                        disabled={isDisabled}
                    />
                    <span className={`column-label ${isVisible ? 'visible' : ''}`}>{label}</span>
                </div>
            </Dropdown.Item>
        )
    }

    // Group toggle buttons
    const GroupToggleButtons: React.FC<{ group: ColumnGroup }> = ({ group }) => (
        <Dropdown.Item
            onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
            onClick={(e: React.MouseEvent) => e.stopPropagation()}
            className="group-toggle-item"
        >
            <div className="group-toggle-buttons">
                <Button
                    compact
                    size="mini"
                    color="blue"
                    onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                    onClick={(e: React.MouseEvent) => {
                        e.stopPropagation()
                        e.preventDefault()
                        toggleGroup(group.columns, true)
                    }}
                >
                    Select All
                </Button>
                <Button
                    compact
                    size="mini"
                    color="grey"
                    onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                    onClick={(e: React.MouseEvent) => {
                        e.stopPropagation()
                        e.preventDefault()
                        toggleGroup(group.columns, false)
                    }}
                >
                    Hide All
                </Button>
            </div>
        </Dropdown.Item>
    )

    // Get columns by group
    const getColumnsByGroup = (groupId?: string) => {
        if (!groupId) {
            return columns.filter((c) => !c.group)
        }
        return columns.filter((c) => c.group === groupId)
    }

    // Get required columns
    const requiredColumns = columns.filter((c) => c.isRequired)
    const ungroupedColumns = columns.filter((c) => !c.group && !c.isRequired)

    return (
        <div ref={dropdownRef} className={className}>
            <Dropdown
                button
                className="icon columns-dropdown"
                floating
                labeled
                icon="columns"
                text="Columns"
                style={{ marginRight: '10px', ...buttonStyle }}
                open={isOpen}
                onClick={(e) => {
                    const target = e.target as HTMLElement
                    if (target && target.closest('.dropdown-menu-content')) {
                        return
                    }
                    setIsOpen(!isOpen)
                }}
                closeOnBlur={false}
                closeOnChange={false}
                closeOnEscape={true}
            >
                <Dropdown.Menu className="dropdown-menu-content">
                    <Dropdown.Header
                        icon="table"
                        content="Column Visibility"
                        className="dropdown-header"
                    />

                    {/* Global toggle buttons */}
                    <Dropdown.Item
                        onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                        onClick={(e: React.MouseEvent) => e.stopPropagation()}
                        className="global-toggle-item"
                    >
                        <div className="global-toggle-buttons">
                            <Button
                                compact
                                size="mini"
                                color="blue"
                                onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                onClick={(e: React.MouseEvent) => {
                                    e.stopPropagation()
                                    e.preventDefault()
                                    toggleAllColumns(true)
                                }}
                            >
                                Select All
                            </Button>
                            <Button
                                compact
                                size="mini"
                                color="grey"
                                onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                onClick={(e: React.MouseEvent) => {
                                    e.stopPropagation()
                                    e.preventDefault()
                                    toggleAllColumns(false)
                                }}
                            >
                                Hide All
                            </Button>
                        </div>
                    </Dropdown.Item>

                    {/* Required columns */}
                    {requiredColumns.length > 0 && (
                        <div className="required-columns-section">
                            <Dropdown.Header
                                content="Required Columns"
                                className="section-header"
                            />
                            {requiredColumns.map((column) => (
                                <Dropdown.Item
                                    key={column.id}
                                    disabled
                                    className="required-column-item"
                                >
                                    <Checkbox
                                        label={
                                            labelFormatter ? labelFormatter(column) : column.label
                                        }
                                        checked={true}
                                        readOnly
                                        disabled
                                    />
                                </Dropdown.Item>
                            ))}
                        </div>
                    )}

                    {/* Grouped columns */}
                    {groups.map((group) => (
                        <div key={group.id} className="column-group-section">
                            <Dropdown.Header content={group.label} className="section-header" />
                            <GroupToggleButtons group={group} />
                            {getColumnsByGroup(group.id).map((column) => (
                                <ColumnCheckbox key={column.id} column={column} />
                            ))}
                        </div>
                    ))}

                    {/* Ungrouped columns */}
                    {ungroupedColumns.length > 0 && (
                        <div className="ungrouped-columns-section">
                            {groups.length > 0 && (
                                <Dropdown.Header
                                    content="Other Columns"
                                    className="section-header"
                                />
                            )}
                            {ungroupedColumns.map((column) => (
                                <ColumnCheckbox key={column.id} column={column} />
                            ))}
                        </div>
                    )}
                </Dropdown.Menu>
            </Dropdown>
        </div>
    )
}

export default ColumnVisibilityDropdown
