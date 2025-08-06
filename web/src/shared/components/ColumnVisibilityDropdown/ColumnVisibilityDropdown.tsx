import React from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
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
    /** Show search input when column count exceeds this threshold (default: 10) */
    searchThreshold?: number
    /** Custom search placeholder text */
    searchPlaceholder?: string
    /** Enable URL persistence for column selection (default: false) */
    enableUrlPersistence?: boolean
    /** URL parameter name for column selection (default: 'columns') */
    urlParamName?: string
}

/**
 * Reusable toggle buttons component for selecting/hiding all columns or groups.
 */
const ToggleButtons: React.FC<{
    onSelectAll: () => void
    onHideAll: () => void
    className?: string
}> = ({ onSelectAll, onHideAll, className = '' }) => (
    <Dropdown.Item
        onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
        onClick={(e: React.MouseEvent) => e.stopPropagation()}
        className={className}
    >
        <div className="toggle-buttons">
            <Button
                compact
                size="mini"
                color="blue"
                onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                onClick={(e: React.MouseEvent) => {
                    e.stopPropagation()
                    e.preventDefault()
                    onSelectAll()
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
                    onHideAll()
                }}
            >
                Hide All
            </Button>
        </div>
    </Dropdown.Item>
)

const ColumnCheckbox: React.FC<{
    column: ColumnConfig
    isColumnVisible: (columnId: string) => boolean
    toggleColumn: (columnId: string, event?: React.SyntheticEvent) => void
    labelFormatter?: (column: ColumnConfig) => string
}> = ({ column, isColumnVisible, toggleColumn, labelFormatter }) => {
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

const ColumnVisibilityDropdown: React.FC<ColumnVisibilityDropdownProps> = ({
    columns,
    groups = [],
    visibleColumns,
    onVisibilityChange,
    buttonStyle,
    className,
    labelFormatter,
    searchThreshold = 10,
    searchPlaceholder = 'Search columns...',
    enableUrlPersistence = false,
    urlParamName = 'columns',
}) => {
    const [isOpen, setIsOpen] = React.useState(false)
    const [searchTerm, setSearchTerm] = React.useState('')
    const dropdownRef = React.useRef<HTMLDivElement>(null)
    const searchInputRef = React.useRef<HTMLInputElement>(null)
    const stickyHeaderRef = React.useRef<HTMLDivElement>(null)

    // URL persistence hooks
    const location = useLocation()
    const navigate = useNavigate()
    const isInitialized = React.useRef(false)

    // Show search when there are many columns
    const showSearch = columns.length > searchThreshold

    // URL persistence utility functions
    const encodeColumnsForUrl = (columns: Set<string>): string => {
        return Array.from(columns).sort().join(',')
    }

    const decodeColumnsFromUrl = (param: string | null): Set<string> => {
        if (!param) return new Set()
        return new Set(param.split(',').filter(Boolean))
    }

    const updateUrlWithColumns = React.useCallback(
        (columns: Set<string>) => {
            if (!enableUrlPersistence) return

            const searchParams = new URLSearchParams(location.search)
            const encodedColumns = encodeColumnsForUrl(columns)

            if (encodedColumns) {
                searchParams.set(urlParamName, encodedColumns)
            } else {
                searchParams.delete(urlParamName)
            }

            const newUrl = `${location.pathname}?${searchParams.toString()}`
            navigate(newUrl, { replace: true })
        },
        [enableUrlPersistence, location.search, location.pathname, navigate, urlParamName]
    )

    // Initialize from URL on mount
    React.useEffect(() => {
        if (enableUrlPersistence && !isInitialized.current && columns.length > 0) {
            const searchParams = new URLSearchParams(location.search)
            const urlColumns = decodeColumnsFromUrl(searchParams.get(urlParamName))

            if (urlColumns.size > 0) {
                // Validate that URL columns exist in available columns
                const availableColumnIds = new Set(columns.map((c) => c.id))
                const validUrlColumns = new Set(
                    Array.from(urlColumns).filter((id) => availableColumnIds.has(id))
                )

                if (validUrlColumns.size > 0) {
                    onVisibilityChange(validUrlColumns)
                }
            }
            isInitialized.current = true
        }
    }, [enableUrlPersistence, urlParamName, columns, location.search, onVisibilityChange])

    // Enhanced onVisibilityChange that also updates URL
    const handleVisibilityChange = React.useCallback(
        (newVisibleColumns: Set<string>) => {
            onVisibilityChange(newVisibleColumns)
            updateUrlWithColumns(newVisibleColumns)
        },
        [onVisibilityChange, updateUrlWithColumns]
    )

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

    // Focus search input when dropdown opens
    React.useEffect(() => {
        if (isOpen && showSearch && searchInputRef.current) {
            setTimeout(() => {
                searchInputRef.current?.focus()
            }, 100)
        }
    }, [isOpen, showSearch])

    // Clear search when dropdown closes
    React.useEffect(() => {
        if (!isOpen) {
            setSearchTerm('')
        }
    }, [isOpen])

    // Filter columns based on search term
    const filterColumns = (columnsToFilter: ColumnConfig[]): ColumnConfig[] => {
        if (!searchTerm.trim()) {
            return columnsToFilter
        }

        const term = searchTerm.toLowerCase()
        return columnsToFilter.filter((column) => {
            const label = labelFormatter ? labelFormatter(column) : column.label
            return (
                label.toLowerCase().includes(term) ||
                column.id.toLowerCase().includes(term) ||
                column.group?.toLowerCase().includes(term)
            )
        })
    }

    // Filter groups based on search term
    const filterGroups = (groupsToFilter: ColumnGroup[]): ColumnGroup[] => {
        if (!searchTerm.trim()) {
            return groupsToFilter
        }

        const term = searchTerm.toLowerCase()
        return groupsToFilter.filter((group) => {
            const groupColumns = columns.filter((c) => c.group === group.id)
            const filteredGroupColumns = filterColumns(groupColumns)
            return filteredGroupColumns.length > 0 || group.label.toLowerCase().includes(term)
        })
    }

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
        handleVisibilityChange(newVisibleColumns)
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
        handleVisibilityChange(newVisibleColumns)
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

    // Group toggle buttons
    const GroupToggleButtons: React.FC<{ group: ColumnGroup }> = ({ group }) => (
        <ToggleButtons
            onSelectAll={() => toggleGroup(group.columns, true)}
            onHideAll={() => toggleGroup(group.columns, false)}
            className="group-toggle-item"
        />
    )

    // Get columns by group
    const getColumnsByGroup = (groupId?: string) => {
        if (!groupId) {
            return columns.filter((c) => !c.group)
        }
        return columns.filter((c) => c.group === groupId)
    }

    // Get required columns
    const requiredColumns = filterColumns(columns.filter((c) => c.isRequired))
    const ungroupedColumns = filterColumns(columns.filter((c) => !c.group && !c.isRequired))
    const filteredGroups = filterGroups(groups)

    // Check if we have any results
    const hasResults =
        requiredColumns.length > 0 ||
        ungroupedColumns.length > 0 ||
        filteredGroups.some((group) => filterColumns(getColumnsByGroup(group.id)).length > 0)

    // Update sticky positioning dynamically
    React.useEffect(() => {
        if (isOpen && stickyHeaderRef.current && dropdownRef.current) {
            const stickyHeader = stickyHeaderRef.current
            const menu = dropdownRef.current.querySelector('.dropdown-menu-content')

            if (menu) {
                const stickyHeaderHeight = stickyHeader.offsetHeight
                const sectionHeaders = menu.querySelectorAll('.section-header')
                const groupToggleButtons = menu.querySelectorAll('.group-toggle-item')

                // Update section header positions
                sectionHeaders.forEach((header) => {
                    const headerElement = header as HTMLElement
                    headerElement.style.top = `${stickyHeaderHeight}px`
                })

                // Update group toggle button positions
                groupToggleButtons.forEach((button) => {
                    const buttonElement = button as HTMLElement
                    buttonElement.style.top = `${stickyHeaderHeight + 32}px` // +32px for section header height
                })
            }
        }
    }, [isOpen, showSearch, hasResults])

    // Handle dynamic positioning based on viewport
    React.useEffect(() => {
        if (isOpen && dropdownRef.current) {
            const dropdownElement = dropdownRef.current
            const menuElement = dropdownElement.querySelector('.menu') as HTMLElement

            if (menuElement) {
                const rect = dropdownElement.getBoundingClientRect()
                const menuRect = menuElement.getBoundingClientRect()
                const viewportWidth = window.innerWidth

                // Remove existing position classes
                menuElement.classList.remove('position-left')

                // Check if dropdown would overflow to the right
                if (rect.left + menuRect.width > viewportWidth - 20) {
                    menuElement.classList.add('position-left')
                }
            }
        }
    }, [isOpen])

    return (
        <div ref={dropdownRef} className={className}>
            <Dropdown
                button
                className="icon columns-dropdown"
                floating
                labeled
                icon="columns"
                text="Fields"
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
                <Dropdown.Menu
                    className={`dropdown-menu-content${showSearch ? ' has-search' : ''}`}
                >
                    {/* Sticky header section - groups header, search, and toggle buttons */}
                    <div ref={stickyHeaderRef} className="sticky-header-section">
                        <Dropdown.Header
                            icon="table"
                            content="Field Visibility"
                            className="dropdown-header"
                        />

                        {/* Search input */}
                        {showSearch && (
                            <div className="search-section">
                                <input
                                    ref={searchInputRef}
                                    type="text"
                                    className="search-input"
                                    placeholder={searchPlaceholder}
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                    onClick={(e) => e.stopPropagation()}
                                    onKeyDown={(e) => e.stopPropagation()}
                                />
                            </div>
                        )}

                        {/* Global toggle buttons - only show if we have results */}
                        {hasResults && (
                            <ToggleButtons
                                onSelectAll={() => toggleAllColumns(true)}
                                onHideAll={() => toggleAllColumns(false)}
                                className="global-toggle-item"
                            />
                        )}
                    </div>

                    {/* Show no results message */}
                    {showSearch && searchTerm.trim() && !hasResults && (
                        <div className="no-results-message">
                            No fields found matching &ldquo;{searchTerm}&rdquo;
                        </div>
                    )}

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
                    {filteredGroups.map((group) => {
                        const groupColumns = filterColumns(getColumnsByGroup(group.id))
                        if (groupColumns.length === 0) return null

                        return (
                            <div key={group.id} className="column-group-section">
                                <Dropdown.Header content={group.label} className="section-header" />
                                <GroupToggleButtons group={group} />
                                {groupColumns.map((column) => (
                                    <ColumnCheckbox
                                        key={column.id}
                                        column={column}
                                        isColumnVisible={isColumnVisible}
                                        toggleColumn={toggleColumn}
                                        labelFormatter={labelFormatter}
                                    />
                                ))}
                            </div>
                        )
                    })}

                    {/* Ungrouped columns */}
                    {ungroupedColumns.length > 0 && (
                        <div className="ungrouped-columns-section">
                            {filteredGroups.length > 0 && (
                                <Dropdown.Header
                                    content="Other Columns"
                                    className="section-header"
                                />
                            )}
                            {ungroupedColumns.map((column) => (
                                <ColumnCheckbox
                                    key={column.id}
                                    column={column}
                                    isColumnVisible={isColumnVisible}
                                    toggleColumn={toggleColumn}
                                    labelFormatter={labelFormatter}
                                />
                            ))}
                        </div>
                    )}
                </Dropdown.Menu>
            </Dropdown>
        </div>
    )
}

export default ColumnVisibilityDropdown
