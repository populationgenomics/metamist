import { KeyboardArrowDown, Search as SearchIcon, ViewColumn } from '@mui/icons-material'
import {
    Box,
    Button,
    Checkbox,
    Divider,
    FormControlLabel,
    InputAdornment,
    List,
    ListItem,
    ListSubheader,
    MenuItem,
    Paper,
    Popover,
    TextField,
    Typography,
} from '@mui/material'
import { styled } from '@mui/material/styles'
import React from 'react'
import { useLocation, useNavigate } from 'react-router-dom'

// Styled components for better theme integration
const StyledPopover = styled(Popover)(({ theme }) => ({
    '& .MuiPaper-root': {
        minWidth: 300,
        maxHeight: 500,
        overflow: 'hidden',
        borderRadius: theme.shape.borderRadius,
        boxShadow: theme.shadows[8],
    },
}))

const StyledList = styled(List)(({ theme }) => ({
    padding: 0,
    maxHeight: 400,
    overflow: 'auto',
    '& .MuiListSubheader-root': {
        backgroundColor: theme.palette.background.default,
        borderBottom: `1px solid ${theme.palette.divider}`,
        lineHeight: '36px',
        fontSize: '0.75rem',
        fontWeight: 600,
        textTransform: 'uppercase',
        letterSpacing: '0.5px',
    },
}))

const HeaderSection = styled(Box)(({ theme }) => ({
    padding: theme.spacing(2),
    borderBottom: `1px solid ${theme.palette.divider}`,
    backgroundColor: theme.palette.background.paper,
    position: 'sticky',
    top: 0,
    zIndex: 1,
}))

const ToggleButtonsContainer = styled(Box)(({ theme }) => ({
    display: 'flex',
    gap: theme.spacing(1),
    marginTop: theme.spacing(1),
}))

const StyledMenuItem = styled(MenuItem)(({ theme }) => ({
    '&.column-checkbox-item': {
        padding: theme.spacing(0.5, 2),
        '&.selected': {
            backgroundColor: theme.palette.action.selected,
        },
        '& .MuiFormControlLabel-root': {
            margin: 0,
            width: '100%',
            '& .MuiFormControlLabel-label': {
                fontSize: '0.875rem',
                '&.visible': {
                    fontWeight: 500,
                },
            },
        },
    },
}))

const NoResultsBox = styled(Box)(({ theme }) => ({
    padding: theme.spacing(3),
    textAlign: 'center',
    color: theme.palette.text.secondary,
    fontStyle: 'italic',
}))

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
}> = ({ onSelectAll, onHideAll }) => (
    <ToggleButtonsContainer>
        <Button
            size="small"
            variant="contained"
            color="primary"
            onClick={(e: React.MouseEvent) => {
                e.stopPropagation()
                e.preventDefault()
                onSelectAll()
            }}
            sx={{ fontSize: '0.75rem', minWidth: 'auto', px: 1 }}
        >
            Select All
        </Button>
        <Button
            size="small"
            variant="outlined"
            onClick={(e: React.MouseEvent) => {
                e.stopPropagation()
                e.preventDefault()
                onHideAll()
            }}
            sx={{ fontSize: '0.75rem', minWidth: 'auto', px: 1 }}
        >
            Hide All
        </Button>
    </ToggleButtonsContainer>
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

    const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        e.stopPropagation()
        toggleColumn(column.id, e)
    }

    const isVisible = isColumnVisible(column.id)
    const isDisabled = column.isRequired

    const label = labelFormatter ? labelFormatter(column) : column.label

    return (
        <StyledMenuItem
            onClick={handleItemClick}
            role="menuitemcheckbox"
            aria-checked={isVisible}
            disabled={isDisabled}
            className={`column-checkbox-item ${isVisible ? 'selected' : ''}`}
            disableRipple
        >
            <FormControlLabel
                control={
                    <Checkbox
                        checked={isVisible}
                        onChange={handleChange}
                        onClick={(e: React.MouseEvent) => e.stopPropagation()}
                        disabled={isDisabled}
                        size="small"
                    />
                }
                label={
                    <span className={`column-label ${isVisible ? 'visible' : ''}`}>{label}</span>
                }
            />
        </StyledMenuItem>
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
    const searchInputRef = React.useRef<HTMLInputElement>(null)

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

    const [anchorEl, setAnchorEl] = React.useState<null | HTMLElement>(null)

    return (
        <div className={className}>
            <Button
                variant="outlined"
                startIcon={<ViewColumn />}
                endIcon={<KeyboardArrowDown />}
                onClick={(e: React.MouseEvent<HTMLButtonElement>) => {
                    setAnchorEl(e.currentTarget)
                    setIsOpen(!isOpen)
                }}
                style={{ marginRight: '10px', ...buttonStyle }}
                aria-haspopup="true"
                aria-expanded={isOpen}
            >
                Fields
            </Button>
            <StyledPopover
                anchorEl={anchorEl}
                open={isOpen}
                onClose={() => {
                    setIsOpen(false)
                    setAnchorEl(null)
                }}
                anchorOrigin={{
                    vertical: 'bottom',
                    horizontal: 'left',
                }}
                transformOrigin={{
                    vertical: 'top',
                    horizontal: 'left',
                }}
                disableAutoFocus
            >
                <Paper>
                    {/* Header section with search and toggle buttons */}
                    <HeaderSection>
                        <Typography
                            variant="subtitle2"
                            sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}
                        >
                            <ViewColumn fontSize="small" />
                            Field Visibility
                        </Typography>

                        {/* Search input */}
                        {showSearch && (
                            <TextField
                                ref={searchInputRef}
                                size="small"
                                fullWidth
                                placeholder={searchPlaceholder}
                                value={searchTerm}
                                onChange={(e) => setSearchTerm(e.target.value)}
                                onClick={(e) => e.stopPropagation()}
                                onKeyDown={(e) => e.stopPropagation()}
                                variant="outlined"
                                InputProps={{
                                    startAdornment: (
                                        <InputAdornment position="start">
                                            <SearchIcon fontSize="small" />
                                        </InputAdornment>
                                    ),
                                }}
                                sx={{ mb: 1 }}
                            />
                        )}

                        {/* Global toggle buttons */}
                        {hasResults && (
                            <ToggleButtons
                                onSelectAll={() => toggleAllColumns(true)}
                                onHideAll={() => toggleAllColumns(false)}
                            />
                        )}
                    </HeaderSection>

                    {/* Show no results message */}
                    {showSearch && searchTerm.trim() && !hasResults && (
                        <NoResultsBox>
                            No fields found matching &ldquo;{searchTerm}&rdquo;
                        </NoResultsBox>
                    )}

                    {/* Content List */}
                    <StyledList>
                        {/* Required columns */}
                        {requiredColumns.length > 0 && (
                            <>
                                <ListSubheader>Required Columns</ListSubheader>
                                {requiredColumns.map((column) => (
                                    <ListItem key={column.id} dense>
                                        <FormControlLabel
                                            control={
                                                <Checkbox checked={true} disabled size="small" />
                                            }
                                            label={
                                                labelFormatter
                                                    ? labelFormatter(column)
                                                    : column.label
                                            }
                                            sx={{ margin: 0, width: '100%', opacity: 0.6 }}
                                        />
                                    </ListItem>
                                ))}
                                {(filteredGroups.length > 0 || ungroupedColumns.length > 0) && (
                                    <Divider />
                                )}
                            </>
                        )}

                        {/* Grouped columns */}
                        {filteredGroups.map((group, groupIndex) => {
                            const groupColumns = filterColumns(getColumnsByGroup(group.id))
                            if (groupColumns.length === 0) return null

                            return (
                                <div key={group.id}>
                                    <ListSubheader>{group.label}</ListSubheader>
                                    <ListItem>
                                        <ToggleButtons
                                            onSelectAll={() => toggleGroup(group.columns, true)}
                                            onHideAll={() => toggleGroup(group.columns, false)}
                                        />
                                    </ListItem>
                                    {groupColumns.map((column) => (
                                        <ColumnCheckbox
                                            key={column.id}
                                            column={column}
                                            isColumnVisible={isColumnVisible}
                                            toggleColumn={toggleColumn}
                                            labelFormatter={labelFormatter}
                                        />
                                    ))}
                                    {groupIndex < filteredGroups.length - 1 && <Divider />}
                                </div>
                            )
                        })}

                        {/* Ungrouped columns */}
                        {ungroupedColumns.length > 0 && (
                            <>
                                {filteredGroups.length > 0 && (
                                    <>
                                        <Divider />
                                        <ListSubheader>Other Columns</ListSubheader>
                                    </>
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
                            </>
                        )}
                    </StyledList>
                </Paper>
            </StyledPopover>
        </div>
    )
}

export default ColumnVisibilityDropdown
