import React from 'react'
import { ColumnConfig, ColumnGroup, ColumnVisibilityDropdown } from '../ColumnVisibilityDropdown'

// Example of how to use the ColumnVisibilityDropdown component
const ExampleUsage: React.FC = () => {
    const [visibleColumns, setVisibleColumns] = React.useState<Set<string>>(
        new Set(['id', 'name', 'jan', 'feb', 'mar'])
    )

    // Example column configuration for a billing table
    const columns: ColumnConfig[] = [
        { id: 'id', label: 'ID', isRequired: true },
        { id: 'name', label: 'Name', isRequired: true },
        { id: 'jan', label: 'January 2024', group: 'months' },
        { id: 'feb', label: 'February 2024', group: 'months' },
        { id: 'mar', label: 'March 2024', group: 'months' },
        { id: 'apr', label: 'April 2024', group: 'months' },
        { id: 'compute', label: 'Compute Cost', group: 'costs' },
        { id: 'storage', label: 'Storage Cost', group: 'costs' },
        { id: 'total', label: 'Total Cost', group: 'costs' },
    ]

    // Example column groups
    const groups: ColumnGroup[] = [
        { id: 'months', label: 'Monthly Data', columns: ['jan', 'feb', 'mar', 'apr'] },
        { id: 'costs', label: 'Cost Breakdown', columns: ['compute', 'storage', 'total'] },
    ]

    // Custom label formatter example
    const formatLabel = (column: ColumnConfig): string => {
        if (column.group === 'months') {
            return `📅 ${column.label}`
        }
        if (column.group === 'costs') {
            return `💰 ${column.label}`
        }
        return column.label
    }

    return (
        <div style={{ padding: '20px' }}>
            <h2>Column Visibility Dropdown Example</h2>

            <div style={{ marginBottom: '20px' }}>
                <ColumnVisibilityDropdown
                    columns={columns}
                    groups={groups}
                    visibleColumns={visibleColumns}
                    onVisibilityChange={setVisibleColumns}
                    labelFormatter={formatLabel}
                />
            </div>

            <h3>Currently Visible Columns:</h3>
            <ul>
                {Array.from(visibleColumns).map((columnId) => {
                    const column = columns.find((c) => c.id === columnId)
                    return <li key={columnId}>{column?.label || columnId}</li>
                })}
            </ul>
        </div>
    )
}

export default ExampleUsage
