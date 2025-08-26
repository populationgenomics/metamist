import { useMemo } from 'react'
import { ColumnConfig } from './ColumnVisibilityDropdown'

/**
 * Hook to help with column visibility management for export functions
 */
export const useColumnVisibility = (columns: ColumnConfig[], visibleColumns: Set<string>) => {
    // Get visible column IDs in order
    const visibleColumnIds = useMemo(() => {
        return columns
            .filter((col) => col.isRequired || visibleColumns.has(col.id))
            .map((col) => col.id)
    }, [columns, visibleColumns])

    // Get visible column labels in order
    const visibleColumnLabels = useMemo(() => {
        return columns
            .filter((col) => col.isRequired || visibleColumns.has(col.id))
            .map((col) => col.label)
    }, [columns, visibleColumns])

    // Check if a column is visible (helper function)
    const isColumnVisible = (columnId: string): boolean => {
        const column = columns.find((c) => c.id === columnId)
        if (column?.isRequired) {
            return true
        }
        return visibleColumns.has(columnId)
    }

    // Filter data based on visible columns
    const filterDataForExport = <T extends Record<string, unknown>>(
        data: T[],
        columnMapping?: Record<string, keyof T>
    ): string[][] => {
        return data.map((item) => {
            return visibleColumnIds.map((columnId) => {
                // Use column mapping if provided, otherwise use columnId directly
                const dataKey = columnMapping?.[columnId] || columnId
                const value = item[dataKey]
                return String(value ?? '')
            })
        })
    }

    return {
        visibleColumnIds,
        visibleColumnLabels,
        isColumnVisible,
        filterDataForExport,
    }
}
