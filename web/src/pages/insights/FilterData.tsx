import { useCallback, useMemo, useState } from 'react'
import { ColumnKey } from './HeaderCell'

interface FilterState {
    [key: string]: Set<string>
}

interface GeneralFilteringResult<T> {
    filteredData: T[]
    updateFilter: (columnName: ColumnKey, selectedOptions: string[]) => void
    getUniqueOptionsForColumn: (key: ColumnKey) => string[]
    getSelectedOptionsForColumn: (key: ColumnKey) => string[]
}

function filterData<T>(allData: T[]): GeneralFilteringResult<T> {
    const [filterState, setFilterState] = useState<FilterState>({})

    const filteredData = useMemo(() => {
        return allData.filter((item) =>
            Object.entries(filterState).every(([key, values]) => {
                if (values.size === 0) return true
                const itemValue = item[key as keyof T]
                if (key === 'stripy' || key === 'mito') {
                    const report = (item as any).web_reports?.[key]
                    return values.has(report ? 'Yes' : 'No')
                }
                if (typeof itemValue === 'boolean') {
                    return values.has(itemValue ? 'Yes' : 'No')
                }
                if (Array.isArray(itemValue)) {
                    return itemValue.some((v) => values.has(String(v)))
                } else {
                    return values.has(String(itemValue))
                }
            })
        )
    }, [allData, filterState])

    const getUniqueOptionsForColumn = useCallback(
        (key: ColumnKey): string[] => {
            const uniqueValues = new Set<string>()
            allData.forEach((item) => {
                if (
                    Object.entries(filterState).every(
                        ([filterKey, filterValues]) =>
                            filterKey === key || // Skip the current column's filter
                            filterValues.size === 0 || // Skip if no filter is applied
                            filterValues.has(String(item[filterKey as keyof T]))
                    )
                ) {
                    if (key === 'stripy' || key === 'mito') {
                        const report = (item as any).web_reports?.[key]
                        uniqueValues.add(report ? 'Yes' : 'No')
                    } else {
                        const value = item[key as keyof T]
                        if (typeof value === 'boolean') {
                            uniqueValues.add(value ? 'Yes' : 'No')
                        } else if (Array.isArray(value)) {
                            value.forEach((v) => uniqueValues.add(String(v)))
                        } else {
                            uniqueValues.add(String(value))
                        }
                    }
                }
            })
            return Array.from(uniqueValues)
        },
        [allData, filterState]
    )

    const updateFilter = useCallback((columnName: ColumnKey, selectedOptions: string[]) => {
        setFilterState((prevState) => ({
            ...prevState,
            [columnName]: new Set(selectedOptions),
        }))
    }, [])

    const getSelectedOptionsForColumn = useCallback(
        (key: ColumnKey): string[] => {
            return Array.from(filterState[key] || [])
        },
        [filterState]
    )

    return {
        filteredData,
        updateFilter,
        getUniqueOptionsForColumn,
        getSelectedOptionsForColumn,
    }
}

export default filterData
