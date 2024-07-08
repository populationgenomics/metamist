import React, { useState } from 'react'
import { Dropdown, Table as SUITable } from 'semantic-ui-react'
import Table from '../../shared/components/Table'
import { ProjectInsightsDetails } from '../../sm-api'
import { ColumnKey, DETAILS_TABLE_HEADER_CELL_CONFIGS, HeaderCell } from './HeaderCell'

interface DetailsTableProps {
    filteredData: ProjectInsightsDetails[]
    handleSelectionChange: (columnName: ColumnKey, selectedOptions: string[]) => void
    getUniqueOptionsForColumn: (key: ColumnKey) => string[]
    getSelectedOptionsForColumn: (key: ColumnKey) => string[]
}

const getRowClassName = (sequencingType: string) => {
    // This function is used to determine the row color based on the sequencing type
    switch (sequencingType) {
        case 'exome':
            return 'exome-row'
        case 'genome':
            return 'genome-row'
        default:
            return 'rna-row'
    }
}

const getCellValue = (details: ProjectInsightsDetails, key: ColumnKey): React.ReactNode => {
    if (key === 'stripy' || key === 'mito') {
        const report = details.web_reports?.[key]
        return report ? <a href={(report as { url: string }).url}>Link</a> : 'N/A'
    }

    const value = details[key as keyof ProjectInsightsDetails]

    if (typeof value === 'boolean') {
        return value ? '✅' : '❌'
    }

    return value
}

const DetailsTableRow: React.FC<{ details: ProjectInsightsDetails }> = ({ details }) => {
    const rowClassName = getRowClassName(details.sequencing_type)
    return (
        <SUITable.Row key={`${details.sequencing_group_id}`} className={rowClassName}>
            {DETAILS_TABLE_HEADER_CELL_CONFIGS.map((config) => (
                <SUITable.Cell
                    key={config.key}
                    data-cell
                    className={
                        config.key === 'dataset' || config.key === 'sequencing_type'
                            ? 'category-cell'
                            : 'table-cell'
                    }
                >
                    {getCellValue(details, config.key)}
                </SUITable.Cell>
            ))}
        </SUITable.Row>
    )
}

const DetailsTable: React.FC<DetailsTableProps> = ({
    filteredData,
    handleSelectionChange,
    getUniqueOptionsForColumn,
    getSelectedOptionsForColumn,
}) => {
    const [sortColumns, setSortColumns] = useState<
        Array<{ column: ColumnKey; direction: 'ascending' | 'descending' }>
    >([])

    const handleSort = (column: ColumnKey, isMultiSort: boolean) => {
        if (isMultiSort) {
            const existingColumnIndex = sortColumns.findIndex(
                (sortColumn) => sortColumn.column === column
            )

            if (existingColumnIndex !== -1) {
                const updatedSortColumns = [...sortColumns]
                updatedSortColumns[existingColumnIndex].direction =
                    updatedSortColumns[existingColumnIndex].direction === 'ascending'
                        ? 'descending'
                        : 'ascending'
                setSortColumns(updatedSortColumns)
            } else {
                setSortColumns([...sortColumns, { column, direction: 'ascending' }])
            }
        } else {
            if (sortColumns.length === 1 && sortColumns[0].column === column) {
                setSortColumns([
                    {
                        column,
                        direction:
                            sortColumns[0].direction === 'ascending' ? 'descending' : 'ascending',
                    },
                ])
            } else {
                setSortColumns([{ column, direction: 'ascending' }])
            }
        }
    }

    const sortedData = React.useMemo(() => {
        const data = [...filteredData]
        data.sort((a, b) => {
            for (const { column, direction } of sortColumns) {
                const valueA = a[column as keyof ProjectInsightsDetails]
                const valueB = b[column as keyof ProjectInsightsDetails]
                if (valueA === valueB) continue
                if (typeof valueA === 'number' && typeof valueB === 'number') {
                    return direction === 'ascending' ? valueA - valueB : valueB - valueA
                } else {
                    return direction === 'ascending'
                        ? String(valueA).localeCompare(String(valueB))
                        : String(valueB).localeCompare(String(valueA))
                }
            }
            return 0
        })
        return data
    }, [filteredData, sortColumns])

    const sortDirectionMap = React.useMemo(() => {
        const map = new Map<string, 'ascending' | 'descending' | undefined>()
        sortColumns.forEach(({ column, direction }) => {
            map.set(column, direction)
        })
        return map
    }, [sortColumns])

    const exportToFile = (format: 'csv' | 'tsv') => {
        const headerData = DETAILS_TABLE_HEADER_CELL_CONFIGS.map((config) => config.label)
        const rowData = sortedData.map((details) =>
            DETAILS_TABLE_HEADER_CELL_CONFIGS.map((config) => {
                const value = getCellValue(details, config.key)
                if (value === '✅') return 'TRUE'
                if (value === '❌') return 'FALSE'
                if (React.isValidElement(value) && value.type === 'a')
                    return (value.props as any).href
                return String(value)
            })
        )

        const separator = format === 'csv' ? ',' : '\t'
        const fileData = [headerData, ...rowData].map((row) => row.join(separator)).join('\n')
        const blob = new Blob([fileData], { type: `text/${format}` })
        const url = URL.createObjectURL(blob)
        const link = document.createElement('a')
        link.href = url
        const currentDate = new Date().toISOString().slice(0, 19)
        const fileName = `project_insights_details_${currentDate}.${format}`
        link.setAttribute('download', fileName)
        document.body.appendChild(link)
        link.click()
        document.body.removeChild(link)
    }

    const exportOptions = [
        { key: 'csv', text: 'Export to CSV', value: 'csv' },
        { key: 'tsv', text: 'Export to TSV', value: 'tsv' },
    ]

    return (
        <div>
            <div style={{ textAlign: 'right' }}>
                <Dropdown
                    button
                    className="icon"
                    floating
                    labeled
                    icon="download"
                    options={exportOptions}
                    text="Export"
                    onChange={(_, data) => exportToFile(data.value as 'csv' | 'tsv')}
                />
            </div>
            <Table sortable id="project-insights-details-table">
                <SUITable.Header>
                    <SUITable.Row>
                        {DETAILS_TABLE_HEADER_CELL_CONFIGS.map((config) => (
                            <HeaderCell
                                key={config.key}
                                config={config}
                                sortDirection={sortDirectionMap.get(config.key)}
                                onSort={handleSort}
                                onFilter={handleSelectionChange}
                                getUniqueOptionsForColumn={getUniqueOptionsForColumn}
                                selectedOptions={getSelectedOptionsForColumn(config.key)}
                            />
                        ))}
                    </SUITable.Row>
                </SUITable.Header>
                <SUITable.Body>
                    {sortedData.map((details) => (
                        <DetailsTableRow
                            data-row
                            key={`${details.sequencing_group_id}`}
                            details={details}
                        />
                    ))}
                </SUITable.Body>
            </Table>
        </div>
    )
}

export default DetailsTable
