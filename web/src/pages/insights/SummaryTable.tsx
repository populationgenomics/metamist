import React, { ReactNode, useState } from 'react'
import { Table as SUITable } from 'semantic-ui-react'
import Table from '../../shared/components/Table'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import HtmlTooltip from '../../shared/utilities/htmlTooltip'
import { ProjectInsightsSummary } from '../../sm-api'
import { ColumnKey, HeaderCell, summaryTableHeaderCellConfigs } from './HeaderCell'
import { FooterCell, footerCellConfigs } from './SummaryTableFooterCell'

interface SummaryTableProps {
    filteredData: ProjectInsightsSummary[]
    handleSelectionChange: (columnName: ColumnKey, selectedOptions: string[]) => void
    getUniqueOptionsForColumn: (key: ColumnKey) => string[]
    getSelectedOptionsForColumn: (key: ColumnKey) => string[]
}

interface PercentageCellProps {
    percentage: number
    tooltipContent: ReactNode
    isDarkMode: boolean
    getPercentageColor: (percentage: number, isDarkMode: boolean) => string
}

const PercentageCell: React.FC<PercentageCellProps> = ({
    percentage,
    tooltipContent,
    isDarkMode,
    getPercentageColor,
}) => (
    <SUITable.Cell
        className="percentage-cell"
        style={{ backgroundColor: getPercentageColor(percentage, isDarkMode) }}
    >
        <HtmlTooltip title={tooltipContent}>
            <div>{percentage.toFixed(2)}%</div>
        </HtmlTooltip>
    </SUITable.Cell>
)

function calculatePercentage(count: number, total: number): number {
    return count > 0 ? (count / total) * 100 : 0
}

function getPercentageColor(percentage: number, isDarkMode: boolean) {
    const hue = (percentage / 100) * 120 // Convert percentage to hue value (0-120)
    const saturation = isDarkMode ? '100%' : '90%' // Set saturation based on mode
    const lightness = isDarkMode ? '25%' : '75%' // Set lightness based on mode

    return `hsl(${hue}, ${saturation}, ${lightness})`
}

const getRowClassName = (sequencingType: string) => {
    switch (sequencingType) {
        case 'exome':
            return 'exome-row'
        case 'genome':
            return 'genome-row'
        default:
            return 'rna-row'
    }
}

const getSummaryCell = (
    summary: ProjectInsightsSummary,
    key: ColumnKey,
    isDarkMode: boolean
): React.ReactNode => {
    // What goes into each cell of the table is determined by the column header or 'key'
    // The percentage calculation cells are handled differently from the other cells
    const percentageAligned =
        summary.total_sequencing_groups > 0
            ? (summary.total_crams / summary.total_sequencing_groups) * 100
            : 0
    const alignedCellTooltip = (
        <p>
            {summary.total_crams} / {summary.total_sequencing_groups} Total Sequencing Groups with a
            Completed CRAM Analysis
        </p>
    )

    const latestAnnotateSgCount = summary.latest_annotate_dataset?.sg_count ?? 0
    const percentageInJointCall = calculatePercentage(
        latestAnnotateSgCount,
        summary.total_sequencing_groups
    )
    const inJointCallTooltip = (
        <p>
            {latestAnnotateSgCount} / {summary.total_sequencing_groups} Sequencing Groups
            <br />
            {summary.latest_annotate_dataset?.timestamp}
            <br />
            Latest {summary.sequencing_type} AnnotateDataset <br />
            Analysis ID: {summary.latest_annotate_dataset?.id}
        </p>
    )

    const latestSnvIndexSgCount = summary.latest_snv_es_index?.sg_count ?? 0
    const percentageInSnvIndex = calculatePercentage(
        latestSnvIndexSgCount,
        summary.total_sequencing_groups
    )
    const inSnvIndexTooltip = (
        <p>
            {latestSnvIndexSgCount} / {summary.total_sequencing_groups} Sequencing Groups
            <br />
            Latest {summary.sequencing_type} SNV Index <br />
            {summary.latest_snv_es_index?.timestamp}
            <br />
            Analysis ID: {summary.latest_snv_es_index?.id} <br />
            {summary.latest_snv_es_index?.name}
        </p>
    )

    const latestSvIndexSgCount = summary.latest_sv_es_index?.sg_count ?? 0
    const percentageInSvIndex = calculatePercentage(
        latestSvIndexSgCount,
        summary.total_sequencing_groups
    )
    const inSvIndexTooltip = (
        <p>
            {latestSvIndexSgCount} / {summary.total_sequencing_groups} Sequencing Groups
            <br />
            Latest {summary.sequencing_type} SV Index <br />
            {summary.latest_sv_es_index?.timestamp}
            <br />
            Analysis ID: {summary.latest_sv_es_index?.id} <br />
            {summary.latest_sv_es_index?.name} <br />
        </p>
    )

    switch (key) {
        case 'aligned_percentage':
            return (
                <PercentageCell
                    percentage={percentageAligned}
                    tooltipContent={alignedCellTooltip}
                    isDarkMode={isDarkMode}
                    getPercentageColor={getPercentageColor}
                ></PercentageCell>
            )
        case 'annotated_dataset_percentage':
            return (
                <PercentageCell
                    percentage={percentageInJointCall}
                    tooltipContent={inJointCallTooltip}
                    isDarkMode={isDarkMode}
                    getPercentageColor={getPercentageColor}
                ></PercentageCell>
            )
        case 'snv_index_percentage':
            return (
                <PercentageCell
                    percentage={percentageInSnvIndex}
                    tooltipContent={inSnvIndexTooltip}
                    isDarkMode={isDarkMode}
                    getPercentageColor={getPercentageColor}
                ></PercentageCell>
            )
        case 'sv_index_percentage':
            return (
                <PercentageCell
                    percentage={percentageInSvIndex}
                    tooltipContent={inSvIndexTooltip}
                    isDarkMode={isDarkMode}
                    getPercentageColor={getPercentageColor}
                ></PercentageCell>
            )
        default:
            return (
                <SUITable.Cell className="table-cell">
                    {summary[key as keyof ProjectInsightsSummary]}
                </SUITable.Cell>
            )
    }
}

const SummaryTableRow: React.FC<{ summary: ProjectInsightsSummary }> = ({ summary }) => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'
    const rowClassName = getRowClassName(summary.sequencing_type)
    return (
        <SUITable.Row
            key={`${summary.dataset}-${summary.sequencing_type}-${summary.sequencing_technology}`}
            className={rowClassName}
        >
            {summaryTableHeaderCellConfigs.map((config) =>
                getSummaryCell(summary, config.key, isDarkMode)
            )}
        </SUITable.Row>
    )
}

const SummaryTable: React.FC<SummaryTableProps> = ({
    filteredData,
    getUniqueOptionsForColumn,
    handleSelectionChange,
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
                const valueA = a[column as keyof ProjectInsightsSummary]
                const valueB = b[column as keyof ProjectInsightsSummary]
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

    return (
        <div>
            <Table sortable>
                <SUITable.Header>
                    <SUITable.Row>
                        {summaryTableHeaderCellConfigs.map((config) => (
                            <HeaderCell
                                key={config.key}
                                config={config}
                                sortDirection={
                                    sortColumns.find((column) => column.column === config.key)
                                        ?.direction
                                }
                                onSort={handleSort}
                                onFilter={handleSelectionChange}
                                getUniqueOptionsForColumn={getUniqueOptionsForColumn}
                                selectedOptions={getSelectedOptionsForColumn(config.key)}
                            />
                        ))}
                    </SUITable.Row>
                </SUITable.Header>
                <SUITable.Body>
                    {sortedData.map((summary) => (
                        <SummaryTableRow
                            key={`${summary.dataset}-${summary.sequencing_type}-${summary.sequencing_technology}`}
                            summary={summary}
                        />
                    ))}
                </SUITable.Body>
                <SUITable.Footer>
                    <SUITable.Row className="grand-total-row" key="grandTotals">
                        {footerCellConfigs.map((config) => (
                            <FooterCell key={config.key} config={config} data={filteredData} />
                        ))}
                    </SUITable.Row>
                </SUITable.Footer>
            </Table>
        </div>
    )
}

export default SummaryTable