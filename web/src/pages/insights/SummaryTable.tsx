import React, { ReactNode } from 'react'
import { Table as SUITable } from 'semantic-ui-react'
import Table from '../../shared/components/Table'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import HtmlTooltip from '../../shared/utilities/htmlTooltip'
import { ProjectInsightsSummary } from '../../sm-api'
import { ColumnKey, HeaderCell, SUMMARY_TABLE_HEADER_CELL_CONFIGS } from './HeaderCell'
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
}

const PercentageCell: React.FC<PercentageCellProps> = ({
    percentage,
    tooltipContent,
    isDarkMode,
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

function getRowClassName(sequencingType: string) {
    switch (sequencingType) {
        case 'exome':
            return 'exome-row'
        case 'genome':
            return 'genome-row'
        default:
            return 'rna-row'
    }
}

const GetAlignedPercentageCell: React.FC<{
    summary: ProjectInsightsSummary
    isDarkMode: boolean
}> = ({ summary, isDarkMode }) => {
    const percentage =
        summary.total_sequencing_groups > 0
            ? (summary.total_crams / summary.total_sequencing_groups) * 100
            : 0
    const tooltipContent = (
        <p>
            {summary.total_crams} / {summary.total_sequencing_groups} Total Sequencing Groups with a
            Completed CRAM Analysis
        </p>
    )
    return (
        <PercentageCell
            percentage={percentage}
            tooltipContent={tooltipContent}
            isDarkMode={isDarkMode}
        />
    )
}

const GetAnnotatedDatasetPercentageCell: React.FC<{
    summary: ProjectInsightsSummary
    isDarkMode: boolean
}> = ({ summary, isDarkMode }) => {
    const latestAnnotateSgCount = summary.latest_annotate_dataset?.sg_count ?? 0
    const percentage = calculatePercentage(latestAnnotateSgCount, summary.total_sequencing_groups)
    const tooltipContent = (
        <p>
            {latestAnnotateSgCount} / {summary.total_sequencing_groups} Sequencing Groups
            <br />
            {summary.latest_annotate_dataset?.timestamp}
            <br />
            Latest {summary.sequencing_type} AnnotateDataset <br />
            Analysis ID: {summary.latest_annotate_dataset?.id}
        </p>
    )
    return (
        <PercentageCell
            percentage={percentage}
            tooltipContent={tooltipContent}
            isDarkMode={isDarkMode}
        />
    )
}

const GetSnvIndexPercentageCell: React.FC<{
    summary: ProjectInsightsSummary
    isDarkMode: boolean
}> = ({ summary, isDarkMode }) => {
    const latestSnvIndexSgCount = summary.latest_snv_es_index?.sg_count ?? 0
    const percentage = calculatePercentage(latestSnvIndexSgCount, summary.total_sequencing_groups)
    const tooltipContent = (
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
    return (
        <PercentageCell
            percentage={percentage}
            tooltipContent={tooltipContent}
            isDarkMode={isDarkMode}
        />
    )
}

const GetSvIndexPercentageCell: React.FC<{
    summary: ProjectInsightsSummary
    isDarkMode: boolean
}> = ({ summary, isDarkMode }) => {
    const latestSvIndexSgCount = summary.latest_sv_es_index?.sg_count ?? 0
    const percentage = calculatePercentage(latestSvIndexSgCount, summary.total_sequencing_groups)
    const tooltipContent = (
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
    return (
        <PercentageCell
            percentage={percentage}
            tooltipContent={tooltipContent}
            isDarkMode={isDarkMode}
        />
    )
}

const GetSummaryCell: React.FC<{
    summary: ProjectInsightsSummary
    columnKey: ColumnKey
    isDarkMode: boolean
}> = ({ summary, columnKey, isDarkMode }) => {
    switch (columnKey) {
        case 'aligned_percentage':
            return <GetAlignedPercentageCell summary={summary} isDarkMode={isDarkMode} />
        case 'annotated_dataset_percentage':
            return <GetAnnotatedDatasetPercentageCell summary={summary} isDarkMode={isDarkMode} />
        case 'snv_index_percentage':
            return <GetSnvIndexPercentageCell summary={summary} isDarkMode={isDarkMode} />
        case 'sv_index_percentage':
            return <GetSvIndexPercentageCell summary={summary} isDarkMode={isDarkMode} />
        default:
            return (
                <SUITable.Cell className="table-cell">
                    {summary[columnKey as keyof ProjectInsightsSummary]}
                </SUITable.Cell>
            )
    }
}

const SummaryTableRow: React.FC<{ summary: ProjectInsightsSummary }> = ({ summary }) => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'
    const rowClassName = getRowClassName(summary.sequencing_type)
    return (
        <SUITable.Row className={rowClassName}>
            {SUMMARY_TABLE_HEADER_CELL_CONFIGS.map((config) => (
                <GetSummaryCell
                    key={config.key}
                    summary={summary}
                    columnKey={config.key}
                    isDarkMode={isDarkMode}
                />
            ))}
        </SUITable.Row>
    )
}

const SummaryTable: React.FC<SummaryTableProps> = ({
    filteredData,
    getUniqueOptionsForColumn,
    handleSelectionChange,
    getSelectedOptionsForColumn,
}) => {
    const [sortColumns, setSortColumns] = React.useState<
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

    const sortDirectionMap = React.useMemo(() => {
        const map = new Map<string, 'ascending' | 'descending' | undefined>()
        sortColumns.forEach(({ column, direction }) => {
            map.set(column, direction)
        })
        return map
    }, [sortColumns])

    return (
        <div>
            <Table sortable>
                <SUITable.Header>
                    <SUITable.Row>
                        {SUMMARY_TABLE_HEADER_CELL_CONFIGS.map((config) => (
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
                    {sortedData.map((summary: ProjectInsightsSummary) => (
                        <SummaryTableRow summary={summary} />
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
