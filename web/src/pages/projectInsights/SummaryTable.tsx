// projectInsights/SummaryTable.tsx
import React, { useState } from 'react'
import { ProjectInsightsSummary } from '../../sm-api'
import { Icon, Table } from 'semantic-ui-react'
import Tooltip, { TooltipProps } from '@mui/material/Tooltip'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import FilterButton from './FilterButton'

interface SummaryTableProps {
    allData: ProjectInsightsSummary[]
    filteredData: ProjectInsightsSummary[]
    selectedProjects: { name: string }[]
    selectedSeqTypes: string[]
    selectedSeqTechnologies: string[]
    handleSelectionChange: (columnName: string, selectedOptions: string[]) => void
}

function getPercentageColor(percentage: number, isDarkMode: boolean) {
    const hue = (percentage / 100) * 120 // Convert percentage to hue value (0-120)
    const saturation = isDarkMode ? '100%' : '90%' // Set saturation based on mode
    const lightness = isDarkMode ? '25%' : '75%' // Set lightness based on mode

    return `hsl(${hue}, ${saturation}, ${lightness})`
}

const HtmlTooltip = (props: TooltipProps) => (
    <Tooltip {...props} classes={{ popper: 'html-tooltip' }} />
)

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

const SummaryTableRow: React.FC<{ summary: ProjectInsightsSummary }> = ({ summary }) => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'
    const percentageAligned =
        summary.total_sequencing_groups > 0
            ? (summary.total_crams / summary.total_sequencing_groups) * 100
            : 0

    const percentageInJointCall =
        summary.latest_annotate_dataset?.sg_count ?? 0 > 0
            ? ((summary.latest_annotate_dataset?.sg_count ?? 0) / summary.total_sequencing_groups) *
              100
            : 0
    const percentageInSnvIndex =
        summary.latest_snv_es_index?.sg_count ?? 0 > 0
            ? ((summary.latest_snv_es_index?.sg_count ?? 0) / summary.total_sequencing_groups) * 100
            : 0
    const percentageInSvIndex =
        summary.latest_sv_es_index?.sg_count ?? 0 > 0
            ? ((summary.latest_sv_es_index?.sg_count ?? 0) / summary.total_sequencing_groups) * 100
            : 0

    const rowClassName = getRowClassName(summary.sequencing_type)

    return (
        <Table.Row key={`${summary.project}-${summary.sequencing_type}`} className={rowClassName}>
            <Table.Cell data-cell className="category-cell">
                {summary.dataset}
            </Table.Cell>
            <Table.Cell data-cell className="category-cell">
                {summary.sequencing_type}
            </Table.Cell>
            <Table.Cell className="table-cell">{summary.sequencing_technology}</Table.Cell>
            <Table.Cell className="table-cell">{summary.total_families}</Table.Cell>
            <Table.Cell className="table-cell">{summary.total_participants}</Table.Cell>
            <Table.Cell className="table-cell">{summary.total_samples}</Table.Cell>
            <Table.Cell className="table-cell">{summary.total_sequencing_groups}</Table.Cell>
            <Table.Cell className="table-cell">{summary.total_crams}</Table.Cell>
            <Table.Cell
                className="table-cell"
                style={{
                    textAlign: 'center',
                    backgroundColor: getPercentageColor(percentageAligned, isDarkMode),
                }}
            >
                <div style={{ display: 'flex', justifyContent: 'center' }}>
                    <HtmlTooltip
                        title={
                            <p>
                                {summary.total_crams} / {summary.total_sequencing_groups} <br />
                                Sequencing Groups Aligned
                            </p>
                        }
                    >
                        <div>{percentageAligned.toFixed(2)}%</div>
                    </HtmlTooltip>
                </div>
            </Table.Cell>
            <Table.Cell
                className="table-cell"
                style={{
                    textAlign: 'center',
                    backgroundColor: getPercentageColor(percentageInJointCall, isDarkMode),
                }}
            >
                <div style={{ display: 'flex', justifyContent: 'center' }}>
                    <HtmlTooltip
                        title={
                            <div>
                                <p>
                                    {summary.latest_annotate_dataset?.sg_count} /{' '}
                                    {summary.total_sequencing_groups} Sequencing Groups
                                </p>
                                <p>{summary.latest_annotate_dataset?.timestamp}</p>
                                <p>
                                    Latest <em>{summary.sequencing_type}</em> AnnotateDataset <br />
                                    Analysis ID: {summary.latest_annotate_dataset?.id}
                                </p>
                            </div>
                        }
                    >
                        <div>{percentageInJointCall.toFixed(2)}%</div>
                    </HtmlTooltip>
                </div>
            </Table.Cell>
            <Table.Cell
                className="table-cell"
                style={{
                    textAlign: 'center',
                    backgroundColor: getPercentageColor(percentageInSnvIndex, isDarkMode),
                }}
            >
                <div style={{ display: 'flex', justifyContent: 'center' }}>
                    <HtmlTooltip
                        title={
                            <div>
                                <p>
                                    {summary.latest_snv_es_index?.sg_count} /{' '}
                                    {summary.total_sequencing_groups} Sequencing Groups
                                </p>
                                <p>
                                    Latest <em>{summary.sequencing_type}</em> SNV Index <br />
                                    {summary.latest_snv_es_index?.timestamp}
                                </p>
                                <p>
                                    Analysis ID: {summary.latest_snv_es_index?.id} <br />
                                    {summary.latest_snv_es_index?.name}
                                </p>
                            </div>
                        }
                    >
                        <div>{percentageInSnvIndex.toFixed(2)}%</div>
                    </HtmlTooltip>
                </div>
            </Table.Cell>
            <Table.Cell
                className="table-cell"
                style={{
                    textAlign: 'center',
                    backgroundColor: getPercentageColor(percentageInSvIndex, isDarkMode),
                }}
            >
                <div style={{ display: 'flex', justifyContent: 'center' }}>
                    <HtmlTooltip
                        title={
                            <div>
                                <p>
                                    {summary.latest_sv_es_index?.sg_count} /{' '}
                                    {summary.total_sequencing_groups} Sequencing Groups
                                </p>
                                <p>
                                    Latest <em>{summary.sequencing_type}</em> SV Index <br />
                                    {summary.latest_sv_es_index?.timestamp}
                                </p>
                                <p>
                                    Analysis ID: {summary.latest_sv_es_index?.id} <br />
                                    {summary.latest_sv_es_index?.name} <br />
                                </p>
                            </div>
                        }
                    >
                        <div>{percentageInSvIndex.toFixed(2)}%</div>
                    </HtmlTooltip>
                </div>
            </Table.Cell>
        </Table.Row>
    )
}

const SummaryTable: React.FC<SummaryTableProps> = ({
    allData,
    filteredData,
    selectedProjects,
    selectedSeqTypes,
    selectedSeqTechnologies,
    handleSelectionChange,
}) => {
    const [sortColumns, setSortColumns] = useState<
        Array<{ column: keyof ProjectInsightsSummary; direction: 'ascending' | 'descending' }>
    >([])
    const handleSort = (column: keyof ProjectInsightsSummary, isMultiSort: boolean) => {
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
                const valueA = a[column]
                const valueB = b[column]
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

    const getUniqueOptionsForColumn = (columnName: keyof ProjectInsightsSummary) => {
        const filteredDataExcludingCurrentColumn = allData.filter((item) => {
            return (
                selectedProjects.some((p) => p.name === item.dataset) &&
                selectedSeqTypes.includes(item.sequencing_type) &&
                (columnName === 'sequencing_technology' ||
                    selectedSeqTechnologies.length === 0 ||
                    selectedSeqTechnologies.includes(item.sequencing_technology))
            )
        })

        let uniqueOptions: string[] = []
        switch (columnName) {
            case 'sequencing_technology':
                uniqueOptions = Array.from(
                    new Set(
                        filteredDataExcludingCurrentColumn.map(
                            (item) => item[columnName]?.toString() || ''
                        )
                    )
                )
                break
            default:
                uniqueOptions = Array.from(
                    new Set(filteredDataExcludingCurrentColumn.map((item) => item[columnName]))
                ).map((option) => option?.toString() || '')
        }

        return uniqueOptions
    }

    return (
        <div>
            <Table sortable>
                <Table.Header>
                    <Table.Row>
                        <Table.HeaderCell
                            className="header-cell"
                            sorted={
                                sortColumns.find((column) => column.column === 'dataset')?.direction
                            }
                            onClick={(event: React.MouseEvent<HTMLElement>) =>
                                handleSort('dataset', event.shiftKey)
                            }
                        >
                            Dataset
                        </Table.HeaderCell>
                        <Table.HeaderCell
                            className="header-cell"
                            sorted={
                                sortColumns.find((column) => column.column === 'sequencing_type')
                                    ?.direction
                            }
                            onClick={(event: React.MouseEvent<HTMLElement>) =>
                                handleSort('sequencing_type', event.shiftKey)
                            }
                        >
                            Seq Type
                        </Table.HeaderCell>
                        <Table.HeaderCell
                            className="header-cell"
                            onClick={(event: React.MouseEvent<HTMLElement>) =>
                                handleSort('sequencing_technology', event.shiftKey)
                            }
                        >
                            <div className="filter-button">
                                <FilterButton
                                    columnName="Technology"
                                    options={getUniqueOptionsForColumn('sequencing_technology')}
                                    selectedOptions={selectedSeqTechnologies}
                                    onSelectionChange={(selectedOptions) =>
                                        handleSelectionChange(
                                            'sequencing_technology',
                                            selectedOptions
                                        )
                                    }
                                />
                            </div>
                            {sortColumns.find(
                                (column) => column.column === 'sequencing_technology'
                            ) && (
                                <Icon
                                    name={
                                        sortColumns.find(
                                            (column) => column.column === 'sequencing_technology'
                                        )?.direction === 'ascending'
                                            ? 'caret up'
                                            : 'caret down'
                                    }
                                    className="sort-icon"
                                />
                            )}
                            <div className="header-text">Technology</div>
                        </Table.HeaderCell>
                        <Table.HeaderCell
                            className="header-cell"
                            onClick={(event: React.MouseEvent<HTMLElement>) =>
                                handleSort('total_families', event.shiftKey)
                            }
                        >
                            {sortColumns.find((column) => column.column === 'total_families') && (
                                <Icon
                                    name={
                                        sortColumns.find(
                                            (column) => column.column === 'total_families'
                                        )?.direction === 'ascending'
                                            ? 'caret up'
                                            : 'caret down'
                                    }
                                    className="sort-icon"
                                />
                            )}
                            <div className="header-text">Families</div>
                        </Table.HeaderCell>
                        <Table.HeaderCell
                            className="header-cell"
                            onClick={(event: React.MouseEvent<HTMLElement>) =>
                                handleSort('total_participants', event.shiftKey)
                            }
                        >
                            {sortColumns.find(
                                (column) => column.column === 'total_participants'
                            ) && (
                                <Icon
                                    name={
                                        sortColumns.find(
                                            (column) => column.column === 'total_participants'
                                        )?.direction === 'ascending'
                                            ? 'caret up'
                                            : 'caret down'
                                    }
                                    className="sort-icon"
                                />
                            )}
                            <div className="header-text">Participants</div>
                        </Table.HeaderCell>
                        <Table.HeaderCell
                            className="header-cell"
                            onClick={(event: React.MouseEvent<HTMLElement>) =>
                                handleSort('total_samples', event.shiftKey)
                            }
                        >
                            {sortColumns.find((column) => column.column === 'total_samples') && (
                                <Icon
                                    name={
                                        sortColumns.find(
                                            (column) => column.column === 'total_samples'
                                        )?.direction === 'ascending'
                                            ? 'caret up'
                                            : 'caret down'
                                    }
                                    className="sort-icon"
                                />
                            )}
                            <div className="header-text">Samples</div>
                        </Table.HeaderCell>
                        <Table.HeaderCell
                            className="header-cell"
                            onClick={(event: React.MouseEvent<HTMLElement>) =>
                                handleSort('total_sequencing_groups', event.shiftKey)
                            }
                        >
                            {sortColumns.find(
                                (column) => column.column === 'total_sequencing_groups'
                            ) && (
                                <Icon
                                    name={
                                        sortColumns.find(
                                            (column) => column.column === 'total_sequencing_groups'
                                        )?.direction === 'ascending'
                                            ? 'caret up'
                                            : 'caret down'
                                    }
                                    className="sort-icon"
                                />
                            )}
                            <div className="header-text">Sequencing Groups</div>
                        </Table.HeaderCell>
                        <Table.HeaderCell
                            className="header-cell"
                            onClick={(event: React.MouseEvent<HTMLElement>) =>
                                handleSort('total_crams', event.shiftKey)
                            }
                        >
                            {sortColumns.find((column) => column.column === 'total_crams') && (
                                <Icon
                                    name={
                                        sortColumns.find(
                                            (column) => column.column === 'total_crams'
                                        )?.direction === 'ascending'
                                            ? 'caret up'
                                            : 'caret down'
                                    }
                                    className="sort-icon"
                                />
                            )}
                            <div className="header-text">CRAMs</div>
                        </Table.HeaderCell>
                        <Table.HeaderCell
                            className="header-cell collapsible-header"
                            onMouseEnter={(event: React.MouseEvent<HTMLElement>) => {
                                event.currentTarget.classList.add('expanded')
                            }}
                            onMouseLeave={(event: React.MouseEvent<HTMLElement>) => {
                                event.currentTarget.classList.remove('expanded')
                            }}
                        >
                            <div style={{ display: 'flex', justifyContent: 'center' }}>
                                <HtmlTooltip
                                    title={
                                        <p>
                                            Percentage of Sequencing Groups with a Completed CRAM
                                            Analysis
                                        </p>
                                    }
                                >
                                    <div className="header-text">% Aligned</div>
                                </HtmlTooltip>
                            </div>
                        </Table.HeaderCell>
                        <Table.HeaderCell
                            className="header-cell collapsible-header"
                            onMouseEnter={(event: React.MouseEvent<HTMLElement>) => {
                                event.currentTarget.classList.add('more-expanded')
                            }}
                            onMouseLeave={(event: React.MouseEvent<HTMLElement>) => {
                                event.currentTarget.classList.remove('more-expanded')
                            }}
                        >
                            <div style={{ display: 'flex', justifyContent: 'center' }}>
                                <HtmlTooltip
                                    title={
                                        <p>
                                            Percentage of Sequencing Groups in the latest
                                            AnnotateDataset Analysis
                                        </p>
                                    }
                                >
                                    <div className="header-text">% in Annotated Dataset</div>
                                </HtmlTooltip>
                            </div>
                        </Table.HeaderCell>
                        <Table.HeaderCell
                            className="header-cell collapsible-header"
                            onMouseEnter={(event: React.MouseEvent<HTMLElement>) => {
                                event.currentTarget.classList.add('more-expanded')
                            }}
                            onMouseLeave={(event: React.MouseEvent<HTMLElement>) => {
                                event.currentTarget.classList.remove('more-expanded')
                            }}
                        >
                            <div style={{ display: 'flex', justifyContent: 'center' }}>
                                <HtmlTooltip
                                    title={
                                        <p>
                                            Percentage of Sequencing Groups in the latest SNV
                                            ES-Index Analysis
                                        </p>
                                    }
                                >
                                    <div className="header-text">% in SNV ES-Index</div>
                                </HtmlTooltip>
                            </div>
                        </Table.HeaderCell>
                        <Table.HeaderCell
                            className="header-cell collapsible-header"
                            onMouseEnter={(event: React.MouseEvent<HTMLElement>) => {
                                event.currentTarget.classList.add('more-expanded')
                            }}
                            onMouseLeave={(event: React.MouseEvent<HTMLElement>) => {
                                event.currentTarget.classList.remove('more-expanded')
                            }}
                        >
                            <div style={{ display: 'flex', justifyContent: 'center' }}>
                                <HtmlTooltip
                                    title={
                                        <p>
                                            Percentage of Sequencing Groups in the latest SV
                                            (genome) or gCNV (exome) ES-Index Analysis
                                        </p>
                                    }
                                >
                                    <div className="header-text">% in SV ES-Index</div>
                                </HtmlTooltip>
                            </div>
                        </Table.HeaderCell>
                    </Table.Row>
                </Table.Header>
                <Table.Body>
                    {sortedData.map((summary) => (
                        <SummaryTableRow
                            key={`${summary.dataset}-${summary.sequencing_type}-${summary.sequencing_technology}`}
                            summary={summary}
                        />
                    ))}
                </Table.Body>
                <Table.Footer>
                    <Table.Row className="grand-total-row" key="grandTotals">
                        <Table.Cell className="table-cell">Grand Total</Table.Cell>
                        <Table.Cell className="table-cell">{sortedData.length} entries</Table.Cell>
                        <Table.Cell className="table-cell"></Table.Cell>
                        <Table.Cell className="table-cell">
                            {filteredData.reduce((acc, curr) => acc + curr.total_families, 0)}
                        </Table.Cell>
                        <Table.Cell className="table-cell">
                            {filteredData.reduce((acc, curr) => acc + curr.total_participants, 0)}
                        </Table.Cell>
                        <Table.Cell className="table-cell">
                            {filteredData.reduce((acc, curr) => acc + curr.total_samples, 0)}
                        </Table.Cell>
                        <Table.Cell className="table-cell">
                            {filteredData.reduce(
                                (acc, curr) => acc + curr.total_sequencing_groups,
                                0
                            )}
                        </Table.Cell>
                        <Table.Cell className="table-cell">
                            {filteredData.reduce((acc, curr) => acc + curr.total_crams, 0)}
                        </Table.Cell>
                        <Table.Cell className="table-cell">
                            <div style={{ display: 'flex', justifyContent: 'center' }}>
                                <HtmlTooltip
                                    title={
                                        <p>
                                            {filteredData.reduce(
                                                (acc, curr) => acc + curr.total_crams,
                                                0
                                            )}{' '}
                                            /{' '}
                                            {filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            )}{' '}
                                            Total Sequencing Groups with a Completed CRAM Analysis
                                        </p>
                                    }
                                >
                                    <div>
                                        {(
                                            (filteredData.reduce(
                                                (acc, curr) => acc + curr.total_crams,
                                                0
                                            ) /
                                                filteredData.reduce(
                                                    (acc, curr) =>
                                                        acc + curr.total_sequencing_groups,
                                                    0
                                                )) *
                                                100 || 0
                                        ).toFixed(2)}
                                        %
                                    </div>
                                </HtmlTooltip>
                            </div>
                        </Table.Cell>
                        <Table.Cell className="table-cell">
                            <div style={{ display: 'flex', justifyContent: 'center' }}>
                                <HtmlTooltip
                                    title={
                                        <p>
                                            {filteredData.reduce(
                                                (acc, curr) =>
                                                    acc +
                                                    (curr.latest_annotate_dataset?.sg_count ?? 0),
                                                0
                                            )}{' '}
                                            /{' '}
                                            {filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            )}{' '}
                                            Total Sequencing Groups in the latest AnnotateDataset
                                            analysis
                                        </p>
                                    }
                                >
                                    <div>
                                        {(
                                            (filteredData.reduce(
                                                (acc, curr) =>
                                                    acc +
                                                    (curr.latest_annotate_dataset?.sg_count ?? 0),
                                                0
                                            ) /
                                                filteredData.reduce(
                                                    (acc, curr) =>
                                                        acc + curr.total_sequencing_groups,
                                                    0
                                                )) *
                                                100 || 0
                                        ).toFixed(2)}
                                        %
                                    </div>
                                </HtmlTooltip>
                            </div>
                        </Table.Cell>
                        <Table.Cell className="table-cell">
                            <div style={{ display: 'flex', justifyContent: 'center' }}>
                                <HtmlTooltip
                                    title={
                                        <p>
                                            {filteredData.reduce(
                                                (acc, curr) =>
                                                    acc + (curr.latest_snv_es_index?.sg_count ?? 0),
                                                0
                                            )}{' '}
                                            /{' '}
                                            {filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            )}{' '}
                                            Total Sequencing Groups in the latest SNV Elasticsearch
                                            Index
                                        </p>
                                    }
                                >
                                    <div>
                                        {(
                                            (filteredData.reduce(
                                                (acc, curr) =>
                                                    acc + (curr.latest_snv_es_index?.sg_count ?? 0),
                                                0
                                            ) /
                                                filteredData.reduce(
                                                    (acc, curr) =>
                                                        acc + curr.total_sequencing_groups,
                                                    0
                                                )) *
                                                100 || 0
                                        ).toFixed(2)}
                                        %
                                    </div>
                                </HtmlTooltip>
                            </div>
                        </Table.Cell>
                        <Table.Cell className="table-cell">
                            <div style={{ display: 'flex', justifyContent: 'center' }}>
                                <HtmlTooltip
                                    title={
                                        <p>
                                            {filteredData.reduce(
                                                (acc, curr) =>
                                                    acc + (curr.latest_sv_es_index?.sg_count ?? 0),
                                                0
                                            )}{' '}
                                            /{' '}
                                            {filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            )}{' '}
                                            Total Sequencing Groups in the latest SV Elasticsearch
                                            Index
                                        </p>
                                    }
                                >
                                    <div>
                                        {(
                                            (filteredData.reduce(
                                                (acc, curr) =>
                                                    acc + (curr.latest_sv_es_index?.sg_count ?? 0),
                                                0
                                            ) /
                                                filteredData.reduce(
                                                    (acc, curr) =>
                                                        acc + curr.total_sequencing_groups,
                                                    0
                                                )) *
                                                100 || 0
                                        ).toFixed(2)}
                                        %
                                    </div>
                                </HtmlTooltip>
                            </div>
                        </Table.Cell>
                    </Table.Row>
                </Table.Footer>
            </Table>
        </div>
    )
}

export default SummaryTable
