import Tooltip, { TooltipProps } from '@mui/material/Tooltip'
import React from 'react'
import { Icon, Table as SUITable } from 'semantic-ui-react'
import FilterButton from './FilterButton' // Assuming you have this component

const HtmlTooltip = (props: TooltipProps) => (
    <Tooltip {...props} classes={{ popper: 'html-tooltip' }} />
)

interface SortColumn {
    column: string
    direction: 'ascending' | 'descending'
}

interface SummaryTableHeaderProps {
    sortColumns: SortColumn[]
    handleSort: (column: string, shiftKey: boolean) => void
    getUniqueOptionsForColumn: (column: string) => string[]
    selectedSeqTechnologies: string[]
    handleSelectionChange: (column: string, selectedOptions: string[]) => void
}

const SummaryTableHeader: React.FC<SummaryTableHeaderProps> = ({
    sortColumns,
    handleSort,
    getUniqueOptionsForColumn,
    selectedSeqTechnologies,
    handleSelectionChange,
}) => {
    const renderHeaderCell = (column: string, title: string, tooltipText: string = '') => {
        const sortColumn = sortColumns.find((col) => col.column === column)
        const isSortable = !!sortColumn

        return (
            <SUITable.HeaderCell
                className="header-cell"
                sorted={sortColumn?.direction}
                onClick={(event: React.MouseEvent<HTMLElement>) =>
                    handleSort(column, event.shiftKey)
                }
            >
                {isSortable && (
                    <Icon
                        name={sortColumn.direction === 'ascending' ? 'caret up' : 'caret down'}
                        className="sort-icon"
                    />
                )}
                {tooltipText ? (
                    <HtmlTooltip title={<p>{tooltipText}</p>}>
                        <div className="header-text">{title}</div>
                    </HtmlTooltip>
                ) : (
                    <div className="header-text">{title}</div>
                )}
            </SUITable.HeaderCell>
        )
    }

    return (
        <SUITable.Header>
            <SUITable.Row>
                {renderHeaderCell('dataset', 'Dataset')}
                {renderHeaderCell('sequencing_type', 'Seq Type')}
                <SUITable.HeaderCell className="header-cell">
                    <div className="filter-button">
                        <FilterButton
                            columnName="Technology"
                            options={getUniqueOptionsForColumn('sequencing_technology')}
                            selectedOptions={selectedSeqTechnologies}
                            onSelectionChange={(selectedOptions) =>
                                handleSelectionChange('sequencing_technology', selectedOptions)
                            }
                        />
                    </div>
                    {renderHeaderCell('sequencing_technology', 'Technology')}
                </SUITable.HeaderCell>
                {renderHeaderCell('total_families', 'Families')}
                {renderHeaderCell('total_participants', 'Participants')}
                {renderHeaderCell('total_samples', 'Samples')}
                {renderHeaderCell('total_sequencing_groups', 'Sequencing Groups')}
                {renderHeaderCell('total_crams', 'CRAMs')}
                {renderHeaderCell(
                    'percent_aligned',
                    '% Aligned',
                    'Percentage of Sequencing Groups with a Completed CRAM Analysis'
                )}
                {renderHeaderCell(
                    'percent_annotated',
                    '% in Annotated Dataset',
                    'Percentage of Sequencing Groups in the latest AnnotateDataset Analysis'
                )}
                {renderHeaderCell(
                    'percent_snv_index',
                    '% in SNV ES-Index',
                    'Percentage of Sequencing Groups in the latest SNV ES-Index Analysis'
                )}
                {renderHeaderCell(
                    'percent_sv_index',
                    '% in SV ES-Index',
                    'Percentage of Sequencing Groups in the latest SV (genome) or gCNV (exome) ES-Index Analysis'
                )}
            </SUITable.Row>
        </SUITable.Header>
    )
}

export default SummaryTableHeader
