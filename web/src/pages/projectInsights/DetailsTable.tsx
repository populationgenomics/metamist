// DetailsRable.tsx
import React, { useState } from 'react'
import { ProjectInsightsDetails } from '../../sm-api'
import { Button, Table, Icon } from 'semantic-ui-react'
import Tooltip, { TooltipProps } from '@mui/material/Tooltip'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import FilterButton from './FilterButton'

interface DetailsTableProps {
    allData: ProjectInsightsDetails[]
    filteredData: ProjectInsightsDetails[]
    selectedProjects: { name: string }[]
    selectedSeqTypes: string[]
    selectedSeqPlatforms: string[]
    selectedSeqTechnologies: string[]
    selectedSampleTypes: string[]
    selectedFamilyIds: string[]
    selectedFamilyExtIds: string[]
    selectedParticipantIds: string[]
    selectedParticipantExtIds: string[]
    selectedSampleIds: string[]
    selectedSampleExtIds: string[]
    selectedSequencingGroupIds: string[]
    selectedCompletedCram: string[]
    selectedInLatestAnnotateDataset: string[]
    selectedInLatestSnvEsIndex: string[]
    selectedInLatestSvEsIndex: string[]
    selectedStripy: string[]
    selectedMito: string[]
    handleSelectionChange: (columnName: string, selectedOptions: string[]) => void
}

// placeholder
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

const HtmlTooltip = (props: TooltipProps) => (
    <Tooltip {...props} classes={{ popper: 'html-tooltip' }} />
)

const DetailsTableRow: React.FC<{ details: ProjectInsightsDetails }> = ({ details }) => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'
    const rowClassName = getRowClassName(details.sequencing_type)

    return (
        <Table.Row key={`${details.sequencing_group_id}`} className={rowClassName}>
            <Table.Cell data-cell className="category-cell">{details.dataset}</Table.Cell>
            <Table.Cell data-cell className="category-cell">{details.sequencing_type}</Table.Cell>
            <Table.Cell data-cell className="category-cell">{details.sequencing_platform}</Table.Cell>
            <Table.Cell data-cell className="category-cell">{details.sequencing_technology}</Table.Cell>
            <Table.Cell data-cell className="category-cell">{details.sample_type}</Table.Cell>
            <Table.Cell data-cell className="table-cell">{details.sequencing_group_id}</Table.Cell>
            <Table.Cell data-cell className="table-cell">{details.family_id}</Table.Cell>
            <Table.Cell data-cell className="table-cell">{details.family_ext_id}</Table.Cell>
            <Table.Cell data-cell className="table-cell">{details.participant_id}</Table.Cell>
            <Table.Cell data-cell className="table-cell">{details.participant_ext_id}</Table.Cell>
            <Table.Cell data-cell className="table-cell">{details.sample_id}</Table.Cell>
            <Table.Cell data-cell className="table-cell">{details.sample_ext_ids}</Table.Cell>
            <Table.Cell data-cell className="table-cell">
                <HtmlTooltip
                        title={
                            <p>
                                {details.in_latest_annotate_dataset}
                            </p>
                        }
                    >
                        <div>{details.completed_cram ? '✅' : '❌'}</div>
                    </HtmlTooltip>
            </Table.Cell>
            <Table.Cell className="table-cell">
                {details.in_latest_annotate_dataset ? '✅' : '❌'}
            </Table.Cell>
            <Table.Cell className="table-cell">
                {details.in_latest_snv_es_index ? '✅' : '❌'}
            </Table.Cell>
            <Table.Cell className="table-cell">
                {details.in_latest_sv_es_index ? '✅' : '❌'}
            </Table.Cell>
            <Table.Cell data-cell className="table-cell">
                {details.sequencing_group_report_links?.stripy ? (
                    <a
                        href={`https://main-web.populationgenomics.org.au/${details.dataset}/stripy/${details.sequencing_group_id}.html`}
                    >
                        Link
                    </a>
                ) : null}
            </Table.Cell>
            <Table.Cell data-cell className="table-cell">
                {details.sequencing_group_report_links?.mito ? (
                    <a
                        href={`https://main-web.populationgenomics.org.au/${details.dataset}/mito/mitoreport-${details.sequencing_group_id}/index.html`}
                    >
                        Link
                    </a>
                ) : null}
            </Table.Cell>
        </Table.Row>
    )
}

const DetailsTable: React.FC<DetailsTableProps> = ({
    allData,
    filteredData,
    selectedProjects,
    selectedSeqTypes,
    selectedSeqPlatforms,
    selectedSeqTechnologies,
    selectedSampleTypes,
    selectedFamilyIds,
    selectedFamilyExtIds,
    selectedParticipantIds,
    selectedSampleIds,
    selectedSampleExtIds,
    selectedParticipantExtIds,
    selectedCompletedCram,
    selectedSequencingGroupIds,
    selectedInLatestAnnotateDataset,
    selectedInLatestSnvEsIndex,
    selectedInLatestSvEsIndex,
    selectedStripy,
    selectedMito,
    handleSelectionChange,
}) => {
    const [sortColumns, setSortColumns] = useState<
        Array<{ column: keyof ProjectInsightsDetails; direction: 'ascending' | 'descending' }>
    >([])
    const handleSort = (column: keyof ProjectInsightsDetails, isMultiSort: boolean) => {
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

    const getUniqueOptionsForColumn = (
        columnName: keyof ProjectInsightsDetails | 'stripy' | 'mito'
    ) => {
        const filteredDataExcludingCurrentColumn = allData.filter((item) => {
            return (
                selectedProjects.some((p) => p.name === item.dataset) &&
                selectedSeqTypes.includes(item.sequencing_type) &&
                (columnName === 'sample_type' ||
                    selectedSampleTypes.length === 0 ||
                    selectedSampleTypes.includes(item.sample_type)) &&
                (columnName === 'sequencing_platform' ||
                    selectedSeqPlatforms.length === 0 ||
                    selectedSeqPlatforms.includes(item.sequencing_platform)) &&
                (columnName === 'sequencing_technology' ||
                    selectedSeqTechnologies.length === 0 ||
                    selectedSeqTechnologies.includes(item.sequencing_technology)) &&
                (columnName === 'family_id' ||
                    selectedFamilyIds.length === 0 ||
                    selectedFamilyIds.includes(item.family_id?.toString() || '')) &&
                (columnName === 'family_ext_id' ||
                    selectedFamilyExtIds.length === 0 ||
                    selectedFamilyExtIds.includes(item.family_ext_id)) &&
                (columnName === 'participant_id' ||
                    selectedParticipantIds.length === 0 ||
                    selectedParticipantIds.includes(item.participant_id?.toString() || '')) &&
                (columnName === 'participant_ext_id' ||
                    selectedParticipantExtIds.length === 0 ||
                    selectedParticipantExtIds.includes(item.participant_ext_id)) &&
                (columnName === 'sample_id' ||
                    selectedSampleIds.length === 0 ||
                    selectedSampleIds.includes(item.sample_id)) &&
                (columnName === 'sample_ext_ids' ||
                    selectedSampleExtIds.length === 0 ||
                    selectedSampleExtIds.includes(item.sample_ext_ids[0])) &&
                (columnName === 'sequencing_group_id' ||
                    selectedSequencingGroupIds.length === 0 ||
                    selectedSequencingGroupIds.includes(item.sequencing_group_id)) &&
                (columnName === 'completed_cram' ||
                    selectedCompletedCram.length === 0 ||
                    item.completed_cram === (selectedCompletedCram[0] === 'true')) &&
                (columnName === 'in_latest_annotate_dataset' ||
                    selectedInLatestAnnotateDataset.length === 0 ||
                    item.in_latest_annotate_dataset ===
                        (selectedInLatestAnnotateDataset[0] === 'true')) &&
                (columnName === 'in_latest_snv_es_index' ||
                    selectedInLatestSnvEsIndex.length === 0 ||
                    item.in_latest_snv_es_index === (selectedInLatestSnvEsIndex[0] === 'true')) &&
                (columnName === 'in_latest_sv_es_index' ||
                    selectedInLatestSvEsIndex.length === 0 ||
                    item.in_latest_sv_es_index === (selectedInLatestSvEsIndex[0] === 'true')) &&
                (columnName === 'stripy' ||
                    selectedStripy.length === 0 ||
                    item.sequencing_group_report_links?.stripy) &&
                (columnName === 'mito' ||
                    selectedMito.length === 0 ||
                    item.sequencing_group_report_links?.mito)
            )
        })

        let uniqueOptions: string[] = []
        switch (columnName) {
            case 'stripy':
            case 'mito':
                uniqueOptions = Array.from(
                    new Set(
                        filteredDataExcludingCurrentColumn.map((item) =>
                            item.sequencing_group_report_links?.[columnName] ? 'Yes' : 'No'
                        )
                    )
                )
                break
            case 'completed_cram':
            case 'in_latest_annotate_dataset':
            case 'in_latest_snv_es_index':
            case 'in_latest_sv_es_index':
                uniqueOptions = Array.from(
                    new Set(
                        filteredDataExcludingCurrentColumn.map((item) =>
                            item[columnName] ? 'Yes' : 'No'
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
    const exportToCSV = () => {
        const headerCells = document.querySelectorAll('.ui.table thead tr th');
        const headerData = Array.from(headerCells).map(cell => cell.textContent);
      
        const rows = document.querySelectorAll('.ui.table tbody tr');
        const rowData = Array.from(rows).map(row => {
            const cells = row.querySelectorAll('td');
            return Array.from(cells).map(cell => {
                if (cell.textContent === '✅') {
                    return 'TRUE';
                } else if (cell.textContent === '❌') {
                    return 'FALSE';
                } else if (cell.querySelector('a')) {
                    const anchor = cell.querySelector('a');
                    return anchor ? anchor.href : null;
                } else {
                    return cell.textContent;
                }
            });
        });
      
        const csvData = [headerData, ...rowData].map(row => row.join(',')).join('\n');
      
        const blob = new Blob([csvData], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', 'table_data.csv');
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
      };

    return (
        <div>
        <Button onClick={exportToCSV}>Export to CSV</Button>
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
                            handleSort('sequencing_platform', event.shiftKey)
                        }
                    >
                        <div className="filter-button">
                            <FilterButton
                                columnName="Platform"
                                options={getUniqueOptionsForColumn('sequencing_platform')}
                                selectedOptions={selectedSampleTypes}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('sequencing_platform', selectedOptions)
                                }
                            />
                        </div>
                        {sortColumns.find((column) => column.column === 'sequencing_platform') && (
                            <Icon
                                name={
                                    sortColumns.find((column) => column.column === 'sequencing_platform')
                                        ?.direction === 'ascending'
                                        ? 'caret up'
                                        : 'caret down'
                                }
                                className="sort-icon"
                            />
                        )}
                        <div className="header-text">Platform</div>
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
                                selectedOptions={selectedSampleTypes}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('sequencing_technology', selectedOptions)
                                }
                            />
                        </div>
                        {sortColumns.find((column) => column.column === 'sequencing_technology') && (
                            <Icon
                                name={
                                    sortColumns.find((column) => column.column === 'sequencing_technology')
                                        ?.direction === 'ascending'
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
                            handleSort('sample_type', event.shiftKey)
                        }
                    >
                        <div className="filter-button">
                            <FilterButton
                                columnName="Sample Type"
                                options={getUniqueOptionsForColumn('sample_type')}
                                selectedOptions={selectedSampleTypes}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('sample_type', selectedOptions)
                                }
                            />
                        </div>
                        {sortColumns.find((column) => column.column === 'sample_type') && (
                            <Icon
                                name={
                                    sortColumns.find((column) => column.column === 'sample_type')
                                        ?.direction === 'ascending'
                                        ? 'caret up'
                                        : 'caret down'
                                }
                                className="sort-icon"
                            />
                        )}
                        <div className="header-text">Sample Type</div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        onClick={(event: React.MouseEvent<HTMLElement>) =>
                            handleSort('sequencing_group_id', event.shiftKey)
                        }
                    >
                        <div className="filter-button">
                            <FilterButton
                                columnName="SG ID"
                                options={getUniqueOptionsForColumn('sequencing_group_id')}
                                selectedOptions={selectedSequencingGroupIds}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('sequencing_group_id', selectedOptions)
                                }
                            />
                        </div>
                        {sortColumns.find((column) => column.column === 'sequencing_group_id') && (
                            <Icon
                                name={
                                    sortColumns.find(
                                        (column) => column.column === 'sequencing_group_id'
                                    )?.direction === 'ascending'
                                        ? 'caret up'
                                        : 'caret down'
                                }
                                className="sort-icon"
                            />
                        )}
                        <div className="header-text">SG ID</div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        onClick={(event: React.MouseEvent<HTMLElement>) =>
                            handleSort('family_id', event.shiftKey)
                        }
                    >
                        <div className="filter-button">
                            <FilterButton
                                columnName="Family ID"
                                options={getUniqueOptionsForColumn('family_id')}
                                selectedOptions={selectedFamilyIds}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('family_id', selectedOptions)
                                }
                            />
                        </div>
                        {sortColumns.find((column) => column.column === 'family_id') && (
                            <Icon
                                name={
                                    sortColumns.find((column) => column.column === 'family_id')
                                        ?.direction === 'ascending'
                                        ? 'caret up'
                                        : 'caret down'
                                }
                                className="sort-icon"
                            />
                        )}
                        <div className="header-text">Family ID</div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        onClick={(event: React.MouseEvent<HTMLElement>) =>
                            handleSort('family_ext_id', event.shiftKey)
                        }
                    >
                        <div className="filter-button">
                            <FilterButton
                                columnName="Family Ext. ID"
                                options={getUniqueOptionsForColumn('family_ext_id')}
                                selectedOptions={selectedFamilyExtIds}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('family_ext_id', selectedOptions)
                                }
                            />
                        </div>
                        {sortColumns.find((column) => column.column === 'family_ext_id') && (
                            <Icon
                                name={
                                    sortColumns.find((column) => column.column === 'family_ext_id')
                                        ?.direction === 'ascending'
                                        ? 'caret up'
                                        : 'caret down'
                                }
                                className="sort-icon"
                            />
                        )}
                        <div className="header-text">Family Ext. ID</div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        onClick={(event: React.MouseEvent<HTMLElement>) =>
                            handleSort('participant_id', event.shiftKey)
                        }
                    >
                        <div className="filter-button">
                            <FilterButton
                                columnName="Participant ID"
                                options={getUniqueOptionsForColumn('participant_id')}
                                selectedOptions={selectedParticipantIds}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('participant_id', selectedOptions)
                                }
                            />
                        </div>
                        {sortColumns.find((column) => column.column === 'participant_id') && (
                            <Icon
                                name={
                                    sortColumns.find((column) => column.column === 'participant_id')
                                        ?.direction === 'ascending'
                                        ? 'caret up'
                                        : 'caret down'
                                }
                                className="sort-icon"
                            />
                        )}
                        <div className="header-text">Participant ID</div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        onClick={(event: React.MouseEvent<HTMLElement>) =>
                            handleSort('participant_ext_id', event.shiftKey)
                        }
                    >
                        <div className="filter-button">
                            <FilterButton
                                columnName="Participant Ext. ID"
                                options={getUniqueOptionsForColumn('participant_ext_id')}
                                selectedOptions={selectedParticipantExtIds}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('participant_ext_id', selectedOptions)
                                }
                            />
                        </div>
                        {sortColumns.find((column) => column.column === 'participant_ext_id') && (
                            <Icon
                                name={
                                    sortColumns.find(
                                        (column) => column.column === 'participant_ext_id'
                                    )?.direction === 'ascending'
                                        ? 'caret up'
                                        : 'caret down'
                                }
                                className="sort-icon"
                            />
                        )}
                        <div className="header-text">Participant Ext. ID</div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        onClick={(event: React.MouseEvent<HTMLElement>) =>
                            handleSort('sample_id', event.shiftKey)
                        }
                    >
                        <div className="filter-button">
                            <FilterButton
                                columnName="Sample ID"
                                options={getUniqueOptionsForColumn('sample_id')}
                                selectedOptions={selectedSampleIds}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('sample_id', selectedOptions)
                                }
                            />
                        </div>
                        {sortColumns.find((column) => column.column === 'sample_id') && (
                            <Icon
                                name={
                                    sortColumns.find((column) => column.column === 'sample_id')
                                        ?.direction === 'ascending'
                                        ? 'caret up'
                                        : 'caret down'
                                }
                                className="sort-icon"
                            />
                        )}
                        <div className="header-text">Sample ID</div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        onClick={(event: React.MouseEvent<HTMLElement>) =>
                            handleSort('sample_ext_ids', event.shiftKey)
                        }
                    >
                        <div className="filter-button">
                            <FilterButton
                                columnName="Sample Ext. ID(s)"
                                options={getUniqueOptionsForColumn('sample_ext_ids')}
                                selectedOptions={selectedSampleExtIds}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('sample_ext_ids', selectedOptions)
                                }
                            />
                        </div>
                        {sortColumns.find((column) => column.column === 'sample_ext_ids') && (
                            <Icon
                                name={
                                    sortColumns.find((column) => column.column === 'sample_ext_ids')
                                        ?.direction === 'ascending'
                                        ? 'caret up'
                                        : 'caret down'
                                }
                                className="sort-icon"
                            />
                        )}
                        <div className="header-text">Sample Ext. ID(s)</div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                      className="header-cell collapsible-header"
                      onClick={(event: React.MouseEvent<HTMLElement>) =>
                        handleSort('completed_cram', event.shiftKey)
                      }
                      onMouseEnter={(event: React.MouseEvent<HTMLElement>) => {
                        event.currentTarget.classList.add('expanded');
                      }}
                      onMouseLeave={(event: React.MouseEvent<HTMLElement>) => {
                        event.currentTarget.classList.remove('expanded');
                      }}
                    >
                      <div className="filter-button">
                        <FilterButton
                          columnName="Completed CRAM"
                          options={getUniqueOptionsForColumn('completed_cram')}
                          selectedOptions={selectedCompletedCram}
                          onSelectionChange={(selectedOptions) =>
                            handleSelectionChange('completed_cram', selectedOptions)
                          }
                        />
                      </div>
                      {sortColumns.find((column) => column.column === 'completed_cram') && (
                        <Icon
                          name={
                            sortColumns.find((column) => column.column === 'completed_cram')
                              ?.direction === 'ascending'
                              ? 'caret up'
                              : 'caret down'
                          }
                          className="sort-icon"
                        />
                      )}
                      <div className="header-text">CRAM</div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell collapsible-header"
                        onClick={(event: React.MouseEvent<HTMLElement>) =>
                            handleSort('in_latest_annotate_dataset', event.shiftKey)
                        }
                        onMouseEnter={(event: React.MouseEvent<HTMLElement>) => {
                            event.currentTarget.classList.add('expanded');
                          }}
                          onMouseLeave={(event: React.MouseEvent<HTMLElement>) => {
                            event.currentTarget.classList.remove('expanded');
                          }}
                    >
                        <div className="filter-button">
                            <FilterButton
                                columnName="In Latest Annotate Dataset"
                                options={getUniqueOptionsForColumn('in_latest_annotate_dataset')}
                                selectedOptions={selectedInLatestAnnotateDataset}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange(
                                        'in_latest_annotate_dataset',
                                        selectedOptions
                                    )
                                }
                            />
                        </div>
                        {sortColumns.find(
                            (column) => column.column === 'in_latest_annotate_dataset'
                        ) && (
                            <Icon
                                name={
                                    sortColumns.find(
                                        (column) => column.column === 'in_latest_annotate_dataset'
                                    )?.direction === 'ascending'
                                        ? 'caret up'
                                        : 'caret down'
                                }
                                className="sort-icon"
                            />
                        )}
                        <div className="header-text">Joint Callset</div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell collapsible-header"
                        onClick={(event: React.MouseEvent<HTMLElement>) =>
                            handleSort('in_latest_snv_es_index', event.shiftKey)
                        }
                        onMouseEnter={(event: React.MouseEvent<HTMLElement>) => {
                            event.currentTarget.classList.add('expanded');
                          }}
                          onMouseLeave={(event: React.MouseEvent<HTMLElement>) => {
                            event.currentTarget.classList.remove('expanded');
                          }}
                    >
                        <div className="filter-button">
                            <FilterButton
                                columnName="In Latest SNV ES-Index"
                                options={getUniqueOptionsForColumn('in_latest_snv_es_index')}
                                selectedOptions={selectedInLatestSnvEsIndex}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('in_latest_snv_es_index', selectedOptions)
                                }
                            />
                        </div>
                        {sortColumns.find(
                            (column) => column.column === 'in_latest_snv_es_index'
                        ) && (
                            <Icon
                                name={
                                    sortColumns.find(
                                        (column) => column.column === 'in_latest_snv_es_index'
                                    )?.direction === 'ascending'
                                        ? 'caret up'
                                        : 'caret down'
                                }
                                className="sort-icon"
                            />
                        )}
                        <div className="header-text">SNV ES-Index</div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell collapsible-header"
                        onClick={(event: React.MouseEvent<HTMLElement>) =>
                            handleSort('in_latest_sv_es_index', event.shiftKey)
                        }
                        onMouseEnter={(event: React.MouseEvent<HTMLElement>) => {
                            event.currentTarget.classList.add('expanded');
                          }}
                          onMouseLeave={(event: React.MouseEvent<HTMLElement>) => {
                            event.currentTarget.classList.remove('expanded');
                          }}
                    >
                        <div className="filter-button">
                            <FilterButton
                                columnName="In Latest SV ES-Index"
                                options={getUniqueOptionsForColumn('in_latest_sv_es_index')}
                                selectedOptions={selectedInLatestSvEsIndex}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('in_latest_sv_es_index', selectedOptions)
                                }
                            />
                        </div>
                        {sortColumns.find(
                            (column) => column.column === 'in_latest_sv_es_index'
                        ) && (
                            <Icon
                                name={
                                    sortColumns.find(
                                        (column) => column.column === 'in_latest_sv_es_index'
                                    )?.direction === 'ascending'
                                        ? 'caret up'
                                        : 'caret down'
                                }
                                className="sort-icon"
                            />
                        )}
                        <div className="header-text">SV ES-Index</div>
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell collapsible-header"
                    onMouseEnter={(event: React.MouseEvent<HTMLElement>) => {
                        event.currentTarget.classList.add('expanded');
                      }}
                      onMouseLeave={(event: React.MouseEvent<HTMLElement>) => {
                        event.currentTarget.classList.remove('expanded');
                      }}>
                        <div className="filter-button">
                            <FilterButton
                                columnName="STRipy Report"
                                options={getUniqueOptionsForColumn('stripy')}
                                selectedOptions={selectedStripy}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('stripy', selectedOptions)
                                }
                            />
                        </div>
                        <div className="header-text">STRipy Report</div>
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell collapsible-header"
                    onMouseEnter={(event: React.MouseEvent<HTMLElement>) => {
                        event.currentTarget.classList.add('expanded');
                      }}
                      onMouseLeave={(event: React.MouseEvent<HTMLElement>) => {
                        event.currentTarget.classList.remove('expanded');
                      }}>
                        <div className="filter-button">
                            <FilterButton
                                columnName="Mito Report"
                                options={getUniqueOptionsForColumn('mito')}
                                selectedOptions={selectedMito}
                                onSelectionChange={(selectedOptions) =>
                                    handleSelectionChange('mito', selectedOptions)
                                }
                            />
                        </div>
                        <div className="header-text">Mito Report</div>
                    </Table.HeaderCell>
                </Table.Row>
            </Table.Header>
            <Table.Body>
                {sortedData.map((details) => (
                    <DetailsTableRow data-row key={`${details.sequencing_group_id}`} details={details} />
                ))}
            </Table.Body>
        </Table>
        </div>
    )
}

export default DetailsTable
