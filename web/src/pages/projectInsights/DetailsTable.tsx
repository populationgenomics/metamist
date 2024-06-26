// DetailsTable.tsx
import React, { useState } from 'react'
import { Button, Table as SUITable } from 'semantic-ui-react'
import Table from '../../shared/components/Table'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import { ProjectInsightsDetails } from '../../sm-api'
import { ColumnKey, HeaderCell, detailsTableHeaderCellConfigs } from './HeaderCell'

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

function isKeyOfProjectInsightsDetails(key: string): key is keyof ProjectInsightsDetails {
    return key in ({} as ProjectInsightsDetails)
}

const DetailsTableRow: React.FC<{ details: ProjectInsightsDetails }> = ({ details }) => {
    const theme = React.useContext(ThemeContext)
    const rowClassName = getRowClassName(details.sequencing_type)

    return (
        <SUITable.Row key={`${details.sequencing_group_id}`} className={rowClassName}>
            <SUITable.Cell data-cell className="category-cell">
                {details.dataset}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="category-cell">
                {details.sequencing_type}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="SUITable-cell">
                {details.sequencing_technology}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="SUITable-cell">
                {details.sequencing_platform}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="SUITable-cell">
                {details.sample_type}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="SUITable-cell">
                {details.sequencing_group_id}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="SUITable-cell">
                {details.family_id}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="SUITable-cell">
                {details.family_ext_id}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="SUITable-cell">
                {details.participant_id}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="SUITable-cell">
                {details.participant_ext_id}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="SUITable-cell">
                {details.sample_id}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="SUITable-cell">
                {details.sample_ext_ids}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="SUITable-cell">
                {details.completed_cram ? '✅' : '❌'}
            </SUITable.Cell>
            <SUITable.Cell className="SUITable-cell">
                {details.in_latest_annotate_dataset ? '✅' : '❌'}
            </SUITable.Cell>
            <SUITable.Cell className="SUITable-cell">
                {details.in_latest_snv_es_index ? '✅' : '❌'}
            </SUITable.Cell>
            <SUITable.Cell className="SUITable-cell">
                {details.in_latest_sv_es_index ? '✅' : '❌'}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="SUITable-cell">
                {details.web_reports?.stripy ? (
                    <a href={(details.web_reports.stripy as { url: string }).url}>Link</a>
                ) : null}
            </SUITable.Cell>
            <SUITable.Cell data-cell className="SUITable-cell">
                {details.web_reports?.mito ? (
                    <a href={(details.web_reports.mito as { url: string }).url}>Link</a>
                ) : null}
            </SUITable.Cell>
        </SUITable.Row>
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
                const valueA = isKeyOfProjectInsightsDetails(column)
                    ? a[column]
                    : (a as any)[column]
                const valueB = isKeyOfProjectInsightsDetails(column)
                    ? b[column]
                    : (b as any)[column]
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

    const getUniqueOptionsForColumn = (columnName: ColumnKey) => {
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
                    item.sample_ext_ids.some((extId) => selectedSampleExtIds.includes(extId))) &&
                (columnName === 'sequencing_group_id' ||
                    selectedSequencingGroupIds.length === 0 ||
                    selectedSequencingGroupIds.includes(item.sequencing_group_id)) &&
                (columnName === 'completed_cram' ||
                    selectedCompletedCram.length === 0 ||
                    selectedCompletedCram.includes(item.completed_cram ? 'Yes' : 'No')) &&
                (columnName === 'in_latest_annotate_dataset' ||
                    selectedInLatestAnnotateDataset.length === 0 ||
                    selectedInLatestAnnotateDataset.includes(
                        item.in_latest_annotate_dataset ? 'Yes' : 'No'
                    )) &&
                (columnName === 'in_latest_snv_es_index' ||
                    selectedInLatestSnvEsIndex.length === 0 ||
                    selectedInLatestSnvEsIndex.includes(
                        item.in_latest_snv_es_index ? 'Yes' : 'No'
                    )) &&
                (columnName === 'in_latest_sv_es_index' ||
                    selectedInLatestSvEsIndex.length === 0 ||
                    selectedInLatestSvEsIndex.includes(
                        item.in_latest_sv_es_index ? 'Yes' : 'No'
                    )) &&
                (columnName === 'stripy' ||
                    selectedStripy.length === 0 ||
                    item.web_reports?.stripy) &&
                (columnName === 'mito' || selectedMito.length === 0 || item.web_reports?.mito)
            )
        })

        let uniqueOptions: string[] = []
        if (isKeyOfProjectInsightsDetails(columnName)) {
            uniqueOptions = Array.from(
                new Set(
                    filteredDataExcludingCurrentColumn.map(
                        (item) => item[columnName]?.toString() || ''
                    )
                )
            )
        } else {
            switch (columnName) {
                case 'stripy':
                case 'mito':
                    uniqueOptions = Array.from(
                        new Set(
                            filteredDataExcludingCurrentColumn.map((item) =>
                                item.web_reports?.[columnName] ? 'Yes' : 'No'
                            )
                        )
                    )
                    break
                case 'sample_ext_ids':
                    uniqueOptions = Array.from(
                        new Set(
                            filteredDataExcludingCurrentColumn.flatMap(
                                (item) => item.sample_ext_ids
                            )
                        )
                    ).map((option) => option?.toString() || '')
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
                        new Set(
                            filteredDataExcludingCurrentColumn.map(
                                (item) => (item as any)[columnName]
                            )
                        )
                    ).map((option) => option?.toString() || '')
            }
        }

        return uniqueOptions
    }

    const exportToFile = (format: 'csv' | 'tsv') => {
        const headerCells = document.querySelectorAll('.ui.SUITable thead tr th')
        const headerData = Array.from(headerCells).map((cell) => cell.textContent)
        const rows = document.querySelectorAll('.ui.SUITable tbody tr')
        const rowData = Array.from(rows).map((row) => {
            const cells = row.querySelectorAll('td')
            return Array.from(cells).map((cell) => {
                if (cell.textContent === '✅') {
                    return 'TRUE'
                } else if (cell.textContent === '❌') {
                    return 'FALSE'
                } else if (cell.querySelector('a')) {
                    const anchor = cell.querySelector('a')
                    return anchor ? anchor.href : null
                } else {
                    return cell.textContent
                }
            })
        })

        const separator = format === 'csv' ? ',' : '\t'
        const fileData = [headerData, ...rowData].map((row) => row.join(separator)).join('\n')
        const blob = new Blob([fileData], { type: `text/${format}` })
        const url = URL.createObjectURL(blob)
        const link = document.createElement('a')
        link.href = url
        // Generate the file name with the current date timestamp
        const currentDate = new Date().toISOString().slice(0, 19)
        const fileName = `project_insights_details_${currentDate}.${format}`
        link.setAttribute('download', fileName)
        document.body.appendChild(link)
        link.click()
        document.body.removeChild(link)
    }

    return (
        <div>
            <div style={{ textAlign: 'right' }}>
                <Button onClick={() => exportToFile('csv')}>Export to CSV</Button>
                <Button onClick={() => exportToFile('tsv')}>Export to TSV</Button>
            </div>
            <Table sortable>
                <SUITable.Header>
                    <SUITable.Row>
                        {detailsTableHeaderCellConfigs.map((config) => (
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
                                selectedOptions={
                                    config.key === 'sequencing_technology'
                                        ? selectedSeqTechnologies
                                        : config.key === 'sequencing_platform'
                                        ? selectedSeqPlatforms
                                        : config.key === 'sample_type'
                                        ? selectedSampleTypes
                                        : config.key === 'family_id'
                                        ? selectedFamilyIds
                                        : config.key === 'family_ext_id'
                                        ? selectedFamilyExtIds
                                        : config.key === 'participant_id'
                                        ? selectedParticipantIds
                                        : config.key === 'participant_ext_id'
                                        ? selectedParticipantExtIds
                                        : config.key === 'sample_id'
                                        ? selectedSampleIds
                                        : config.key === 'sample_ext_ids'
                                        ? selectedSampleExtIds
                                        : config.key === 'sequencing_group_id'
                                        ? selectedSequencingGroupIds
                                        : config.key === 'completed_cram'
                                        ? selectedCompletedCram
                                        : config.key === 'in_latest_annotate_dataset'
                                        ? selectedInLatestAnnotateDataset
                                        : config.key === 'in_latest_snv_es_index'
                                        ? selectedInLatestSnvEsIndex
                                        : config.key === 'in_latest_sv_es_index'
                                        ? selectedInLatestSvEsIndex
                                        : config.key === 'stripy'
                                        ? selectedStripy
                                        : config.key === 'mito'
                                        ? selectedMito
                                        : []
                                }
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
