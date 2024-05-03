// SeqrProjectsDetailsRable.tsx
import React, { useState } from 'react'
import { SeqrProjectsDetails } from '../../sm-api'
import { Table } from 'semantic-ui-react'
import Tooltip, { TooltipProps } from '@mui/material/Tooltip'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import FilterButton from './FilterButton'

interface DetailsTableProps {
    allData: SeqrProjectsDetails[]
    filteredData: SeqrProjectsDetails[]
    selectedProjects: { name: string }[]
    selectedSeqTypes: string[]
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

const DetailsTableRow: React.FC<{ details: SeqrProjectsDetails }> = ({ details }) => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'
    const rowClassName = getRowClassName(details.sequencing_type)

    return (
        <Table.Row key={`${details.sequencing_group_id}`} className={rowClassName}>
            <Table.Cell className="dataset-cell">{details.dataset}</Table.Cell>
            <Table.Cell className="table-cell">{details.sequencing_type}</Table.Cell>
            <Table.Cell className="table-cell">{details.sample_type}</Table.Cell>
            <Table.Cell className="table-cell">{details.sequencing_group_id}</Table.Cell>
            <Table.Cell className="table-cell">{details.family_id}</Table.Cell>
            <Table.Cell className="table-cell">{details.family_ext_id}</Table.Cell>
            <Table.Cell className="table-cell">{details.participant_id}</Table.Cell>
            <Table.Cell className="table-cell">{details.participant_ext_id}</Table.Cell>
            <Table.Cell className="table-cell">{details.sample_id}</Table.Cell>
            <Table.Cell className="table-cell">{details.sample_ext_ids}</Table.Cell>
            <Table.Cell className="table-cell">{
                details.completed_cram ? 'Yes' : 'No'
            }</Table.Cell>
            <Table.Cell className="table-cell">{
                details.in_latest_annotate_dataset ? 'Yes' : 'No'
            }</Table.Cell>
            <Table.Cell className="table-cell">{
                details.in_latest_snv_es_index ? 'Yes' : 'No'
            }</Table.Cell>
            <Table.Cell className="table-cell">{
                details.in_latest_sv_es_index ? 'Yes' : 'No'
            }</Table.Cell>
            <Table.Cell className="table-cell">
                {details.sequencing_group_report_links?.stripy ? (
                    <a href={`https://main-web.populationgenomics.org.au/${details.dataset}/stripy/${details.sequencing_group_id}.html`}>
                        Link
                    </a>
                ) : (
                    null
                )}
            </Table.Cell>
            <Table.Cell className="table-cell">
                {details.sequencing_group_report_links?.mito ? (
                    <a href={`https://main-web.populationgenomics.org.au/${details.dataset}/mito/mitoreport-${details.sequencing_group_id}/index.html`}>
                        Link
                    </a>
                ) : (
                    null
                )}
            </Table.Cell>
        </Table.Row>
    )
}

const DetailsTable: React.FC<DetailsTableProps> = ({ allData, filteredData, selectedProjects, selectedSeqTypes, selectedSampleTypes, selectedFamilyIds, selectedFamilyExtIds, selectedParticipantIds, selectedSampleIds, selectedSampleExtIds, selectedParticipantExtIds, selectedCompletedCram, selectedSequencingGroupIds, selectedInLatestAnnotateDataset, selectedInLatestSnvEsIndex, selectedInLatestSvEsIndex, handleSelectionChange }) => {
    const [sortColumn, setSortColumn] = useState<keyof SeqrProjectsDetails | null>(null)
    const [sortDirection, setSortDirection] = useState<'ascending' | 'descending'>('ascending')
    const handleSort = (column: keyof SeqrProjectsDetails) => {
        if (sortColumn === column) {
            setSortDirection(sortDirection === 'ascending' ? 'descending' : 'ascending')
        } else {
            setSortColumn(column)
            setSortDirection('ascending')
        }
    }

    const sortedData = React.useMemo(() => {
        const data = [...filteredData]
        if (sortColumn) {
            data.sort((a, b) => {
                const valueA = a[sortColumn]
                const valueB = b[sortColumn]
                if (valueA === valueB) return 0
                if (typeof valueA === 'number' && typeof valueB === 'number') {
                    return sortDirection === 'ascending' ? valueA - valueB : valueB - valueA
                } else {
                    return sortDirection === 'ascending'
                        ? String(valueA).localeCompare(String(valueB))
                        : String(valueB).localeCompare(String(valueA))
                }
            })
        }
        return data
    }, [filteredData, sortColumn, sortDirection])

    const getUniqueOptionsForColumn = (columnName: keyof SeqrProjectsDetails) => {
        const filteredDataExcludingCurrentColumn = allData.filter((item) => {
          return (
            selectedProjects.some((p) => p.name === item.dataset) &&
            selectedSeqTypes.includes(item.sequencing_type) &&
            (columnName === 'sample_type' || selectedSampleTypes.length === 0 || selectedSampleTypes.includes(item.sample_type)) &&
            (columnName === 'family_id' || selectedFamilyIds.length === 0 || selectedFamilyIds.includes(item.family_id?.toString() || '')) &&
            (columnName === 'family_ext_id' || selectedFamilyExtIds.length === 0 || selectedFamilyExtIds.includes(item.family_ext_id)) &&
            (columnName === 'participant_id' || selectedParticipantIds.length === 0 || selectedParticipantIds.includes(item.participant_id?.toString() || '')) &&
            (columnName === 'participant_ext_id' || selectedParticipantExtIds.length === 0 || selectedParticipantExtIds.includes(item.participant_ext_id)) &&
            (columnName === 'sample_id' || selectedSampleIds.length === 0 || selectedSampleIds.includes(item.sample_id)) &&
            (columnName === 'sample_ext_ids' || selectedSampleExtIds.length === 0 || selectedSampleExtIds.includes(item.sample_ext_ids[0])) &&
            (columnName === 'sequencing_group_id' || selectedSequencingGroupIds.length === 0 || selectedSequencingGroupIds.includes(item.sequencing_group_id)) &&
            (columnName === 'completed_cram' || selectedCompletedCram.length === 0 || item.completed_cram === (selectedCompletedCram[0] === 'true')) &&
            (columnName === 'in_latest_annotate_dataset' || selectedInLatestAnnotateDataset.length === 0 || item.in_latest_annotate_dataset === (selectedInLatestAnnotateDataset[0] === 'true')) &&
            (columnName === 'in_latest_snv_es_index' || selectedInLatestSnvEsIndex.length === 0 || item.in_latest_snv_es_index === (selectedInLatestSnvEsIndex[0] === 'true')) &&
            (columnName === 'in_latest_sv_es_index' || selectedInLatestSvEsIndex.length === 0 || item.in_latest_sv_es_index === (selectedInLatestSvEsIndex[0] === 'true'))
          );
        });
      
        const uniqueOptions = Array.from(new Set(filteredDataExcludingCurrentColumn.map((item) => item[columnName])));
        return uniqueOptions.map((option) => option?.toString() || '');
    };

    return (
        <Table sortable>
            <Table.Header>
                <Table.Row>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'dataset' ? sortDirection : undefined}
                        onClick={() => handleSort('dataset')}
                    >
                        Dataset
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'sequencing_type' ? sortDirection : undefined}
                        onClick={() => handleSort('sequencing_type')}
                    >
                        Seq Type
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'sample_type' ? sortDirection : undefined}
                    >
                        <div>
                        <FilterButton
                            columnName="sample_type"
                            options={getUniqueOptionsForColumn('sample_type')}
                            selectedOptions={selectedSampleTypes}
                            onSelectionChange={(selectedOptions) => handleSelectionChange('sample_type', selectedOptions)}
                        />
                        </div>
                        <div>
                        Sample Type 
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'sequencing_group_id' ? sortDirection : undefined}
                    >
                        <div>
                        <FilterButton
                            columnName="sequencing_group_id"
                            options={getUniqueOptionsForColumn('sequencing_group_id')}
                            selectedOptions={selectedSequencingGroupIds}
                            onSelectionChange={(selectedOptions) => handleSelectionChange('sequencing_group_id', selectedOptions)}
                        />
                        </div>
                        <div>
                        SG ID
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'family_id' ? sortDirection : undefined}
                    >   
                        <div>
                        <FilterButton
                            columnName="family_id"
                            options={getUniqueOptionsForColumn('family_id')}
                            selectedOptions={selectedFamilyIds}
                            onSelectionChange={(selectedOptions) => handleSelectionChange('family_id', selectedOptions)}
                        />
                        </div>
                        <div>
                        Family ID
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'family_ext_id' ? sortDirection : undefined}
                    >
                        <div>
                        <FilterButton
                            columnName="family_ext_id"
                            options={getUniqueOptionsForColumn('family_ext_id')}
                            selectedOptions={selectedFamilyExtIds}
                            onSelectionChange={(selectedOptions) => handleSelectionChange('family_ext_id', selectedOptions)}
                        />
                        </div>
                        <div>
                        Family Ext. ID
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={
                            sortColumn === 'participant_id' ? sortDirection : undefined
                        }
                    >
                        <div>
                        <FilterButton
                            columnName="participant_id"
                            options={getUniqueOptionsForColumn('participant_id')}
                            selectedOptions={selectedParticipantIds}
                            onSelectionChange={(selectedOptions) => handleSelectionChange('participant_id', selectedOptions)}
                        />
                        </div>
                        <div>
                        Participant ID
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'participant_ext_id' ? sortDirection : undefined}
                    >
                        <div>
                        <FilterButton
                            columnName="participant_ext_id"
                            options={getUniqueOptionsForColumn('participant_ext_id')}
                            selectedOptions={selectedParticipantExtIds}
                            onSelectionChange={(selectedOptions) => handleSelectionChange('participant_ext_id', selectedOptions)}
                        />
                        </div>
                        <div>
                        Participant Ext. ID
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'sample_id' ? sortDirection : undefined}
                    >
                        <div>
                        <FilterButton
                            columnName="sample_id"
                            options={getUniqueOptionsForColumn('sample_id')}
                            selectedOptions={selectedSampleIds}
                            onSelectionChange={(selectedOptions) => handleSelectionChange('sample_id', selectedOptions)}
                        />
                        </div>
                        <div>
                        Sample ID
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'sample_ext_ids' ? sortDirection : undefined}
                    >
                        <div>
                        <FilterButton
                            columnName="sample_ext_ids"
                            options={getUniqueOptionsForColumn('sample_ext_ids')}
                            selectedOptions={selectedSampleExtIds}
                            onSelectionChange={(selectedOptions) => handleSelectionChange('sample_ext_ids', selectedOptions)}
                        />
                        </div>
                        <div>
                        Sample Ext. ID(s)
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell"
                        sorted={sortColumn === 'completed_cram' ? sortDirection : undefined}
                    >
                        <div>
                        <FilterButton
                            columnName="completed_cram"
                            options={getUniqueOptionsForColumn('completed_cram')}
                            selectedOptions={selectedCompletedCram}
                            onSelectionChange={(selectedOptions) => handleSelectionChange('completed_cram', selectedOptions)}
                        />
                        </div>
                        <div>
                        Completed CRAM
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell"
                        sorted={sortColumn === 'in_latest_annotate_dataset' ? sortDirection : undefined}
                    >
                        <div>
                        <FilterButton
                            columnName="in_latest_annotate_dataset"
                            options={getUniqueOptionsForColumn('in_latest_annotate_dataset')}
                            selectedOptions={selectedInLatestAnnotateDataset}
                            onSelectionChange={(selectedOptions) => handleSelectionChange('in_latest_annotate_dataset', selectedOptions)}
                        />
                        </div>
                        <div>
                        In latest J.C.
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell"
                        sorted={sortColumn === 'in_latest_snv_es_index' ? sortDirection : undefined}
                    >
                        <div>
                        <FilterButton
                            columnName="in_latest_snv_es_index"
                            options={getUniqueOptionsForColumn('in_latest_snv_es_index')}
                            selectedOptions={selectedInLatestSnvEsIndex}
                            onSelectionChange={(selectedOptions) => handleSelectionChange('in_latest_snv_es_index', selectedOptions)}
                        />
                        </div>
                        <div>
                        In latest SNV ES-Index
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell"
                        sorted={sortColumn === 'in_latest_sv_es_index' ? sortDirection : undefined}
                    >
                        <div>
                        <FilterButton
                            columnName="in_latest_sv_es_index"
                            options={getUniqueOptionsForColumn('in_latest_sv_es_index')}
                            selectedOptions={selectedInLatestSvEsIndex}
                            onSelectionChange={(selectedOptions) => handleSelectionChange('in_latest_sv_es_index', selectedOptions)}
                        />
                        </div>
                        <div>
                        In latest SV ES-Index
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell">
                        STRipy Report
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell">
                        Mito Report
                    </Table.HeaderCell>
                </Table.Row>
            </Table.Header>
            <Table.Body>
                {sortedData.map((details) => (
                    <DetailsTableRow
                        key={`${details.sequencing_group_id}`}
                        details={details}
                    />
                ))}
            </Table.Body>
        </Table>
    )
}

export default DetailsTable
