// SeqrProjectsDetailsRable.tsx
import React, { useState } from 'react'
import { SeqrProjectsDetails } from '../../sm-api'
import { Table } from 'semantic-ui-react'
import Tooltip, { TooltipProps } from '@mui/material/Tooltip'
import { ThemeContext } from '../../shared/components/ThemeProvider'

interface DetailsTableProps {
    filteredData: SeqrProjectsDetails[]
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
            <Table.Cell className="table-cell">{details.sequencing_group_report_links?.stripy}</Table.Cell>
            <Table.Cell className="table-cell">{details.sequencing_group_report_links?.mito}</Table.Cell>
        </Table.Row>
    )
}

const DetailsTable: React.FC<DetailsTableProps> = ({ filteredData }) => {
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
                        Sequencing Type
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'sample_type' ? sortDirection : undefined}
                        onClick={() => handleSort('sample_type')}
                    >
                        Sample Type
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'sequencing_group_id' ? sortDirection : undefined}
                        onClick={() => handleSort('sequencing_group_id')}
                    >
                        Sequencing Group ID
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'family_id' ? sortDirection : undefined}
                        onClick={() => handleSort('family_id')}
                    >
                        Family ID
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'family_ext_id' ? sortDirection : undefined}
                        onClick={() => handleSort('family_ext_id')}
                    >
                        Family External ID
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={
                            sortColumn === 'participant_id' ? sortDirection : undefined
                        }
                        onClick={() => handleSort('participant_id')}
                    >
                        Participant ID
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'participant_ext_id' ? sortDirection : undefined}
                        onClick={() => handleSort('participant_ext_id')}
                    >
                        Participant External ID
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'sample_id' ? sortDirection : undefined}
                        onClick={() => handleSort('sample_id')}
                    >
                        Sample ID
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'sample_ext_ids' ? sortDirection : undefined}
                        onClick={() => handleSort('sample_ext_ids')}
                    >
                        Sample External ID(s)
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell"
                        sorted={sortColumn === 'completed_cram' ? sortDirection : undefined}
                        onClick={() => handleSort('completed_cram')}
                    >
                        Completed CRAM
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell"
                        sorted={sortColumn === 'in_latest_annotate_dataset' ? sortDirection : undefined}
                        onClick={() => handleSort('in_latest_annotate_dataset')}
                    >
                        In latest AnnotateDataset
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell"
                        sorted={sortColumn === 'in_latest_snv_es_index' ? sortDirection : undefined}
                        onClick={() => handleSort('in_latest_snv_es_index')}
                    >
                        In latest SNV ES-Index
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell"
                        sorted={sortColumn === 'in_latest_sv_es_index' ? sortDirection : undefined}
                        onClick={() => handleSort('in_latest_sv_es_index')}
                    >
                        In latest SV ES-Index
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell">
                        Sequencing Group Report Links
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
