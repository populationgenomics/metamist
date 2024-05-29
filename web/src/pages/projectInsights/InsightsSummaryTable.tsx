// InsightsSummaryTable.tsx
import React, { useState } from 'react'
import { ProjectInsightsSummary } from '../../sm-api'
import { Table } from 'semantic-ui-react'
import Tooltip, { TooltipProps } from '@mui/material/Tooltip'
import { ThemeContext } from '../../shared/components/ThemeProvider'

interface InsightsSummaryTableProps {
    filteredData: ProjectInsightsSummary[]
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

const InsightsSummaryTableRow: React.FC<{ summary: ProjectInsightsSummary }> = ({ summary }) => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'
    const percentageAligned =
        summary.total_sequencing_groups > 0
            ? (summary.total_crams / summary.total_sequencing_groups) * 100
            : 0

    const percentageInJointCall =
        summary.latest_annotate_dataset?.sg_count ?? 0 > 0
            ? ((summary.latest_annotate_dataset?.sg_count ?? 0) / summary.total_sequencing_groups) * 100
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
            <Table.Cell className="dataset-cell">{summary.dataset}</Table.Cell>
            <Table.Cell className="table-cell">{summary.sequencing_type}</Table.Cell>
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
                                {summary.total_crams} / {summary.total_sequencing_groups} Total
                                Sequencing Groups with a Completed CRAM Analysis
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
                            <p>
                                {summary.latest_annotate_dataset?.sg_count} /{' '}
                                {summary.total_sequencing_groups} Total Sequencing Groups in the
                                latest {summary.sequencing_type} AnnotateDataset analysis Analysis ID:{' '}
                                {summary.latest_annotate_dataset?.id}
                            </p>
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
                            <p>
                                {summary.latest_snv_es_index?.sg_count} /{' '}
                                {summary.total_sequencing_groups} Total Sequencing Groups in the
                                latest {summary.sequencing_type} SNV Elasticsearch Index Analysis ID:{' '}
                                {summary.latest_snv_es_index?.id}
                            </p>
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
                            <p>
                                {summary.latest_sv_es_index?.sg_count} /{' '}
                                {summary.total_sequencing_groups} Total Sequencing Groups in the
                                latest {summary.sequencing_type} SV Elasticsearch Index Analysis ID:{' '}
                                {summary.latest_sv_es_index?.id}
                            </p>
                        }
                    >
                        <div>{percentageInSvIndex.toFixed(2)}%</div>
                    </HtmlTooltip>
                </div>
            </Table.Cell>
        </Table.Row>
    )
}

const InsightsSummaryTable: React.FC<InsightsSummaryTableProps> = ({ filteredData }) => {
    const [sortColumn, setSortColumn] = useState<keyof ProjectInsightsSummary | null>(null)
    const [sortDirection, setSortDirection] = useState<'ascending' | 'descending'>('ascending')
    const handleSort = (column: keyof ProjectInsightsSummary) => {
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
                        sorted={sortColumn === 'total_families' ? sortDirection : undefined}
                        onClick={() => handleSort('total_families')}
                    >
                        Families
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'total_participants' ? sortDirection : undefined}
                        onClick={() => handleSort('total_participants')}
                    >
                        Participants
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'total_samples' ? sortDirection : undefined}
                        onClick={() => handleSort('total_samples')}
                    >
                        Samples
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={
                            sortColumn === 'total_sequencing_groups' ? sortDirection : undefined
                        }
                        onClick={() => handleSort('total_sequencing_groups')}
                    >
                        Sequencing Groups
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        className="header-cell"
                        sorted={sortColumn === 'total_crams' ? sortDirection : undefined}
                        onClick={() => handleSort('total_crams')}
                    >
                        CRAMs
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell">
                        <div style={{ display: 'flex', justifyContent: 'center' }}>
                            <HtmlTooltip
                                title={
                                    <p>
                                        Percentage of Sequencing Groups with a Completed CRAM
                                        Analysis
                                    </p>
                                }
                            >
                                <div>% Aligned</div>
                            </HtmlTooltip>
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell">
                        <div style={{ display: 'flex', justifyContent: 'center' }}>
                            <HtmlTooltip
                                title={
                                    <p>
                                        Percentage of Sequencing Groups in the latest
                                        AnnotateDataset Analysis
                                    </p>
                                }
                            >
                                <div>% in Annotated Dataset</div>
                            </HtmlTooltip>
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell">
                        <div style={{ display: 'flex', justifyContent: 'center' }}>
                            <HtmlTooltip
                                title={
                                    <p>
                                        Percentage of Sequencing Groups in the latest SNV ES-Index
                                        Analysis
                                    </p>
                                }
                            >
                                <div>% in SNV ES-Index</div>
                            </HtmlTooltip>
                        </div>
                    </Table.HeaderCell>
                    <Table.HeaderCell className="header-cell">
                        <div style={{ display: 'flex', justifyContent: 'center' }}>
                            <HtmlTooltip
                                title={
                                    <p>
                                        Percentage of Sequencing Groups in the latest SV (genome) or
                                        gCNV (exome) ES-Index Analysis
                                    </p>
                                }
                            >
                                <div>% in SV ES-Index</div>
                            </HtmlTooltip>
                        </div>
                    </Table.HeaderCell>
                </Table.Row>
            </Table.Header>
            <Table.Body>
                {sortedData.map((summary) => (
                    <InsightsSummaryTableRow
                        key={`${summary.dataset}-${summary.sequencing_type}`}
                        summary={summary}
                    />
                ))}
            </Table.Body>
            <Table.Footer>
                <Table.Row className="grand-total-row" key="grandTotals">
                    <Table.Cell className="table-cell">Grand Total</Table.Cell>
                    <Table.Cell className="table-cell">{sortedData.length} entries</Table.Cell>
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
                        {filteredData.reduce((acc, curr) => acc + curr.total_sequencing_groups, 0)}
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
                                        filteredData.reduce(
                                            (acc, curr) => acc + curr.total_crams,
                                            0
                                        ) /
                                            filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            ) || 0 * 100
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
                                                acc + (curr.latest_annotate_dataset?.sg_count ?? 0),
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
                                        filteredData.reduce(
                                            (acc, curr) =>
                                                acc + (curr.latest_annotate_dataset?.sg_count ?? 0),
                                            0
                                        ) /
                                            filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            ) || 0 * 100
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
                                            (acc, curr) => acc + (curr.latest_snv_es_index?.sg_count ?? 0),
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
                                        filteredData.reduce(
                                            (acc, curr) => acc + (curr.latest_snv_es_index?.sg_count ?? 0),
                                            0
                                        ) /
                                            filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            ) || 0 * 100
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
                                            (acc, curr) => acc + (curr.latest_sv_es_index?.sg_count ?? 0),
                                            0
                                        )}{' '}
                                        /{' '}
                                        {filteredData.reduce(
                                            (acc, curr) => acc + curr.total_sequencing_groups,
                                            0
                                        )}{' '}
                                        Total Sequencing Groups in the latest SV Elasticsearch Index
                                    </p>
                                }
                            >
                                <div>
                                    {(
                                        filteredData.reduce(
                                            (acc, curr) => acc + (curr.latest_sv_es_index?.sg_count ?? 0),
                                            0
                                        ) /
                                            filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            ) || 0 * 100
                                    ).toFixed(2)}
                                    %
                                </div>
                            </HtmlTooltip>
                        </div>
                    </Table.Cell>
                </Table.Row>
            </Table.Footer>
        </Table>
    )
}

export default InsightsSummaryTable
