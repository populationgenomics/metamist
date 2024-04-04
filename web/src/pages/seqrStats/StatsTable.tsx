// StatsTable.tsx
import React, { useState, useEffect } from 'react'
import { ProjectInsightsStats } from '../../sm-api'
import { Table } from 'semantic-ui-react'
import { styled } from '@mui/material/styles'
import Tooltip, { TooltipProps, tooltipClasses } from '@mui/material/Tooltip'

interface StatsTableProps {
    filteredData: ProjectInsightsStats[]
}

function getPercentageColor(percentage: number) {
    const red = 265 - (percentage / 100) * 85 // Reducing intensity
    const green = 180 + (percentage / 100) * 85 // Reducing intensity
    const blue = 155 // Adding more blue for a pastel tone
    return `rgb(${red}, ${green}, ${blue})`
}

const HtmlTooltip = styled(({ className, ...props }: TooltipProps) => (
    <Tooltip {...props} classes={{ popper: className }} />
))(({ theme }) => ({
    [`& .${tooltipClasses.tooltip}`]: {
        backgroundColor: '#f5f5f9',
        color: 'rgba(0, 0, 0, 0.87)',
        maxWidth: 220,
        fontSize: theme.typography.pxToRem(12),
        border: '1px solid #dadde9',
    },
}))

const styles: Record<string, React.CSSProperties> = {
    tableCell: {
        textAlign: 'center',
    },
    exomeRow: {
        // Light yellow for exome rows
        backgroundColor: '#ffffe0',
    },
    genomeRow: {
        // Faint peach for genome rows
        backgroundColor: '#fff5ee',
    },
    rnaRow: {
        backgroundColor: '#f0fff0', // Light green tint for RNA rows
    },
    pageTotalRow: {
        // Light blue and emphasized for page total row
        backgroundColor: '#f0f8ff',
        fontWeight: 'bold',
    },
    grandTotalRow: {
        // Light purple and emphasized for grand total row
        backgroundColor: '#e6e6fa',
        fontWeight: 'bold',
    },
}

const StatsTableRow: React.FC<{ stats: ProjectInsightsStats }> = ({ stats }) => {
    const percentageAligned =
        stats.total_sequencing_groups > 0
            ? (stats.total_crams / stats.total_sequencing_groups) * 100
            : 0

    const percentageInJointCall =
        stats.latest_annotate_dataset.sg_count ?? 0 > 0
            ? ((stats.latest_annotate_dataset.sg_count ?? 0) / stats.total_sequencing_groups) * 100
            : 0
    const percentageInSnvIndex =
        stats.latest_snv_es_index.sg_count ?? 0 > 0
            ? ((stats.latest_snv_es_index.sg_count ?? 0) / stats.total_sequencing_groups) * 100
            : 0
    const percentageInSvIndex =
        stats.latest_sv_es_index.sg_count ?? 0 > 0
            ? ((stats.latest_sv_es_index.sg_count ?? 0) / stats.total_sequencing_groups) * 100
            : 0

    const rowStyle =
        stats.sequencing_type === 'exome'
            ? styles.exomeRow
            : stats.sequencing_type === 'genome'
            ? styles.genomeRow
            : styles.rnaRow

    return (
        <Table.Row key={`${stats.project}-${stats.sequencing_type}`} style={rowStyle}>
            <Table.Cell style={styles.tableCell}>{stats.dataset}</Table.Cell>
            <Table.Cell style={styles.tableCell}>{stats.sequencing_type}</Table.Cell>
            <Table.Cell style={styles.tableCell}>{stats.total_families}</Table.Cell>
            <Table.Cell style={styles.tableCell}>{stats.total_participants}</Table.Cell>
            <Table.Cell style={styles.tableCell}>{stats.total_samples}</Table.Cell>
            <Table.Cell style={styles.tableCell}>{stats.total_sequencing_groups}</Table.Cell>
            <Table.Cell style={styles.tableCell}>{stats.total_crams}</Table.Cell>
            <Table.Cell
                style={{
                    ...styles.tableCell,
                    backgroundColor: getPercentageColor(percentageAligned),
                }}
            >
                <div style={{ display: 'flex', justifyContent: 'center' }}>
                    <HtmlTooltip
                        title={
                            <React.Fragment>
                                <div>
                                    {stats.total_crams} / {stats.total_sequencing_groups} Total
                                    Sequencing Groups with a Completed CRAM Analysis
                                </div>
                            </React.Fragment>
                        }
                    >
                        <div>{percentageAligned.toFixed(2)}%</div>
                    </HtmlTooltip>
                </div>
            </Table.Cell>
            <Table.Cell
                style={{
                    ...styles.tableCell,
                    backgroundColor: getPercentageColor(percentageInJointCall),
                }}
            >
                <div style={{ display: 'flex', justifyContent: 'center' }}>
                    <HtmlTooltip
                        title={
                            <React.Fragment>
                                <div>
                                    {stats.latest_annotate_dataset.sg_count} /{' '}
                                    {stats.total_sequencing_groups} Total Sequencing Groups in the
                                    latest {stats.sequencing_type} AnnotateDataset analysis
                                </div>
                                <div>Analysis ID: {stats.latest_annotate_dataset.id}</div>
                            </React.Fragment>
                        }
                    >
                        <div>{percentageInJointCall.toFixed(2)}%</div>
                    </HtmlTooltip>
                </div>
            </Table.Cell>
            <Table.Cell
                style={{
                    ...styles.tableCell,
                    backgroundColor: getPercentageColor(percentageInSnvIndex),
                }}
            >
                <div style={{ display: 'flex', justifyContent: 'center' }}>
                    <HtmlTooltip
                        title={
                            <React.Fragment>
                                <div>
                                    {stats.latest_snv_es_index.sg_count} /{' '}
                                    {stats.total_sequencing_groups} Total Sequencing Groups in the
                                    latest {stats.sequencing_type} SNV Elasticsearch Index
                                </div>
                                <div>Analysis ID: {stats.latest_snv_es_index.id}</div>
                            </React.Fragment>
                        }
                    >
                        <div>{percentageInSnvIndex.toFixed(2)}%</div>
                    </HtmlTooltip>
                </div>
            </Table.Cell>
            <Table.Cell
                style={{
                    ...styles.tableCell,
                    backgroundColor: getPercentageColor(percentageInSvIndex),
                }}
            >
                <div style={{ display: 'flex', justifyContent: 'center' }}>
                    <HtmlTooltip
                        title={
                            <React.Fragment>
                                <div>
                                    {stats.latest_sv_es_index.sg_count} /{' '}
                                    {stats.total_sequencing_groups} Total Sequencing Groups in the
                                    latest {stats.sequencing_type} SV Elasticsearch Index
                                </div>
                                <div>Analysis ID: {stats.latest_sv_es_index.id}</div>
                            </React.Fragment>
                        }
                    >
                        <div>{percentageInSvIndex.toFixed(2)}%</div>
                    </HtmlTooltip>
                </div>
            </Table.Cell>
        </Table.Row>
    )
}

const StatsTable: React.FC<StatsTableProps> = ({ filteredData }) => {
    const [sortColumn, setSortColumn] = useState<keyof ProjectInsightsStats | null>(null)
    const [sortDirection, setSortDirection] = useState<'ascending' | 'descending'>('ascending')
    const handleSort = (column: keyof ProjectInsightsStats) => {
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
                        style={styles.tableCell}
                        sorted={sortColumn === 'dataset' ? sortDirection : undefined}
                        onClick={() => handleSort('dataset')}
                    >
                        Dataset
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        style={styles.tableCell}
                        sorted={sortColumn === 'sequencing_type' ? sortDirection : undefined}
                        onClick={() => handleSort('sequencing_type')}
                    >
                        Sequencing Type
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        style={styles.tableCell}
                        sorted={sortColumn === 'total_families' ? sortDirection : undefined}
                        onClick={() => handleSort('total_families')}
                    >
                        Families
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        style={styles.tableCell}
                        sorted={sortColumn === 'total_participants' ? sortDirection : undefined}
                        onClick={() => handleSort('total_participants')}
                    >
                        Participants
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        style={styles.tableCell}
                        sorted={sortColumn === 'total_samples' ? sortDirection : undefined}
                        onClick={() => handleSort('total_samples')}
                    >
                        Samples
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        style={styles.tableCell}
                        sorted={
                            sortColumn === 'total_sequencing_groups' ? sortDirection : undefined
                        }
                        onClick={() => handleSort('total_sequencing_groups')}
                    >
                        Sequencing Groups
                    </Table.HeaderCell>
                    <Table.HeaderCell
                        style={styles.tableCell}
                        sorted={sortColumn === 'total_crams' ? sortDirection : undefined}
                        onClick={() => handleSort('total_crams')}
                    >
                        CRAMs
                    </Table.HeaderCell>
                    <Table.HeaderCell style={styles.tableCell}>% Aligned</Table.HeaderCell>
                    <Table.HeaderCell style={styles.tableCell}>
                        % in Annotated Dataset
                    </Table.HeaderCell>
                    <Table.HeaderCell style={styles.tableCell}>% in SNV-Index</Table.HeaderCell>
                    <Table.HeaderCell style={styles.tableCell}>% in SV-Index</Table.HeaderCell>
                </Table.Row>
            </Table.Header>
            <Table.Body>
                {sortedData.map((stats) => (
                    <StatsTableRow
                        key={`${stats.dataset}-${stats.sequencing_type}`}
                        stats={stats}
                    />
                ))}
            </Table.Body>
            <Table.Footer>
                <Table.Row key="grandTotals" style={styles.grandTotalRow}>
                    <Table.Cell style={styles.tableCell}>Grand Total</Table.Cell>
                    <Table.Cell style={styles.tableCell}>{sortedData.length} entries</Table.Cell>
                    <Table.Cell style={styles.tableCell}>
                        {filteredData.reduce((acc, curr) => acc + curr.total_families, 0)}
                    </Table.Cell>
                    <Table.Cell style={styles.tableCell}>
                        {filteredData.reduce((acc, curr) => acc + curr.total_participants, 0)}
                    </Table.Cell>
                    <Table.Cell style={styles.tableCell}>
                        {filteredData.reduce((acc, curr) => acc + curr.total_samples, 0)}
                    </Table.Cell>
                    <Table.Cell style={styles.tableCell}>
                        {filteredData.reduce((acc, curr) => acc + curr.total_sequencing_groups, 0)}
                    </Table.Cell>
                    <Table.Cell style={styles.tableCell}>
                        {filteredData.reduce((acc, curr) => acc + curr.total_crams, 0)}
                    </Table.Cell>
                    <Table.Cell style={styles.tableCell}>
                        <div style={{ display: 'flex', justifyContent: 'center' }}>
                            <HtmlTooltip
                                title={
                                    <React.Fragment>
                                        <div>
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
                                        </div>
                                    </React.Fragment>
                                }
                            >
                                <div>
                                    {(
                                        (filteredData.reduce(
                                            (acc, curr) => acc + curr.total_crams,
                                            0
                                        ) /
                                            filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            )) *
                                        100
                                    ).toFixed(2)}
                                    %
                                </div>
                            </HtmlTooltip>
                        </div>
                    </Table.Cell>
                    <Table.Cell style={styles.tableCell}>
                        <div style={{ display: 'flex', justifyContent: 'center' }}>
                            <HtmlTooltip
                                title={
                                    <React.Fragment>
                                        <div>
                                            {filteredData.reduce(
                                                (acc, curr) =>
                                                    acc + curr.latest_annotate_dataset.sg_count,
                                                0
                                            )}{' '}
                                            /{' '}
                                            {filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            )}{' '}
                                            Total Sequencing Groups in the latest AnnotateDataset
                                            analysis
                                        </div>
                                    </React.Fragment>
                                }
                            >
                                <div>
                                    {(
                                        (filteredData.reduce(
                                            (acc, curr) =>
                                                acc + curr.latest_annotate_dataset.sg_count,
                                            0
                                        ) /
                                            filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            )) *
                                        100
                                    ).toFixed(2)}
                                    %
                                </div>
                            </HtmlTooltip>
                        </div>
                    </Table.Cell>
                    <Table.Cell style={styles.tableCell}>
                        <div style={{ display: 'flex', justifyContent: 'center' }}>
                            <HtmlTooltip
                                title={
                                    <React.Fragment>
                                        <div>
                                            {filteredData.reduce(
                                                (acc, curr) =>
                                                    acc + curr.latest_snv_es_index.sg_count,
                                                0
                                            )}{' '}
                                            /{' '}
                                            {filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            )}{' '}
                                            Total Sequencing Groups in the latest SNV Elasticsearch
                                            Index
                                        </div>
                                    </React.Fragment>
                                }
                            >
                                <div>
                                    {(
                                        (filteredData.reduce(
                                            (acc, curr) => acc + curr.latest_snv_es_index.sg_count,
                                            0
                                        ) /
                                            filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            )) *
                                        100
                                    ).toFixed(2)}
                                    %
                                </div>
                            </HtmlTooltip>
                        </div>
                    </Table.Cell>
                    <Table.Cell style={styles.tableCell}>
                        <div style={{ display: 'flex', justifyContent: 'center' }}>
                            <HtmlTooltip
                                title={
                                    <React.Fragment>
                                        <div>
                                            {filteredData.reduce(
                                                (acc, curr) =>
                                                    acc + curr.latest_sv_es_index.sg_count,
                                                0
                                            )}{' '}
                                            /{' '}
                                            {filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            )}{' '}
                                            Total Sequencing Groups in the latest SV Elasticsearch
                                            Index
                                        </div>
                                    </React.Fragment>
                                }
                            >
                                <div>
                                    {(
                                        (filteredData.reduce(
                                            (acc, curr) => acc + curr.latest_sv_es_index.sg_count,
                                            0
                                        ) /
                                            filteredData.reduce(
                                                (acc, curr) => acc + curr.total_sequencing_groups,
                                                0
                                            )) *
                                        100
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

export default StatsTable
