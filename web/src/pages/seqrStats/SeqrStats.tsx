import React, { useState, useEffect } from 'react'
import { ProjectInsightsStats, ProjectApi, EnumsApi } from '../../sm-api'
import ProjectAndSeqTypeSelector from './ProjectAndSeqTypeSelector'
import { Table, TableFooter } from 'semantic-ui-react'
import ToolTip from '@mui/material/Tooltip'
import { styled } from '@mui/material/styles'
import Tooltip, { TooltipProps, tooltipClasses } from '@mui/material/Tooltip'
// import Papa from 'papaparse'
// import { saveAs } from 'file-saver'

interface SelectedProject {
    id: number
    name: string
}

function getPercentageColor(percentage: number) {
    const red = 265 - (percentage / 100) * 85 // Reducing intensity
    const green = 180 + (percentage / 100) * 85 // Reducing intensity
    const blue = 155 // Adding more blue for a pastel tone
    return `rgb(${red}, ${green}, ${blue})`
}
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

const InsightsStats: React.FC = () => {
    // Get the list of seqr projects from the project API
    const [allData, setAllData] = useState<ProjectInsightsStats[]>([])
    const [seqrProjectNames, setSeqrProjectNames] = React.useState<string[]>([])
    const [seqrProjectIds, setSeqrProjectIds] = React.useState<number[]>([])
    const [selectedProjects, setSelectedProjects] = React.useState<SelectedProject[]>([])
    // New state for sequencing types
    const [seqTypes, setSeqTypes] = React.useState<string[]>([])
    const [selectedSeqTypes, setSelectedSeqTypes] = React.useState<string[]>([])
    const [pageNumber, setPageNumber] = React.useState<number>(0)
    const [pageSize, setPageSize] = React.useState<number>(25)
    const [sortColumn, setSortColumn] = useState<keyof ProjectInsightsStats | null>(null)
    const [sortDirection, setSortDirection] = useState<'ascending' | 'descending'>('ascending')

    useEffect(() => {
        let sequencingTypes: string[] = []

        new EnumsApi()
            .getSequencingTypes()
            .then((seqTypesResp) => {
                sequencingTypes = seqTypesResp.data
                setSeqTypes(sequencingTypes)

                return new ProjectApi().getSeqrProjects({})
            })
            .then((projectsResp) => {
                const projects: { id: number; name: string }[] = projectsResp.data
                setSeqrProjectNames(projects.map((project) => project.name))
                setSeqrProjectIds(projects.map((project) => project.id))

                // Call getProjectsInsightsStats with the project IDs and sequencing types
                return new ProjectApi().getProjectsInsightsStats({
                    projects: projects.map((project) => project.id),
                    sequencing_types: sequencingTypes,
                })
            })
            .then((statsResp) => {
                setAllData(statsResp.data)
            })
            .catch((error) => {
                // Handle any errors that occur during the API calls
                console.error('Error fetching data:', error)
            })
    }, [])

    const handleProjectChange = (projectNames: string[], isSelected: boolean[]) => {
        let newSelectedProjects: SelectedProject[] = [...selectedProjects]

        projectNames.forEach((projectName, index) => {
            if (isSelected[index]) {
                // Add the project to the list of selected projects if it is not already there
                if (!newSelectedProjects.some((p) => p.name === projectName)) {
                    const projectIndex = seqrProjectNames.indexOf(projectName)
                    newSelectedProjects.push({
                        id: seqrProjectIds[projectIndex],
                        name: projectName,
                    })
                }
            } else {
                // Remove the project from the list of selected projects
                newSelectedProjects = newSelectedProjects.filter((p) => p.name !== projectName)
            }
        })
        setSelectedProjects(newSelectedProjects)
        // Reset the page number when project selections change
        setPageNumber(0)
    }

    const handleSeqTypeChange = (seqTypes: string[], isSelected: boolean[]) => {
        let newSelectedSeqTypes: string[] = [...selectedSeqTypes]

        seqTypes.forEach((seqType, index) => {
            if (isSelected[index]) {
                // Add the sequencing type to the list of selected sequencing types if it is not already there
                if (!newSelectedSeqTypes.includes(seqType)) {
                    newSelectedSeqTypes.push(seqType)
                }
            } else {
                // Remove the sequencing type from the list of selected sequencing types
                newSelectedSeqTypes = newSelectedSeqTypes.filter((type) => type !== seqType)
            }
        })
        setSelectedSeqTypes(newSelectedSeqTypes)
        // Reset the page number when sequencing type selections change
        setPageNumber(0)
    }
    const filteredData = allData.filter(
        (item) =>
            selectedProjects.some((p) => p.name === item.dataset) &&
            selectedSeqTypes.includes(item.sequencing_type) &&
            (item.total_families > 0 ||
                item.total_participants > 0 ||
                item.total_samples > 0 ||
                item.total_sequencing_groups > 0 ||
                item.total_crams > 0)
    )
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

    const pagedResults = sortedData.slice(pageNumber * pageSize, (pageNumber + 1) * pageSize)

    // const exportTableToCSV = (exportFormat: string) => {
    //     if (exportFormat === 'csv') {
    //         const csv = Papa.unparse(sortedData)
    //         const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    //         saveAs(blob, 'table_export.csv')
    //     } else {
    //         const tsv = Papa.unparse(sortedData, { delimiter: '\t' })
    //         const blob = new Blob([tsv], { type: 'text/tab-separated-values;charset=utf-8;' })
    //         saveAs(blob, 'table_export.tsv')
    //     }
    // }
    return (
        <div>
            <ProjectAndSeqTypeSelector
                projects={seqrProjectNames.map((name, index) => ({
                    id: seqrProjectIds[index],
                    name,
                }))}
                seqTypes={seqTypes}
                selectedProjects={selectedProjects.map((p) => p.name)}
                selectedSeqTypes={selectedSeqTypes}
                onProjectChange={handleProjectChange}
                onSeqTypeChange={handleSeqTypeChange}
            />
            <h2>Page Size</h2>
            <select
                onChange={(e) => {
                    setPageSize(parseInt(e.currentTarget.value))
                    setPageNumber(0)
                }}
                value={pageSize}
            >
                {[1, 10, 25, 50, 100].map((value) => (
                    <option value={value} key={`setPageSize-${value}`}>
                        {value}
                    </option>
                ))}
            </select>
            {/* <button onClick={exportTableToCSV.bind(null, 'csv')}>Export to CSV</button>
            <button onClick={exportTableToCSV.bind(null, 'tsv')}>Export to TSV</button> */}
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
                    {pagedResults.map((ss) => {
                        // Calculate the % of sequencing groups with a completed CRAM
                        const percentageAligned =
                            ss.total_sequencing_groups > 0
                                ? (ss.total_crams / ss.total_sequencing_groups) * 100
                                : 0

                        const percentageInJointCall =
                            ss.latest_annotate_dataset.sg_count ?? 0 > 0
                                ? ((ss.latest_annotate_dataset.sg_count ?? 0) /
                                      ss.total_sequencing_groups) *
                                  100
                                : 0
                        const percentageInSnvIndex =
                            ss.latest_snv_es_index.sg_count ?? 0 > 0
                                ? ((ss.latest_snv_es_index.sg_count ?? 0) /
                                      ss.total_sequencing_groups) *
                                  100
                                : 0
                        const percentageInSvIndex =
                            ss.latest_sv_es_index.sg_count ?? 0 > 0
                                ? ((ss.latest_sv_es_index.sg_count ?? 0) /
                                      ss.total_sequencing_groups) *
                                  100
                                : 0

                        const rowStyle =
                            ss.sequencing_type === 'exome'
                                ? styles.exomeRow
                                : ss.sequencing_type === 'genome'
                                ? styles.genomeRow
                                : styles.rnaRow

                        return (
                            <Table.Row key={`${ss.project}-${ss.sequencing_type}`} style={rowStyle}>
                                <Table.Cell style={styles.tableCell}>{ss.dataset}</Table.Cell>
                                <Table.Cell style={styles.tableCell}>
                                    {ss.sequencing_type}
                                </Table.Cell>
                                <Table.Cell style={styles.tableCell}>
                                    {ss.total_families}
                                </Table.Cell>
                                <Table.Cell style={styles.tableCell}>
                                    {ss.total_participants}
                                </Table.Cell>
                                <Table.Cell style={styles.tableCell}>{ss.total_samples}</Table.Cell>
                                <Table.Cell style={styles.tableCell}>
                                    {ss.total_sequencing_groups}
                                </Table.Cell>
                                <Table.Cell style={styles.tableCell}>{ss.total_crams}</Table.Cell>
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
                                                        {ss.total_crams} /{' '}
                                                        {ss.total_sequencing_groups} Total
                                                        Sequencing Groups with a Completed CRAM
                                                        Analysis
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
                                                        {ss.latest_annotate_dataset.sg_count} /{' '}
                                                        {ss.total_sequencing_groups} Total
                                                        Sequencing Groups in the latest{' '}
                                                        {ss.sequencing_type} AnnotateDataset
                                                        analysis
                                                    </div>
                                                    <div>
                                                        Analysis ID: {ss.latest_annotate_dataset.id}
                                                    </div>
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
                                                        {ss.latest_snv_es_index.sg_count} /{' '}
                                                        {ss.total_sequencing_groups} Total
                                                        Sequencing Groups in the latest{' '}
                                                        {ss.sequencing_type} SNV Elasticsearch Index
                                                    </div>
                                                    <div>
                                                        Analysis ID: {ss.latest_snv_es_index.id}
                                                    </div>
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
                                                        {ss.latest_sv_es_index.sg_count} /{' '}
                                                        {ss.total_sequencing_groups} Total
                                                        Sequencing Groups in the latest{' '}
                                                        {ss.sequencing_type} SV Elasticsearch Index
                                                    </div>
                                                    <div>
                                                        Analysis ID: {ss.latest_sv_es_index.id}
                                                    </div>
                                                </React.Fragment>
                                            }
                                        >
                                            <div>{percentageInSvIndex.toFixed(2)}%</div>
                                        </HtmlTooltip>
                                    </div>
                                </Table.Cell>
                            </Table.Row>
                        )
                    })}

                    {/* {filteredData.length > pageSize && (
                        // TODO: Toggle to display these "Page totals"
                        <Table.Row key="pageTotals" style={styles.pageTotalRow}>
                            <Table.Cell style={styles.tableCell}>Page Total</Table.Cell>
                            <Table.Cell style={styles.tableCell}></Table.Cell>
                            <Table.Cell style={styles.tableCell}>
                                {pagedResults.reduce((acc, curr) => acc + curr.total_families, 0)}
                            </Table.Cell>
                            <Table.Cell style={styles.tableCell}>
                                {pagedResults.reduce((acc, curr) => acc + curr.total_participants, 0)}
                            </Table.Cell>
                            <Table.Cell style={styles.tableCell}>
                                {pagedResults.reduce((acc, curr) => acc + curr.total_samples, 0)}
                            </Table.Cell>
                            <Table.Cell style={styles.tableCell}>
                                {pagedResults.reduce((acc, curr) => acc + curr.total_sequencing_groups, 0)}
                            </Table.Cell>
                            <Table.Cell style={styles.tableCell}>
                                {pagedResults.reduce((acc, curr) => acc + curr.total_crams, 0)}
                            </Table.Cell>
                            <Table.Cell style={styles.tableCell}></Table.Cell>
                            <Table.Cell style={styles.tableCell}></Table.Cell>
                            <Table.Cell style={styles.tableCell}></Table.Cell>
                            <Table.Cell style={styles.tableCell}></Table.Cell>
                        </Table.Row>
                    )*/}
                </Table.Body>
                <Table.Footer>
                    <Table.Row key="grandTotals" style={styles.grandTotalRow}>
                        <Table.Cell style={styles.tableCell}>Grand Total</Table.Cell>
                        <Table.Cell style={styles.tableCell}></Table.Cell>
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
                            {filteredData.reduce(
                                (acc, curr) => acc + curr.total_sequencing_groups,
                                0
                            )}
                        </Table.Cell>
                        <Table.Cell style={styles.tableCell}>
                            {filteredData.reduce((acc, curr) => acc + curr.total_crams, 0)}
                        </Table.Cell>
                        <Table.Cell style={styles.tableCell}></Table.Cell>
                        <Table.Cell style={styles.tableCell}></Table.Cell>
                        <Table.Cell style={styles.tableCell}></Table.Cell>
                        <Table.Cell style={styles.tableCell}></Table.Cell>
                    </Table.Row>

                    {pagedResults.length > 0 && (
                        <Table.Row>
                            <Table.Cell
                                colSpan={11}
                                style={{ ...styles.tableCell, fontStyle: 'italic', color: 'gray' }}
                            >
                                {pageNumber * pageSize + 1} -{' '}
                                {Math.min((pageNumber + 1) * pageSize, filteredData.length)} /{' '}
                                {filteredData.length}
                            </Table.Cell>
                        </Table.Row>
                    )}
                </Table.Footer>
            </Table>

            <div
                style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    marginTop: '16px',
                }}
            >
                <button
                    style={{
                        padding: '8px 16px',
                        backgroundColor: pageNumber === 0 ? '#e0e0e0' : '#f0f0f0',
                        border: 'none',
                        borderRadius: '4px',
                        cursor: pageNumber === 0 ? 'not-allowed' : 'pointer',
                        marginRight: '8px',
                    }}
                    disabled={pageNumber === 0}
                    onClick={() => setPageNumber(pageNumber - 1)}
                >
                    Previous
                </button>
                <span style={{ marginRight: '8px', fontWeight: 'bold' }}>
                    Page {pageNumber + 1}
                </span>
                <button
                    style={{
                        padding: '8px 16px',
                        backgroundColor: '#f0f0f0',
                        border: 'none',
                        borderRadius: '4px',
                        cursor: 'pointer',
                    }}
                    disabled={(pageNumber + 1) * pageSize >= filteredData.length}
                    onClick={() => setPageNumber(pageNumber + 1)}
                >
                    Next
                </button>
            </div>
        </div>
    )
}

export default InsightsStats
