// projectInsights/Summary.tsx
import React, { useCallback, useEffect, useState } from 'react'
import { Button } from 'semantic-ui-react'
import { EnumsApi, ProjectApi, ProjectInsightsApi, ProjectInsightsSummary } from '../../sm-api'
import filterData from './FilterData'
import ProjectAndSeqTypeSelector from './ProjectAndSeqTypeSelector'
import SummaryTable from './SummaryTable'

const Summary: React.FC = () => {
    // States for selected projects and sequencing types
    const [projectNames, setProjectNames] = React.useState<string[]>([])
    const [selectedProjects, setSelectedProjects] = React.useState<string[]>([])
    const [seqTypes, setSeqTypes] = React.useState<string[]>([])
    const [selectedSeqTypes, setSelectedSeqTypes] = React.useState<string[]>([])

    // State containing all the records fetched from the ProjectInsightsSummary API
    const [allData, setAllData] = useState<ProjectInsightsSummary[]>([])
    // Filtered data based on the selected projects, sequencing types, and sequencing technologies
    const { filteredData, updateFilter, getUniqueOptionsForColumn, getSelectedOptionsForColumn } =
        filterData<ProjectInsightsSummary>(allData)

    const handleProjectChange = useCallback((selectedProjects: string[]) => {
        setSelectedProjects(selectedProjects)
    }, [])
    const handleSeqTypeChange = useCallback((selectedSeqTypes: string[]) => {
        setSelectedSeqTypes(selectedSeqTypes)
    }, [])

    const fetchSelectedData = useCallback(async () => {
        // Fetch the summary data for the selected projects and sequencing types
        try {
            const detailsResp = await new ProjectInsightsApi().getProjectInsightsSummary({
                project_names: selectedProjects,
                sequencing_types: selectedSeqTypes,
            })
            setAllData(detailsResp.data)
        } catch (error) {
            console.error('Error fetching selected data:', error)
        }
    }, [selectedProjects, selectedSeqTypes])

    useEffect(() => {
        const fetchInitialData = async () => {
            // Fetch the sequencing types and projects for user selection
            try {
                const [seqTypesResp, projectsResp] = await Promise.all([
                    new EnumsApi().getSequencingTypes(),
                    new ProjectApi().getMyProjects({}),
                ])
                setSeqTypes(seqTypesResp.data)
                setProjectNames(projectsResp.data)
            } catch (error) {
                console.error('Error fetching initial data:', error)
            }
        }
        fetchInitialData()
    }, [])

    return (
        <>
            <ProjectAndSeqTypeSelector
                projects={projectNames}
                seqTypes={seqTypes}
                selectedProjects={selectedProjects}
                selectedSeqTypes={selectedSeqTypes}
                onProjectChange={handleProjectChange}
                onSeqTypeChange={handleSeqTypeChange}
            />
            <div style={{ paddingBottom: '20px' }}>
                <Button
                    primary
                    onClick={fetchSelectedData}
                    disabled={selectedProjects.length === 0 || selectedSeqTypes.length === 0}
                >
                    Fetch Selected Data
                </Button>
            </div>
            <SummaryTable
                filteredData={filteredData}
                handleSelectionChange={updateFilter}
                getUniqueOptionsForColumn={getUniqueOptionsForColumn}
                getSelectedOptionsForColumn={getSelectedOptionsForColumn}
            ></SummaryTable>
        </>
    )
}

export default Summary
