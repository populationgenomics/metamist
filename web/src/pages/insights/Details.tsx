import React, { useCallback, useEffect, useState } from 'react'
import { Button } from 'semantic-ui-react'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import { EnumsApi, ProjectApi, ProjectInsightsApi, ProjectInsightsDetails } from '../../sm-api'
import DetailsTable from './DetailsTable'
import filterData from './FilterData'
import ProjectAndSeqTypeSelector from './ProjectAndSeqTypeSelector'

const Details: React.FC = () => {
    // Projects come from the project API and the sequencing types from the enums API
    const [projectNames, setProjectNames] = React.useState<string[]>([])
    const [seqTypes, setSeqTypes] = React.useState<string[]>([])
    const [selectedProjects, setSelectedProjects] = useState<string[]>([])
    const [selectedSeqTypes, setSelectedSeqTypes] = useState<string[]>([])

    // State containing all the records fetched from the ProjectInsightsDetails API
    const [allData, setAllData] = useState<ProjectInsightsDetails[]>([])
    // Filtered data based on the selected projects, sequencing types, and column filters
    const { filteredData, getUniqueOptionsForColumn, updateFilter, getSelectedOptionsForColumn } =
        filterData(allData)

    const handleProjectChange = useCallback((selectedProjects: string[]) => {
        setSelectedProjects(selectedProjects)
    }, [])
    const handleSeqTypeChange = useCallback((selectedSeqTypes: string[]) => {
        setSelectedSeqTypes(selectedSeqTypes)
    }, [])

    const fetchSelectedData = useCallback(async () => {
        // Fetch the detailed data for the selected projects and sequencing types
        try {
            const detailsResp = await new ProjectInsightsApi().getProjectInsightsDetails({
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
        <PaddedPage>
            <ProjectAndSeqTypeSelector
                projects={projectNames}
                seqTypes={seqTypes}
                selectedProjects={selectedProjects}
                selectedSeqTypes={selectedSeqTypes}
                onProjectChange={handleProjectChange}
                onSeqTypeChange={handleSeqTypeChange}
            />
            <Button
                primary
                onClick={fetchSelectedData}
                disabled={selectedProjects.length === 0 || selectedSeqTypes.length === 0}
            >
                Fetch Selected Data
            </Button>
            <DetailsTable
                filteredData={filteredData}
                handleSelectionChange={updateFilter}
                getUniqueOptionsForColumn={getUniqueOptionsForColumn}
                getSelectedOptionsForColumn={getSelectedOptionsForColumn}
            ></DetailsTable>
        </PaddedPage>
    )
}

export default Details
