import React, { useCallback, useEffect, useState } from 'react'
import { Button } from 'semantic-ui-react'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import { EnumsApi, Project, ProjectApi, ProjectInsightsApi, ProjectInsightsDetails } from '../../sm-api'
import DetailsTable from './DetailsTable'
import filterData from './FilterData'
import ProjectAndSeqTypeSelector from './ProjectAndSeqTypeSelector'
import { useInsightsUrlState } from './useInsightsUrlState'

const Details: React.FC = () => {
    const [projects, setProjects] = useState<Project[]>([])
    const [seqTypes, setSeqTypes] = useState<string[]>([])

    const { selectedProjects, setSelectedProjects, selectedSeqTypes, setSelectedSeqTypes } =
        useInsightsUrlState(
            projects.map((p) => p.name),
            seqTypes
        )

    const [allData, setAllData] = useState<ProjectInsightsDetails[]>([])
    const { filteredData, getUniqueOptionsForColumn, updateFilter, getSelectedOptionsForColumn } =
        filterData(allData)

    const fetchSelectedData = useCallback(async () => {
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
            try {
                const [seqTypesResp, projectsResp] = await Promise.all([
                    new EnumsApi().getSequencingTypes(),
                    new ProjectApi().getAllProjects(),
                ])
                setSeqTypes(seqTypesResp.data)
                setProjects(projectsResp.data)
            } catch (error) {
                console.error('Error fetching initial data:', error)
            }
        }
        fetchInitialData()
    }, [])

    return (
        <PaddedPage>
            <ProjectAndSeqTypeSelector
                projects={projects.map((p) => p.name)}
                seqTypes={seqTypes}
                selectedProjects={selectedProjects}
                selectedSeqTypes={selectedSeqTypes}
                onProjectChange={setSelectedProjects}
                onSeqTypeChange={setSelectedSeqTypes}
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
            />
        </PaddedPage>
    )
}

export default Details
