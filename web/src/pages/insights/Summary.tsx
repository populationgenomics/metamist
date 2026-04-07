import React, { useCallback, useEffect, useState } from 'react'
import { Button } from 'semantic-ui-react'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import { EnumsApi, Project, ProjectApi, ProjectInsightsApi, ProjectInsightsSummary } from '../../sm-api'
import filterData from './FilterData'
import ProjectAndSeqTypeSelector from './ProjectAndSeqTypeSelector'
import SummaryTable from './SummaryTable'
import { useInsightsUrlState } from './useInsightsUrlState'

const Summary: React.FC = () => {
    const [projects, setProjects] = useState<Project[]>([])
    const [seqTypes, setSeqTypes] = useState<string[]>([])

    const { selectedProjects, setSelectedProjects, selectedSeqTypes, setSelectedSeqTypes } =
        useInsightsUrlState(
            projects.map((p) => p.name),
            seqTypes
        )

    const [allData, setAllData] = useState<ProjectInsightsSummary[]>([])
    const { filteredData, updateFilter, getUniqueOptionsForColumn, getSelectedOptionsForColumn } =
        filterData<ProjectInsightsSummary>(allData)

    const fetchSelectedData = useCallback(async () => {
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
            />
        </PaddedPage>
    )
}

export default Summary
