// SeqrDeets.tsx
import React, { useState, useEffect } from 'react'
import { SeqrProjectsDetails, ProjectApi, SeqrProjectsStatsApi, EnumsApi } from '../../sm-api'
import ProjectAndSeqTypeSelector from './ProjectAndSeqTypeSelector'
import SeqrProjectsDetailsTable from './SeqrProjectsDetailsTable'
import RowIdFilterModal from './SearchAndFilterModals'
import { Button } from 'semantic-ui-react'

interface SelectedProject {
    id: number
    name: string
}

const SeqrDeets: React.FC = () => {
    // Get the list of seqr projects from the project API
    const [allData, setAllData] = useState<SeqrProjectsDetails[]>([])
    const [seqrProjectNames, setSeqrProjectNames] = React.useState<string[]>([])
    const [seqrProjectIds, setSeqrProjectIds] = React.useState<number[]>([])
    const [selectedProjects, setSelectedProjects] = React.useState<SelectedProject[]>([])
    // New state for sequencing types
    const [seqTypes, setSeqTypes] = React.useState<string[]>([])
    const [selectedSeqTypes, setSelectedSeqTypes] = React.useState<string[]>([])
    // All the filters users can apply to the data
    const [selectedFamilyIds, setSelectedFamilyIds] = React.useState<string[]>([])
    const [selectedFamilyExtIds, setSelectedFamilyExtIds] = React.useState<string[]>([])
    const [selectedParticipantIds, setSelectedParticipantIds] = React.useState<string[]>([])
    const [selectedParticipantExtIds, setSelectedParticipantExtIds] = React.useState<string[]>([])
    const [selectedSampleIds, setSelectedSampleIds] = React.useState<string[]>([])
    const [selectedSampleExtIds, setSelectedSampleExtIds] = React.useState<string[]>([])
    const [selectedSequencingGroupIds, setSelectedSequencingGroupIds] = React.useState<string[]>([])
    const [selectedCompletedCram, setSelectedCompletedCram] = React.useState<string[]>([])
    const [selectedInLatestAnnotateDataset, setSelectedInLatestAnnotateDataset] = React.useState<string[]>([])
    const [selectedInLatestSnvEsIndex, setSelectedInLatestSnvEsIndex] = React.useState<string[]>([])
    const [selectedInLatestSvEsIndex, setSelectedInLatestSvEsIndex] = React.useState<string[]>([])

    useEffect(() => {
        Promise.all([
            new EnumsApi().getSequencingTypes(),
            new ProjectApi().getSeqrProjects({}),
        ])
            .then(([seqTypesResp, projectsResp]) => {
                const sequencingTypes: string[] = seqTypesResp.data
                setSeqTypes(sequencingTypes)
    
                const projects: { id: number; name: string }[] = projectsResp.data
                setSeqrProjectNames(projects.map((project) => project.name))
                setSeqrProjectIds(projects.map((project) => project.id))
    
                // Call getProjectsInsightsStats with the project IDs and sequencing types
                return new SeqrProjectsStatsApi().getSeqrProjectsDetails({
                    project_ids: projects.map((project) => project.id),
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
    }
    const filteredData = allData.filter(
        (item) =>
            selectedProjects.some((p) => p.name === item.dataset) &&
            selectedSeqTypes.includes(item.sequencing_type) &&
            (selectedFamilyExtIds.length === 0 || selectedFamilyExtIds.includes(item.family_ext_id))
            // && (item.family_id === null || item.family_id === '')
    )

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
            <Button onClick={() => RowIdFilterModal }>
                Filter by Family Ext ID
            </Button> 

            {/* <RowIdFilterModal rowIds={filteredData.map((item) => item.family_ext_id)} selectedRowIds={selectedFamilyExtIds} onSelectionChange={setSelectedFamilyExtIds} onClose={() => {}} /> */}
            <SeqrProjectsDetailsTable filteredData={filteredData}> </SeqrProjectsDetailsTable>
        </div>
    )
}

export default SeqrDeets