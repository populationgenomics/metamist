// projectInsights/Summary.tsx
import React, { useState, useEffect } from 'react'
import { ProjectInsightsSummary, ProjectApi, ProjectInsightsApi, EnumsApi } from '../../sm-api'
import ProjectAndSeqTypeSelector from './ProjectAndSeqTypeSelector'
import SummaryTable from './SummaryTable'

interface SelectedProject {
    id: number
    name: string
}

const Summary: React.FC = () => {
    // Get the list of projects from the project API
    const [allData, setAllData] = useState<ProjectInsightsSummary[]>([])
    const [projectNames, setProjectNames] = React.useState<string[]>([])
    const [projectIds, setProjectIds] = React.useState<number[]>([])
    const [selectedProjects, setSelectedProjects] = React.useState<SelectedProject[]>([])
    // New state for sequencing types
    const [seqTypes, setSeqTypes] = React.useState<string[]>([])
    const [selectedSeqTypes, setSelectedSeqTypes] = React.useState<string[]>([])
    const [selectedSeqTechnologies, setSelectedSeqTechnologies] = React.useState<string[]>([])

    useEffect(() => {
        Promise.all([new EnumsApi().getSequencingTypes(), new ProjectApi().getSeqrProjects({})])
            .then(([seqTypesResp, projectsResp]) => {
                const sequencingTypes: string[] = seqTypesResp.data
                setSeqTypes(sequencingTypes)

                const projects: { id: number; name: string }[] = projectsResp.data
                setProjectNames(projects.map((project) => project.name))
                setProjectIds(projects.map((project) => project.id))

                // Call getProjectInsightsSummary with the project IDs and sequencing types
                return new ProjectInsightsApi().getProjectInsightsSummary({
                    project_ids: projects.map((project) => project.id),
                    sequencing_types: sequencingTypes,
                })
            })
            .then((summarysResp) => {
                setAllData(summarysResp.data)
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
                    const projectIndex = projectNames.indexOf(projectName)
                    newSelectedProjects.push({
                        id: projectIds[projectIndex],
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
            (item.total_families > 0 ||
                item.total_participants > 0 ||
                item.total_samples > 0 ||
                item.total_sequencing_groups > 0 ||
                item.total_crams > 0) &&
            (selectedSeqTechnologies.length === 0 ||
                selectedSeqTechnologies.includes(item.sequencing_technology))
    )
    const handleSelectionChange = (columnName: string, selectedOptions: string[]) => {
        // Update the selected options for the given column based on the state variable
        switch (columnName) {
            case 'sequencing_technology':
                setSelectedSeqTechnologies(selectedOptions)
                break
            default:
                break
        }
    }

    return (
        <div>
            <ProjectAndSeqTypeSelector
                projects={projectNames.map((name, index) => ({
                    id: projectIds[index],
                    name,
                }))}
                seqTypes={seqTypes}
                selectedProjects={selectedProjects.map((p) => p.name)}
                selectedSeqTypes={selectedSeqTypes}
                onProjectChange={handleProjectChange}
                onSeqTypeChange={handleSeqTypeChange}
            />
            <SummaryTable 
                allData={allData} 
                filteredData={filteredData}
                selectedProjects={selectedProjects}
                selectedSeqTypes={selectedSeqTypes}
                selectedSeqTechnologies={selectedSeqTechnologies}
                handleSelectionChange={handleSelectionChange}
            ></SummaryTable>
        </div>
    )
}

export default Summary
