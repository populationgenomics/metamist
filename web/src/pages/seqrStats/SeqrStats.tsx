import React, { useState, useEffect } from 'react'
import { SeqrProjectsSummary, ProjectApi, SeqrProjectsStatsApi, EnumsApi } from '../../sm-api'
import ProjectAndSeqTypeSelector from './ProjectAndSeqTypeSelector'
import StatsTable from './StatsTable'

interface SelectedProject {
    id: number
    name: string
}

const InsightsStats: React.FC = () => {
    // Get the list of seqr projects from the project API
    const [allData, setAllData] = useState<SeqrProjectsSummary[]>([])
    const [seqrProjectNames, setSeqrProjectNames] = React.useState<string[]>([])
    const [seqrProjectIds, setSeqrProjectIds] = React.useState<number[]>([])
    const [selectedProjects, setSelectedProjects] = React.useState<SelectedProject[]>([])
    // New state for sequencing types
    const [seqTypes, setSeqTypes] = React.useState<string[]>([])
    const [selectedSeqTypes, setSelectedSeqTypes] = React.useState<string[]>([])

    useEffect(() => {
        Promise.all([new EnumsApi().getSequencingTypes(), new ProjectApi().getSeqrProjects({})])
            .then(([seqTypesResp, projectsResp]) => {
                const sequencingTypes: string[] = seqTypesResp.data
                setSeqTypes(sequencingTypes)

                const projects: { id: number; name: string }[] = projectsResp.data
                setSeqrProjectNames(projects.map((project) => project.name))
                setSeqrProjectIds(projects.map((project) => project.id))

                // Call getProjectsInsightsStats with the project IDs and sequencing types
                return new SeqrProjectsStatsApi().getSeqrProjectsStats({
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
            (item.total_families > 0 ||
                item.total_participants > 0 ||
                item.total_samples > 0 ||
                item.total_sequencing_groups > 0 ||
                item.total_crams > 0)
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
            <StatsTable filteredData={filteredData}> </StatsTable>
        </div>
    )
}

export default InsightsStats
