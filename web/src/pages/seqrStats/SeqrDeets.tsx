// SeqrDeets.tsx
import React, { useState, useEffect } from 'react'
import { SeqrProjectsDetails, ProjectApi, SeqrProjectsStatsApi, EnumsApi } from '../../sm-api'
import ProjectAndSeqTypeSelector from './ProjectAndSeqTypeSelector'
import SeqrProjectsDetailsTable from './SeqrProjectsDetailsTable'

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
    const [selectedSampleTypes, setSelectedSampleTypes] = React.useState<string[]>([])
    const [selectedFamilyIds, setSelectedFamilyIds] = React.useState<string[]>([])
    const [selectedFamilyExtIds, setSelectedFamilyExtIds] = React.useState<string[]>([])
    const [selectedParticipantIds, setSelectedParticipantIds] = React.useState<string[]>([])
    const [selectedParticipantExtIds, setSelectedParticipantExtIds] = React.useState<string[]>([])
    const [selectedSampleIds, setSelectedSampleIds] = React.useState<string[]>([])
    const [selectedSampleExtIds, setSelectedSampleExtIds] = React.useState<string[]>([])
    const [selectedSequencingGroupIds, setSelectedSequencingGroupIds] = React.useState<string[]>([])
    const [selectedCompletedCram, setSelectedCompletedCram] = React.useState<string[]>([])
    const [selectedInLatestAnnotateDataset, setSelectedInLatestAnnotateDataset] = React.useState<
        string[]
    >([])
    const [selectedInLatestSnvEsIndex, setSelectedInLatestSnvEsIndex] = React.useState<string[]>([])
    const [selectedInLatestSvEsIndex, setSelectedInLatestSvEsIndex] = React.useState<string[]>([])
    const [selectedStripy, setSelectedStripy] = React.useState<string[]>([])
    const [selectedMito, setSelectedMito] = React.useState<string[]>([])

    useEffect(() => {
        Promise.all([new EnumsApi().getSequencingTypes(), new ProjectApi().getSeqrProjects({})])
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
            (selectedSampleTypes.length === 0 || selectedSampleTypes.includes(item.sample_type)) &&
            (selectedFamilyIds.length === 0 ||
                selectedFamilyIds.includes(item.family_id?.toString() || '')) &&
            (selectedFamilyExtIds.length === 0 ||
                selectedFamilyExtIds.includes(item.family_ext_id)) &&
            (selectedParticipantIds.length === 0 ||
                selectedParticipantIds.includes(item.participant_id?.toString() || '')) &&
            (selectedParticipantExtIds.length === 0 ||
                selectedParticipantExtIds.includes(item.participant_ext_id)) &&
            (selectedSampleIds.length === 0 || selectedSampleIds.includes(item.sample_id)) &&
            (selectedSampleExtIds.length === 0 ||
                selectedSampleExtIds.includes(item.sample_ext_ids[0])) &&
            (selectedSequencingGroupIds.length === 0 ||
                selectedSequencingGroupIds.includes(item.sequencing_group_id)) &&
            (selectedCompletedCram.length === 0 ||
                (selectedCompletedCram.includes('Yes') && item.completed_cram) ||
                (selectedCompletedCram.includes('No') && !item.completed_cram)) &&
            (selectedInLatestAnnotateDataset.length === 0 ||
                (selectedInLatestAnnotateDataset.includes('Yes') &&
                    item.in_latest_annotate_dataset) ||
                (selectedInLatestAnnotateDataset.includes('No') &&
                    !item.in_latest_annotate_dataset)) &&
            (selectedInLatestSnvEsIndex.length === 0 ||
                (selectedInLatestSnvEsIndex.includes('Yes') && item.in_latest_snv_es_index) ||
                (selectedInLatestSnvEsIndex.includes('No') && !item.in_latest_snv_es_index)) &&
            (selectedInLatestSvEsIndex.length === 0 ||
                (selectedInLatestSvEsIndex.includes('Yes') && item.in_latest_sv_es_index) ||
                (selectedInLatestSvEsIndex.includes('No') && !item.in_latest_sv_es_index)) &&
            (selectedStripy.length === 0 ||
                (selectedStripy.includes('Yes') && item.sequencing_group_report_links?.stripy) ||
                (selectedStripy.includes('No') && !item.sequencing_group_report_links?.stripy)) &&
            (selectedMito.length === 0 ||
                (selectedMito.includes('Yes') && item.sequencing_group_report_links?.mito) ||
                (selectedMito.includes('No') && !item.sequencing_group_report_links?.mito))
    )
    const handleSelectionChange = (columnName: string, selectedOptions: string[]) => {
        // Update the selected options for the given column based on the state variable
        switch (columnName) {
            case 'sample_type':
                setSelectedSampleTypes(selectedOptions)
                break
            case 'family_id':
                setSelectedFamilyIds(selectedOptions)
                break
            case 'family_ext_id':
                setSelectedFamilyExtIds(selectedOptions)
                break
            case 'participant_id':
                setSelectedParticipantIds(selectedOptions)
                break
            case 'participant_ext_id':
                setSelectedParticipantExtIds(selectedOptions)
                break
            case 'sample_id':
                setSelectedSampleIds(selectedOptions)
                break
            case 'sample_ext_id':
                setSelectedSampleExtIds(selectedOptions)
                break
            case 'sequencing_group_id':
                setSelectedSequencingGroupIds(selectedOptions)
                break
            case 'completed_cram':
                setSelectedCompletedCram(selectedOptions)
                break
            case 'in_latest_annotate_dataset':
                setSelectedInLatestAnnotateDataset(selectedOptions)
                break
            case 'in_latest_snv_es_index':
                setSelectedInLatestSnvEsIndex(selectedOptions)
                break
            case 'in_latest_sv_es_index':
                setSelectedInLatestSvEsIndex(selectedOptions)
                break
            case 'stripy':
                setSelectedStripy(selectedOptions)
                break
            case 'mito':
                setSelectedMito(selectedOptions)
                break
            default:
                break
        }
    }

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
            <SeqrProjectsDetailsTable
                allData={allData}
                filteredData={filteredData}
                selectedProjects={selectedProjects}
                selectedSeqTypes={selectedSeqTypes}
                selectedSampleTypes={selectedSampleTypes}
                selectedFamilyIds={selectedFamilyIds}
                selectedFamilyExtIds={selectedFamilyExtIds}
                selectedParticipantIds={selectedParticipantIds}
                selectedParticipantExtIds={selectedParticipantExtIds}
                selectedSampleIds={selectedSampleIds}
                selectedSampleExtIds={selectedSampleExtIds}
                selectedSequencingGroupIds={selectedSequencingGroupIds}
                selectedCompletedCram={selectedCompletedCram}
                selectedInLatestAnnotateDataset={selectedInLatestAnnotateDataset}
                selectedInLatestSnvEsIndex={selectedInLatestSnvEsIndex}
                selectedInLatestSvEsIndex={selectedInLatestSvEsIndex}
                selectedStripy={selectedStripy}
                selectedMito={selectedMito}
                handleSelectionChange={handleSelectionChange}
            ></SeqrProjectsDetailsTable>
        </div>
    )
}

export default SeqrDeets
