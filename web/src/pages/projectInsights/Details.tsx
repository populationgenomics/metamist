// Details.tsx
import React, { useEffect, useState } from 'react'
import { Button } from 'semantic-ui-react'
import { EnumsApi, ProjectApi, ProjectInsightsApi, ProjectInsightsDetails } from '../../sm-api'
import DetailsTable from './DetailsTable'
import ProjectAndSeqTypeSelector from './ProjectAndSeqTypeSelector'

const Details: React.FC = () => {
    // Get the list of  projects from the project API
    const [allData, setAllData] = useState<ProjectInsightsDetails[]>([])
    const [projectNames, setProjectNames] = React.useState<string[]>([])
    const [projectIds, setProjectIds] = React.useState<number[]>([])
    const [selectedProjects, setSelectedProjects] = React.useState<string[]>([])
    const [fetchedProjects, setFetchedProjects] = React.useState<string[]>([])
    // New state for sequencing types
    const [seqTypes, setSeqTypes] = React.useState<string[]>([])
    const [selectedSeqTypes, setSelectedSeqTypes] = React.useState<string[]>([])
    const [fetchedSeqTypes, setFetchedSeqTypes] = React.useState<string[]>([])
    // All the filters users can apply to the data
    const [selectedSeqPlatforms, setSelectedSeqPlatforms] = React.useState<string[]>([])
    const [selectedSeqTechnologies, setSelectedSeqTechnologies] = React.useState<string[]>([])
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
                setProjectNames(projects.map((project) => project.name))
                setProjectIds(projects.map((project) => project.id))
            })
            .catch((error) => {
                // Handle any errors that occur during the API calls
                console.error('Error fetching data:', error)
            })
    }, [])

    const handleProjectChange = (selectedProjects: string[]) => {
        setSelectedProjects(selectedProjects)
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

    const fetchSelectedData = async () => {
        try {
            const detailsResp = await new ProjectInsightsApi().getProjectInsightsDetails({
                project_names: selectedProjects,
                sequencing_types: selectedSeqTypes,
            })
            setAllData(detailsResp.data)
            setFetchedProjects(selectedProjects)
            setFetchedSeqTypes(selectedSeqTypes)
        } catch (error) {
            console.error('Error fetching selected data:', error)
        }
    }

    const filteredData = allData.filter(
        (item) =>
            fetchedProjects.some((p) => p === item.dataset) &&
            fetchedSeqTypes.includes(item.sequencing_type) &&
            (selectedSeqPlatforms.length === 0 ||
                selectedSeqPlatforms.includes(item.sequencing_platform)) &&
            (selectedSeqTechnologies.length === 0 ||
                selectedSeqTechnologies.includes(item.sequencing_technology)) &&
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
                selectedCompletedCram.includes(item.completed_cram ? 'Yes' : 'No')) &&
            (selectedInLatestAnnotateDataset.length === 0 ||
                selectedInLatestAnnotateDataset.includes(
                    item.in_latest_annotate_dataset ? 'Yes' : 'No'
                )) &&
            (selectedInLatestSnvEsIndex.length === 0 ||
                selectedInLatestSnvEsIndex.includes(item.in_latest_snv_es_index ? 'Yes' : 'No')) &&
            (selectedInLatestSvEsIndex.length === 0 ||
                selectedInLatestSvEsIndex.includes(item.in_latest_sv_es_index ? 'Yes' : 'No')) &&
            (selectedStripy.length === 0 ||
                (selectedStripy.includes('Yes') && item.web_reports?.stripy) ||
                (selectedStripy.includes('No') && !item.web_reports?.stripy)) &&
            (selectedMito.length === 0 ||
                (selectedMito.includes('Yes') && item.web_reports?.mito) ||
                (selectedMito.includes('No') && !item.web_reports?.mito))
    )
    const handleSelectionChange = (columnName: string, selectedOptions: string[]) => {
        // Update the selected options for the given column based on the state variable
        switch (columnName) {
            case 'sample_type':
                setSelectedSampleTypes(selectedOptions)
                break
            case 'sequencing_platform':
                setSelectedSeqPlatforms(selectedOptions)
                break
            case 'sequencing_technology':
                setSelectedSeqTechnologies(selectedOptions)
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
            case 'sample_ext_ids':
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
        <>
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
                allData={allData}
                filteredData={filteredData}
                selectedProjects={selectedProjects}
                selectedSeqTypes={selectedSeqTypes}
                selectedSeqPlatforms={selectedSeqPlatforms}
                selectedSeqTechnologies={selectedSeqTechnologies}
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
            ></DetailsTable>
        </>
    )
}

export default Details
