// ProjectAndSeqTypeSelector.tsx
import React from 'react'
import { Checkbox } from 'semantic-ui-react'

interface ProjectAndSeqTypeSelectorProps {
    projects: { id: number; name: string }[]
    selectedProjects: { id: number; name: string }[]
    seqTypes: string[]
    selectedSeqTypes: string[]
    onProjectChange: (projects: { id: number; name: string }[], isSelected: boolean[]) => void
    onSeqTypeChange: (seqTypes: string[], isSelected: boolean[]) => void
}

const handleSelectAllProjects = (
    projects: { id: number; name: string }[],
    onProjectChange: (projects: { id: number; name: string }[], isSelected: boolean[]) => void,
    selectAll: boolean
) => {
    let newSelectedProjects: { id: number; name: string }[] = projects
    let isSelected: boolean[] = newSelectedProjects.map(() => selectAll)
    onProjectChange(newSelectedProjects, isSelected)
}

const handleSelectAllSeqTypes = (
    seqTypes: string[],
    onSeqTypeChange: (seqTypes: string[], isSelected: boolean[]) => void,
    selectAll: boolean
) => {
    let newSelectedSeqTypes: string[] = seqTypes
    let isSelected: boolean[] = newSelectedSeqTypes.map(() => selectAll)
    onSeqTypeChange(newSelectedSeqTypes, isSelected)
}

const handleSelectMultipleTypes = (
    seqTypes: string[],
    onSeqTypeChange: (seqTypes: string[], isSelected: boolean[]) => void,
    selectedSeqTypes: string[],
    selectMultiple: boolean
) => {
    // If not select all, then select only "selectedSeqTypes" and unselect the rest
    let newSelectedSeqTypes: string[] = selectedSeqTypes
    let seqTypesToUnselect: string[] = seqTypes.filter(
        (seqType) => !selectedSeqTypes.includes(seqType)
    )
    let isSelected: boolean[] = newSelectedSeqTypes.map(() => selectMultiple)
    isSelected = isSelected.concat(seqTypesToUnselect.map(() => false))
    onSeqTypeChange(newSelectedSeqTypes.concat(seqTypesToUnselect), isSelected)
}

const ProjectAndSeqTypeSelector: React.FC<ProjectAndSeqTypeSelectorProps> = ({
    projects,
    seqTypes,
    selectedProjects,
    selectedSeqTypes,
    onProjectChange,
    onSeqTypeChange,
}) => {
    return (
        <div style={{ display: 'flex', justifyContent: 'space-between', padding: '20px' }}>
            <div style={{ flex: '1', marginRight: '20px' }}>
                <h2 style={{ fontSize: '18px', marginBottom: '10px' }}>Select Projects</h2>
                <div style={{ marginBottom: '10px' }}>
                    <Checkbox
                        className="seq-type-project-checkbox"
                        label="Select All"
                        checked={selectedProjects.length === projects.length}
                        onChange={(_, data) =>
                            handleSelectAllProjects(
                                projects,
                                onProjectChange,
                                data.checked ?? false
                            )
                        }
                    />
                </div>
                <div style={{ maxHeight: '200px', overflowY: 'auto', columns: 2 }}>
                    {projects.map((project) => (
                        <div key={project.id}>
                            <Checkbox
                                className="seq-type-project-checkbox"
                                label={project.name}
                                checked={selectedProjects.some((p) => p.id === project.id)}
                                onChange={(_, data) => {
                                    const isChecked = data.checked ?? false
                                    const updatedSelectedProjects = isChecked
                                        ? [...selectedProjects, project]
                                        : selectedProjects.filter((p) => p.id !== project.id)
                                    onProjectChange(
                                        updatedSelectedProjects,
                                        updatedSelectedProjects.map(() => true)
                                    )
                                }}
                            />
                        </div>
                    ))}
                </div>
            </div>
            <div style={{ flex: '1' }}>
                <h2 style={{ fontSize: '18px', marginBottom: '10px' }}>Select Sequencing Types</h2>
                <div style={{ marginBottom: '10px' }}>
                    <Checkbox
                        className="seq-type-project-checkbox"
                        style={{ marginRight: '50px' }}
                        label="Select All"
                        checked={selectedSeqTypes.length === seqTypes.length}
                        onChange={(_, data) =>
                            handleSelectAllSeqTypes(
                                seqTypes,
                                onSeqTypeChange,
                                data.checked ?? false
                            )
                        }
                    />
                    <Checkbox
                        className="seq-type-project-checkbox"
                        style={{ marginLeft: '50px' }}
                        label="WGS & WES Only"
                        labelColor="blue"
                        // When this button is clicked, the 'genome' and 'exome' checkboxes should be checked and the rest should be unchecked
                        checked={
                            selectedSeqTypes.includes('genome') &&
                            selectedSeqTypes.includes('exome') &&
                            selectedSeqTypes.length === 2
                        }
                        onChange={(_, data) =>
                            handleSelectMultipleTypes(
                                seqTypes,
                                onSeqTypeChange,
                                ['genome', 'exome'],
                                data.checked ?? false
                            )
                        }
                    />
                </div>
                <div style={{ maxHeight: '200px', overflowY: 'auto' }}>
                    {seqTypes.map((seqType) => (
                        <div key={seqType}>
                            <Checkbox
                                className="seq-type-project-checkbox"
                                label={seqType}
                                checked={selectedSeqTypes.includes(seqType)}
                                onChange={(_, data) =>
                                    onSeqTypeChange([seqType], [data.checked ?? false])
                                }
                            />
                        </div>
                    ))}
                </div>
            </div>
        </div>
    )
}

export default ProjectAndSeqTypeSelector
