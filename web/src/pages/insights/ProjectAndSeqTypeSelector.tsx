import React from 'react'
import { Checkbox, Dropdown, DropdownProps } from 'semantic-ui-react'
import { Project } from '../../sm-api'

interface PresetConfig {
    label: string
    getItems: (projects: Project[]) => string[]
}

interface ProjectSelectorProps {
    projects: Project[]
    selectedProjects: string[]
    onSelectionChange: (selectedProjects: string[]) => void
}

interface SeqTypeSelectorProps {
    seqTypes: string[]
    selectedSeqTypes: string[]
    onSelectionChange: (selectedSeqTypes: string[]) => void
}

interface ProjectAndSeqTypeSelectorProps {
    projects: Project[]
    selectedProjects: string[]
    seqTypes: string[]
    selectedSeqTypes: string[]
    onProjectChange: (selectedProjects: string[]) => void
    onSeqTypeChange: (selectedSeqTypes: string[]) => void
}

const PROJECT_PRESETS: PresetConfig[] = [
    {
        label: 'All projects (excluding test)',
        getItems: (projects) =>
            projects.filter((p) => !p.name.endsWith('-test')).map((p) => p.name),
    },
    {
        label: 'Only seqr projects',
        getItems: (projects) =>
            projects
                .filter((p) => {
                    const meta = p.meta as Record<string, unknown> | null | undefined
                    return meta?.is_seqr
                })
                .map((p) => p.name),
    },
]

const SEQ_TYPE_PRESETS: { label: string; items: string[] }[] = [
    {
        label: 'WGS & WES only',
        items: ['genome', 'exome'],
    },
]

const ProjectSelector: React.FC<ProjectSelectorProps> = ({
    projects,
    selectedProjects,
    onSelectionChange,
}) => {
    const projectNames = projects.map((p) => p.name)
    const options = projectNames.map((name) => ({ key: name, text: name, value: name }))

    const handleChange = (_: React.SyntheticEvent<HTMLElement>, data: DropdownProps) => {
        onSelectionChange(data.value as string[])
    }

    const handlePresetToggle = (presetItems: string[]) => {
        const allSelected = presetItems.every((item) => selectedProjects.includes(item))
        if (allSelected) {
            onSelectionChange(selectedProjects.filter((p) => !presetItems.includes(p)))
        } else {
            onSelectionChange([...new Set([...selectedProjects, ...presetItems])])
        }
    }

    return (
        <div style={{ flex: '1', marginRight: '20px' }}>
            <h2 style={{ fontSize: '18px', marginBottom: '10px' }}>Projects</h2>
            <Dropdown
                placeholder="Select Projects"
                fluid
                multiple
                search
                selection
                options={options}
                value={selectedProjects}
                onChange={handleChange}
                style={{ marginBottom: '10px' }}
            />
            {PROJECT_PRESETS.map((preset) => {
                const presetItems = preset.getItems(projects)
                if (presetItems.length === 0) return null
                return (
                    <div key={preset.label} style={{ marginBottom: '4px' }}>
                        <Checkbox
                            label={`${preset.label} (${presetItems.length})`}
                            checked={
                                presetItems.length > 0 &&
                                presetItems.every((item) => selectedProjects.includes(item))
                            }
                            onChange={() => handlePresetToggle(presetItems)}
                        />
                    </div>
                )
            })}
        </div>
    )
}

const SeqTypeSelector: React.FC<SeqTypeSelectorProps> = ({
    seqTypes,
    selectedSeqTypes,
    onSelectionChange,
}) => {
    const options = seqTypes.map((st) => ({ key: st, text: st, value: st }))

    const handleChange = (_: React.SyntheticEvent<HTMLElement>, data: DropdownProps) => {
        onSelectionChange(data.value as string[])
    }

    const handlePresetToggle = (presetItems: string[]) => {
        const availableItems = presetItems.filter((item) => seqTypes.includes(item))
        const allSelected = availableItems.every((item) => selectedSeqTypes.includes(item))
        if (allSelected) {
            onSelectionChange(selectedSeqTypes.filter((st) => !availableItems.includes(st)))
        } else {
            onSelectionChange([...new Set([...selectedSeqTypes, ...availableItems])])
        }
    }

    return (
        <div style={{ flex: '1' }}>
            <h2 style={{ fontSize: '18px', marginBottom: '10px' }}>Sequencing Types</h2>
            <Dropdown
                placeholder="Select Sequencing Types"
                fluid
                multiple
                search
                selection
                options={options}
                value={selectedSeqTypes}
                onChange={handleChange}
                style={{ marginBottom: '10px' }}
            />
            {SEQ_TYPE_PRESETS.map((preset) => {
                const availableItems = preset.items.filter((item) => seqTypes.includes(item))
                if (availableItems.length === 0) return null
                return (
                    <div key={preset.label} style={{ marginBottom: '4px' }}>
                        <Checkbox
                            label={preset.label}
                            checked={
                                availableItems.length > 0 &&
                                availableItems.every((item) => selectedSeqTypes.includes(item))
                            }
                            onChange={() => handlePresetToggle(availableItems)}
                        />
                    </div>
                )
            })}
        </div>
    )
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
            <ProjectSelector
                projects={projects}
                selectedProjects={selectedProjects}
                onSelectionChange={onProjectChange}
            />
            <SeqTypeSelector
                seqTypes={seqTypes}
                selectedSeqTypes={selectedSeqTypes}
                onSelectionChange={onSeqTypeChange}
            />
        </div>
    )
}

export default ProjectAndSeqTypeSelector
