import React from 'react'
import { Checkbox, Dropdown, DropdownProps } from 'semantic-ui-react'

interface SelectorProps {
    items: string[]
    selectedItems: string[]
    onSelectionChange: (selectedItems: string[]) => void
    title: string
    specialSelectionLabel?: string
    specialSelectionItems?: string[]
}

interface ProjectAndSeqTypeSelectorProps {
    projects: string[]
    selectedProjects: string[]
    seqTypes: string[]
    selectedSeqTypes: string[]
    onProjectChange: (selectedProjects: string[]) => void
    onSeqTypeChange: (selectedSeqTypes: string[]) => void
}

const Selector: React.FC<SelectorProps> = ({
    items,
    selectedItems,
    onSelectionChange,
    title,
    specialSelectionLabel,
    specialSelectionItems,
}) => {
    const handleChange = (event: React.SyntheticEvent<HTMLElement>, data: DropdownProps) => {
        const value = data.value as string[]
        onSelectionChange(value)
    }

    const handleSpecialSelection = () => {
        if (specialSelectionItems) {
            const areAllSpecialItemsSelected = specialSelectionItems.every((item) =>
                selectedItems.includes(item)
            )
            if (areAllSpecialItemsSelected) {
                onSelectionChange(
                    selectedItems.filter((item) => !specialSelectionItems.includes(item))
                )
            } else {
                onSelectionChange([...new Set([...selectedItems, ...specialSelectionItems])])
            }
        }
    }

    const options = items.map((item) => ({
        key: item,
        text: item,
        value: item,
    }))

    return (
        <div style={{ flex: '1', marginRight: '20px' }}>
            <h2 style={{ fontSize: '18px', marginBottom: '10px' }}>{title}</h2>
            <Dropdown
                placeholder={`Select ${title}`}
                fluid
                multiple
                search
                selection
                options={options}
                value={selectedItems}
                onChange={handleChange}
                style={{ marginBottom: '10px' }}
            />
            {specialSelectionLabel && (
                <Checkbox
                    label={specialSelectionLabel}
                    checked={specialSelectionItems?.every((item) => selectedItems.includes(item))}
                    onChange={handleSpecialSelection}
                />
            )}
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
            <Selector
                items={projects}
                selectedItems={selectedProjects}
                onSelectionChange={onProjectChange}
                title="Projects"
                // Special selection for all projects without "-test" suffix
                specialSelectionLabel="All Projects (excluding test)"
                specialSelectionItems={projects.filter((project) => !project.endsWith('-test'))}
            />
            <Selector
                items={seqTypes}
                selectedItems={selectedSeqTypes}
                onSelectionChange={onSeqTypeChange}
                title="Sequencing Types"
                specialSelectionLabel="WGS & WES Only"
                specialSelectionItems={['genome', 'exome']}
            />
        </div>
    )
}

export default ProjectAndSeqTypeSelector
