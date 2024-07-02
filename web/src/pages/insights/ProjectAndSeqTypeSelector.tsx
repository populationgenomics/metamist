import React from 'react'
import { Checkbox } from 'semantic-ui-react'

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
    const handleSelectAll = (selectAll: boolean) => {
        onSelectionChange(selectAll ? [...items] : [])
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

    const handleItemChange = (item: string, isChecked: boolean) => {
        if (isChecked) {
            onSelectionChange([...selectedItems, item])
        } else {
            onSelectionChange(selectedItems.filter((i) => i !== item))
        }
    }

    const isSpecialSelectionChecked = specialSelectionItems
        ? specialSelectionItems.every((item) => selectedItems.includes(item))
        : false

    return (
        <div style={{ flex: '1', marginRight: '20px' }}>
            <h2 style={{ fontSize: '18px', marginBottom: '10px' }}>{title}</h2>
            <div style={{ marginBottom: '10px' }}>
                <Checkbox
                    className="seq-type-project-checkbox"
                    label="Select All"
                    checked={selectedItems.length === items.length}
                    onChange={(_, data) => handleSelectAll(data.checked ?? false)}
                />
                {specialSelectionLabel && (
                    <Checkbox
                        className="seq-type-project-checkbox"
                        style={{ marginLeft: '50px' }}
                        label={specialSelectionLabel}
                        checked={isSpecialSelectionChecked}
                        onChange={handleSpecialSelection}
                    />
                )}
            </div>
            <div
                style={{
                    maxHeight: '200px',
                    overflowY: 'auto',
                    columns: title === 'Select Projects' ? 4 : 'auto',
                    maxWidth: title === 'Select Projects' ? '400px' : 'auto',
                }}
            >
                {items.map((item) => (
                    <div key={item}>
                        <Checkbox
                            className="seq-type-project-checkbox"
                            label={item}
                            checked={selectedItems.includes(item)}
                            onChange={(_, data) => handleItemChange(item, data.checked ?? false)}
                        />
                    </div>
                ))}
            </div>
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
                title="Select Projects"
            />
            <Selector
                items={seqTypes}
                selectedItems={selectedSeqTypes}
                onSelectionChange={onSeqTypeChange}
                title="Select Sequencing Types"
                specialSelectionLabel="WGS & WES Only"
                specialSelectionItems={['genome', 'exome']}
            />
        </div>
    )
}

export default ProjectAndSeqTypeSelector
