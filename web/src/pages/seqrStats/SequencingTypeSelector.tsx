import { Checkbox, CheckboxProps } from 'semantic-ui-react'

import { FormEvent } from 'react'

interface SequencingTypeSelectorProps {
    seqTypes: string[]
    onSeqTypeSelected: (seqType: string, isSelected: boolean) => void
}

const SequencingTypeSelector = ({
    seqTypes,
    onSeqTypeSelected,
}: SequencingTypeSelectorProps) => {
    const handleCheckboxChange = (event: FormEvent<HTMLInputElement>, data: CheckboxProps) => {
        const { name, checked } = data
        onSeqTypeSelected(name as string, checked || false)
    }

    return (
        <div>
            {seqTypes.map((seqType: string) => (
                <div key={seqType}>
                    <Checkbox
                        label={seqType}
                        name={seqType}
                        onChange={(e, data) => handleCheckboxChange(e, data)}
                    />
                </div>
            ))}
        </div>
    )
}

export default SequencingTypeSelector
