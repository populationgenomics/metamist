import React, { useContext } from 'react'
import { Form } from 'semantic-ui-react'

import { ThemeContext } from '../../shared/components/ThemeProvider'

import { SequencingGroup } from './types'

interface IAddFromIdListForm {
    onAdd: (sequencingGroups: SequencingGroup[]) => void
}

const AddFromIdListForm: React.FC<IAddFromIdListForm> = ({ onAdd }) => {
    const { theme } = useContext(ThemeContext)
    const inverted = theme === 'dark-mode'

    const handleInput = () => {
        const element = document.getElementById('sequencing-group-ids-csv') as HTMLInputElement

        if (!element == null) {
            onAdd([])
            return
        }

        if (!element?.value || !element.value.trim()) {
            onAdd([])
            return
        }

        const ids = element.value.trim().split(',')
        const sgs: SequencingGroup[] = ids.map((id: string) => ({
            id: id.trim(),
            type: '?',
            technology: '?',
            platform: '?',
            project: { id: -1, name: '?' },
        }))

        onAdd(sgs.filter((sg) => sg.id !== ''))
    }

    return (
        <Form inverted={inverted}>
            <h3>Add by Sequencing Group ID</h3>
            <p>Input a comma-separated list of valid Sequencing Group IDs</p>
            <Form.TextArea
                id="sequencing-group-ids-csv"
                label="Sequencing Group ID"
                placeholder="Comma separated list of Sequencing Group IDs"
            />
            <Form.Button type="button" content="Add" onClick={handleInput} />
        </Form>
    )
}

export default AddFromIdListForm
