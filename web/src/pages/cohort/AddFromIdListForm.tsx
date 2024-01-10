import React, { useContext, useState } from 'react'
import { Form, Message } from 'semantic-ui-react'
import { useLazyQuery } from '@apollo/client'

import { gql } from '../../__generated__'
import { ThemeContext } from '../../shared/components/ThemeProvider'

import { SequencingGroup } from './types'
import SequencingGroupTable from './SequencingGroupTable'
import MuckError from '../../shared/components/MuckError'

const GET_SEQUENCING_GROUPS_QUERY = gql(`
query FetchSequencingGroupsById($ids: [String!]!) {
    sequencingGroups(id: {in_: $ids}) {
      id
      type
      technology
      platform
      sample {
        project {
          id
          name
        }
      }
    }
  }
`)

interface IAddFromIdListForm {
    onAdd: (sequencingGroups: SequencingGroup[]) => void
}

const AddFromIdListForm: React.FC<IAddFromIdListForm> = ({ onAdd }) => {
    const { theme } = useContext(ThemeContext)
    const inverted = theme === 'dark-mode'

    const [text, setText] = useState<string>('')
    const [sequencingGroups, setSequencingGroups] = useState<SequencingGroup[] | null>(null)

    const [fetchSequencingGroups, { loading, error }] = useLazyQuery(GET_SEQUENCING_GROUPS_QUERY)

    const search = () => {
        const element = document.getElementById('sequencing-group-ids-csv') as HTMLInputElement

        if (!element == null) {
            return
        }

        if (!element?.value || !element.value.trim()) {
            return
        }

        const ids = element.value.trim().split(',')
        fetchSequencingGroups({
            variables: { ids },
            onError: () => {
                setSequencingGroups(null)
            },
            onCompleted: (hits) =>
                setSequencingGroups(
                    hits.sequencingGroups.map((sg) => ({
                        ...sg,
                        project: sg.sample.project,
                    }))
                ),
        })
    }

    const addToCohort = () => {
        if (sequencingGroups == null) {
            return
        }

        onAdd(sequencingGroups)
        setSequencingGroups(null)
    }

    const renderTable = () => {
        if (loading || sequencingGroups == null) {
            return null
        }

        if (sequencingGroups.length === 0) {
            return (
                <Message warning visible>
                    No sequencing groups found matching your query
                </Message>
            )
        }

        return <SequencingGroupTable sequencingGroups={sequencingGroups} editable={false} />
    }

    return (
        <Form inverted={inverted}>
            <h3>Add by Sequencing Group ID</h3>
            <p>Input a comma-separated list of valid Sequencing Group IDs</p>
            <Form.TextArea
                id="sequencing-group-ids-csv"
                label="Sequencing Group ID"
                value={text}
                onChange={(e) => setText(e.target.value)}
                placeholder="Comma separated list of Sequencing Group IDs"
            />
            <Form.Group>
                <Form.Button
                    type="button"
                    loading={loading}
                    content="Search"
                    onClick={search}
                    disabled={!text}
                />
                <Form.Button
                    type="button"
                    content="Add"
                    disabled={
                        loading ||
                        error != null ||
                        sequencingGroups == null ||
                        sequencingGroups.length === 0
                    }
                    onClick={addToCohort}
                />
            </Form.Group>
            {error && (
                <Message color="red">
                    <MuckError message={error.message} />
                </Message>
            )}
            {renderTable()}
        </Form>
    )
}

export default AddFromIdListForm
