import React, { useState, useContext } from 'react'
import { Container, Form } from 'semantic-ui-react'
import { useQuery } from '@apollo/client'

import { gql } from '../../__generated__/gql'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import MuckError from '../../shared/components/MuckError'

import SequencingGroupTable from './SequencingGroupTable'
import { SequencingGroup } from './types'
import { setSearchParams } from '../../sm-api/common'

const GET_PROJECTS_QUERY = gql(`
query VisibleProjects($activeOnly: Boolean!) {
    myProjects {
        id
        name
        sequencingGroups(activeOnly: {eq: $activeOnly}) {
            id
            type
            technology
            platform
        }
    }
}`)

interface IAddFromProjectForm {
    onAdd: (sequencingGroups: SequencingGroup[]) => void
}

const AddFromProjectForm: React.FC<IAddFromProjectForm> = ({ onAdd }) => {
    const [sequencingGroups, setSequencingGroups] = useState<SequencingGroup[]>([])
    const [searchHits, setSearchHits] = useState<SequencingGroup[]>([])

    const { theme } = useContext(ThemeContext)
    const inverted = theme === 'dark-mode'

    // Load all available projects and associated data for this user
    const { loading, error, data } = useQuery(GET_PROJECTS_QUERY, {
        variables: { activeOnly: true },
    })

    if (loading) {
        return <div>Loading project form...</div>
    }

    if (error) {
        return <MuckError message={error.message} />
    }

    const projectOptions: { key: any; value: any; text: any }[] | undefined = []
    if (!loading && data?.myProjects) {
        data.myProjects.forEach((project) => {
            projectOptions.push({ key: project.id, value: project.id, text: project.name })
        })
    }

    const search = () => {
        const seqType = (document.getElementById('seq_type') as HTMLInputElement)?.value?.trim()
        const technology = (
            document.getElementById('technology') as HTMLInputElement
        )?.value?.trim()
        const platform = (document.getElementById('platform') as HTMLInputElement)?.value?.trim()
        // const batch = (document.getElementById('batch') as HTMLInputElement)?.value?.trim()

        const hits = sequencingGroups
        setSearchHits(
            hits.filter(
                (sg: SequencingGroup) =>
                    (!seqType || sg.type.toLowerCase().includes(seqType.toLowerCase())) &&
                    (!technology ||
                        sg.technology.toLowerCase().includes(technology.toLowerCase())) &&
                    (!platform || sg.platform.toLowerCase().includes(platform.toLowerCase()))
            )
        )
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const updateSequencingGroupSelection = (_: any, d: any) => {
        if (!data?.myProjects) {
            setSequencingGroups([])
            return
        }

        const newSequencingGroups: SequencingGroup[] = []
        data.myProjects.forEach((project) => {
            if (d.value.includes(project.id)) {
                project.sequencingGroups.forEach((sg: SequencingGroup) => {
                    newSequencingGroups.push({
                        id: sg.id,
                        type: sg.type,
                        technology: sg.technology,
                        platform: sg.platform,
                        project: { id: project.id, name: project.name },
                    })
                })
            }
        })

        setSequencingGroups(newSequencingGroups)
    }

    return (
        <Form inverted={inverted}>
            <h3>Project</h3>
            <p>Include Sequencing Groups from the following projects</p>
            <Form.Dropdown
                placeholder="Select Projects"
                fluid
                multiple
                selection
                onChange={updateSequencingGroupSelection}
                options={projectOptions}
            />

            <p>
                Including the Sequencing Groups which match the following criteria (leave blank to
                include all Sequencing Groups)
            </p>
            <Container>
                <Form.Input label="Sequencing Type" name="seq_type" id="seq_type" />
                <Form.Input label="Technology" name="technology" id="technology" />
                <Form.Input label="Platform" name="platform" id="platform" />
                <Form.Input label="Assay Batch" name="batch" id="batch" />
            </Container>
            <br />
            <Form.Group>
                <Form.Button type="button" content="Search" onClick={search} />
                <Form.Button
                    type="button"
                    content="Add"
                    onClick={() => {
                        onAdd(searchHits)
                    }}
                />
            </Form.Group>
            <br />
            <SequencingGroupTable editable={false} sequencingGroups={searchHits} />
        </Form>
    )
}

export default AddFromProjectForm
