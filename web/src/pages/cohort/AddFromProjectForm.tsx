import React, { useState, useContext } from 'react'
import { Container, Form, Message } from 'semantic-ui-react'
import { useLazyQuery } from '@apollo/client'
import { uniq } from 'lodash'

import { gql } from '../../__generated__/gql'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import MuckError from '../../shared/components/MuckError'

import SequencingGroupTable from './SequencingGroupTable'
import { Project, SequencingGroup } from './types'
import { FetchSequencingGroupsQueryVariables } from '../../__generated__/graphql'

const GET_SEQUENCING_GROUPS_QUERY = gql(`
query FetchSequencingGroups(
    $project: String!, 
    $platform: String, 
    $technology: String, 
    $seqType: String,
    $assayMeta: JSON,
    $excludeIds: [String!]
  ) {
    project(name: $project) {
      participants {
        samples {
          assays(meta: $assayMeta) {
            sample {
              sequencingGroups(
                id: {nin: $excludeIds}
                platform: {icontains: $platform}
                technology: {icontains: $technology}
                type: {contains: $seqType}
              ) {
                id
                type
                technology
                platform
              }
            }
          }
        }
      }
    }
  }
`)

// NOTE: Put additional objects here to add more search fields for assay metadata
const assayMetaSearchFields = [
    { label: 'Batch', id: 'batch', searchVariable: 'batch' },
    // { label: 'Emoji', id: 'emoji', searchVariable: 'emoji' },
]

interface IAddFromProjectForm {
    projects: Project[]
    onAdd: (sequencingGroups: SequencingGroup[]) => void
}

const AddFromProjectForm: React.FC<IAddFromProjectForm> = ({ projects, onAdd }) => {
    const [selectedProject, setSelectedProject] = useState<Project>()
    const [searchHits, setSearchHits] = useState<SequencingGroup[] | null>(null)

    const { theme } = useContext(ThemeContext)
    const inverted = theme === 'dark-mode'

    // Load all available projects and associated data for this user
    const [searchSquencingGroups, { loading, error }] = useLazyQuery(GET_SEQUENCING_GROUPS_QUERY)

    const search = () => {
        const seqTypeInput = document.getElementById('seq_type') as HTMLInputElement
        const technologyInput = document.getElementById('technology') as HTMLInputElement
        const platformInput = document.getElementById('platform') as HTMLInputElement
        const excludeInput = document.getElementById('exclude') as HTMLInputElement

        if (!selectedProject?.name) {
            return
        }

        const searchParams: FetchSequencingGroupsQueryVariables = {
            project: selectedProject.name,
            seqType: null,
            technology: null,
            platform: null,
            excludeIds: [],
            assayMeta: {},
        }

        if (seqTypeInput?.value) {
            searchParams.seqType = seqTypeInput.value
        }

        if (technologyInput?.value) {
            searchParams.technology = technologyInput.value
        }

        if (platformInput?.value) {
            searchParams.platform = platformInput.value
        }

        if (excludeInput?.value && excludeInput.value.trim().length > 0) {
            searchParams.excludeIds = excludeInput.value
                .split(',')
                .map((id) => id.trim())
                .filter((id) => id.length > 0)
        }

        assayMetaSearchFields.forEach((field) => {
            const input = document.getElementById(field.id) as HTMLInputElement
            if (input?.value) {
                searchParams.assayMeta[field.searchVariable] = input.value
            }
        })

        searchSquencingGroups({
            variables: {
                project: selectedProject.name,
                seqType: searchParams.seqType,
                technology: searchParams.technology,
                platform: searchParams.platform,
                assayMeta: searchParams.assayMeta,
                excludeIds: searchParams.excludeIds,
            },

            onCompleted: (hits) => {
                const samples = hits.project.participants.flatMap((p) => p.samples)
                const assays = samples.flatMap((s) => s.assays)

                const sgs = assays.flatMap((a) =>
                    a.sample.sequencingGroups.map((sg) => ({
                        id: sg.id,
                        type: sg.type,
                        technology: sg.technology,
                        platform: sg.platform,
                        project: { id: selectedProject.id, name: selectedProject.name },
                    }))
                )

                // Remove duplicates by merging string fields with same id
                const merged: SequencingGroup[] = []
                const seen = new Set()
                sgs.forEach((sg) => {
                    if (!seen.has(sg.id)) {
                        merged.push(sg)
                        seen.add(sg.id)
                    } else {
                        const existing = merged.find((e) => e.id === sg.id)
                        if (existing) {
                            existing.type = `${existing.type}|${sg.type}`
                            existing.technology = `${existing.technology}|${sg.technology}`
                            existing.platform = `${existing.platform}|${sg.platform}`
                        }
                    }
                })

                setSearchHits(
                    merged.map((sg) => ({
                        ...sg,
                        type: uniq(sg.type.split('|')).sort().join(' | '),
                        technology: uniq(sg.technology.split('|')).sort().join(' | '),
                        platform: uniq(sg.platform.split('|')).sort().join(' | '),
                    }))
                )
            },
        })
    }

    const renderTable = () => {
        if (loading) {
            return (
                <>
                    <br />
                    <div>Finding sequencing groups...</div>
                </>
            )
        }

        if (searchHits == null) {
            return null
        }

        if (searchHits.length === 0) {
            return (
                <>
                    <br />
                    <b>No sequencing groups found matching your query</b>
                </>
            )
        }

        return <SequencingGroupTable sequencingGroups={searchHits} editable={false} />
    }

    const projectOptions = projects.map((project) => ({
        key: project.id,
        value: project.id,
        text: project.name,
    }))

    return (
        <Form inverted={inverted}>
            <h3>Project</h3>
            <p>Include sequencing groups from the following project</p>
            <Form.Dropdown
                placeholder="Select Projects"
                fluid
                selection
                onChange={(_, d) => {
                    const project = projects.find((p) => p.id === d.value)
                    if (!project) return
                    setSelectedProject({ id: project.id, name: project.name })
                }}
                options={projectOptions}
            />

            <p>
                Matching the following search criteria (leave blank to include all Sequencing
                Groups)
            </p>
            <Container>
                <Form.Input label="Sequencing Type" id="seq_type" />
                <Form.Input label="Technology" id="technology" />
                <Form.Input label="Platform" id="platform" />
                {assayMetaSearchFields.map((field) => (
                    <Form.Input label={field.label} id={field.id} key={field.id} />
                ))}
                <Form.Input
                    placeholder="CPG1,CPG2"
                    label="Exclude Sequencing Group IDs"
                    id="exclude"
                />
            </Container>
            <br />
            <Form.Group>
                <Form.Button
                    type="button"
                    loading={loading}
                    disabled={selectedProject == null}
                    content="Search"
                    onClick={search}
                />
                <Form.Button
                    type="button"
                    disabled={
                        loading ||
                        error != null ||
                        selectedProject == null ||
                        searchHits == null ||
                        searchHits.length === 0
                    }
                    content="Add"
                    onClick={() => {
                        if (searchHits == null) return
                        // eslint-disable-next-line no-alert
                        const proceed = window.confirm(
                            'This will add all sequencing groups in the table to your Cohort. ' +
                                'sequencing groups hidden by the interactive table search will ' +
                                'also be added. Do you wish to continue?'
                        )
                        if (proceed) {
                            onAdd(searchHits)
                            setSearchHits(null)
                        }
                    }}
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

export default AddFromProjectForm
