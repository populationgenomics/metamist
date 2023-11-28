import React, { useState, useContext } from 'react'
import { Container, Divider, Form, Message, Tab } from 'semantic-ui-react'
import { uniqBy } from 'lodash'
import { useQuery } from '@apollo/client'

import { gql } from '../../__generated__'
import { ThemeContext } from '../../shared/components/ThemeProvider'

import SequencingGroupTable from './SequencingGroupTable'
import AddFromProjectForm from './AddFromProjectForm'
import AddFromIdListForm from './AddFromIdListForm'
import { Project, SequencingGroup } from './types'
import MuckError from '../../shared/components/MuckError'

const GET_PROJECTS_QUERY = gql(`
query GetProjectsForCohortBuilder {
    myProjects {
        id
        name
    }
}`)

interface CohortFormData {
    name: string
    description: string
    sequencingGroups: SequencingGroup[]
}

const CohortBuilderView = () => {
    const { theme } = useContext(ThemeContext)
    const inverted = theme === 'dark-mode'

    // State for new cohort data
    const [createCohortError, setCreateCohortError] = useState<string | null>(null)
    const [createCohortSuccess, setCreateCohortSuccess] = useState<string | null>(null)
    const [createCohortLoading, setCreateCohortLoading] = useState<boolean>(false)
    const [selectedProject, setSelectedProject] = useState<Project>()
    const [cohortFormData, setCohortFormData] = useState<CohortFormData>({
        name: '',
        description: '',
        sequencingGroups: [],
    })

    // Loading projects query for drop-down menu selection
    const { loading, error, data } = useQuery(GET_PROJECTS_QUERY)

    const addSequencingGroups = (sgs: SequencingGroup[]) => {
        setCohortFormData({
            ...cohortFormData,
            sequencingGroups: uniqBy([...cohortFormData.sequencingGroups, ...sgs], 'id'),
        })
    }

    const removeSequencingGroup = (id: string) => {
        setCohortFormData({
            ...cohortFormData,
            sequencingGroups: cohortFormData.sequencingGroups.filter((sg) => sg.id !== id),
        })
    }

    const createCohort = () => {
        if (!selectedProject) return

        // eslint-disable-next-line no-alert
        const proceed = window.confirm(
            'Are you sure you want to create this cohort? A cohort cannot be edited once created.'
        )
        if (!proceed) return

        setCreateCohortLoading(true)
        setCreateCohortError(null)
        setCreateCohortSuccess(null)

        fetch(`/api/v1/cohort/${selectedProject.name}/`, {
            method: 'POST',
            body: JSON.stringify({
                name: cohortFormData.name,
                description: cohortFormData.description,
                sequencingGroups: cohortFormData.sequencingGroups.map((sg) => sg.id),
            }),
        })
            .then((response) => {
                if (!response.ok) {
                    let message = `Error creating cohort: ${response.status} (${response.status})`
                    if (response.status === 404) {
                        message = `Error creating cohort: Project not found (${response.status})`
                    }
                    setCreateCohortError(message)
                } else {
                    response
                        .json()
                        .then((d) => {
                            // eslint-disable-next-line no-alert
                            setCreateCohortSuccess(`Cohort created with ID ${d.cohort_id}`)
                        })
                        .catch((e) => {
                            // Catch JSON parsing error
                            setCreateCohortError(`Error parsing JSON response: ${e}`)
                            // eslint-disable-next-line no-console
                            console.error(e)
                        })
                        .finally(() => setCreateCohortLoading(false))
                }
            })
            .catch((e) => {
                setCreateCohortError(`An unknown error occurred while creating cohort: ${e}`)
            })
            .finally(() => setCreateCohortLoading(false))
    }

    const tabPanes = [
        {
            menuItem: 'From Project',
            render: () => (
                <Tab.Pane inverted={inverted}>
                    <AddFromProjectForm
                        onAdd={addSequencingGroups}
                        projects={data?.myProjects || []}
                    />
                </Tab.Pane>
            ),
        },
        {
            menuItem: 'By ID(s)',
            render: () => (
                <Tab.Pane inverted={inverted}>
                    <AddFromIdListForm onAdd={addSequencingGroups} />
                </Tab.Pane>
            ),
        },
    ]

    const projectOptions = (data?.myProjects ?? []).map((project) => ({
        key: project.id,
        value: project.id,
        text: project.name,
    }))

    if (error) {
        return <MuckError message={error.message} />
    }

    if (loading) {
        return (
            <Container>
                <div>Loading available projects..</div>
            </Container>
        )
    }

    return (
        <Container>
            <section id="introduction">
                <h1>Cohort Builder</h1>
                <p>
                    Welcome to the cohort builder! This form will guide you through the process of
                    creating a new cohort. You can add sequencing groups from any projects available
                    to you. Once you have created a cohort, you will not be able to edit it.
                </p>
            </section>
            <Divider />
            <Form onSubmit={createCohort} inverted={theme === 'dark-mode'} id="detauls">
                <section id="sequencing-group-details-form">
                    <h2>Details</h2>
                    <Form.Dropdown
                        name="project"
                        label={
                            <>
                                <label>Project</label>
                                <p>
                                    Select this cohort&apos;s parent project. Only those projects
                                    which are accessible to you are displayed in the drop-down menu.
                                </p>
                            </>
                        }
                        placeholder="Select Project"
                        fluid
                        selection
                        onChange={(_, d) => {
                            const project = data?.myProjects?.find((p) => p.id === d.value)
                            if (!project) return
                            setSelectedProject({ id: 1, name: 'fake' })
                        }}
                        options={projectOptions}
                    />
                    <Form.Input
                        name="name"
                        label={
                            <>
                                <label>Cohort Name</label>
                                <p>
                                    Please provide a human-readable name for this cohort. Names must
                                    be unique across all projects.
                                </p>
                            </>
                        }
                        placeholder="Cohort Name"
                        maxLength={255}
                        required
                        onChange={(e) =>
                            setCohortFormData({ ...cohortFormData, name: e.target.value })
                        }
                    />
                    <Form.Input
                        name="description"
                        label={
                            <>
                                <label>Cohort Description</label>
                                <p>
                                    Please provide a short-form description regarding this cohort.
                                </p>
                            </>
                        }
                        placeholder="Cohort Description"
                        maxLength={255}
                        required
                        onChange={(e) =>
                            setCohortFormData({
                                ...cohortFormData,
                                description: e.target.value,
                            })
                        }
                    />
                </section>
                <Divider />
                <section id="sequencing-group-form-tabs">
                    <h2>Sequencing Groups</h2>
                    <p>
                        Add sequencing groups to this cohort. You can bulk add sequencing groups
                        from any project available to to you, or add sequencing groups manually by
                        entering their IDs in a comma-separated list.
                    </p>
                    <Tab panes={tabPanes} menu={{ inverted, secondary: true, pointing: true }} />
                </section>
                <Divider />
                <section id="sequencing-group-table">
                    <h3>Selected Sequencing Groups</h3>
                    <p>
                        The table below displays the sequencing groups that will be added to this
                        cohort
                    </p>

                    <SequencingGroupTable
                        editable={true}
                        onDelete={removeSequencingGroup}
                        sequencingGroups={cohortFormData.sequencingGroups}
                    />
                </section>
                <Divider />
                <section id="form-buttons">
                    <Message negative hidden={!createCohortError} content={createCohortError} />
                    <Message positive hidden={!createCohortSuccess} content={createCohortSuccess} />
                    <br />
                    <Form.Group>
                        <Form.Button
                            type="submit"
                            content="Create"
                            color="green"
                            onClick={createCohort}
                            loading={createCohortLoading}
                            disabled={
                                loading ||
                                createCohortLoading ||
                                selectedProject == null ||
                                cohortFormData.sequencingGroups.length === 0 ||
                                !cohortFormData ||
                                !cohortFormData
                            }
                        />
                        <Form.Button
                            type="button"
                            content="Clear"
                            color="red"
                            loading={createCohortLoading}
                            onClick={() => {
                                // eslint-disable-next-line no-restricted-globals, no-alert
                                const yes = confirm(
                                    "Remove all sequencing groups? This can't be undone."
                                )
                                if (!yes) return

                                setCohortFormData({
                                    ...cohortFormData,
                                    sequencingGroups: [],
                                })
                            }}
                        />
                    </Form.Group>
                </section>
            </Form>
        </Container>
    )
}

export default CohortBuilderView
