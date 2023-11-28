import React, { useState, useContext } from 'react'
import { Container, Divider, Form, Message, Tab } from 'semantic-ui-react'
import { uniqBy } from 'lodash'
import { useQuery } from '@apollo/client'

import { gql } from '../../__generated__'
import { CohortApi, CohortBody } from '../../sm-api'

import { ThemeContext } from '../../shared/components/ThemeProvider'
import MuckError from '../../shared/components/MuckError'

import SequencingGroupTable from './SequencingGroupTable'
import AddFromProjectForm from './AddFromProjectForm'
import AddFromIdListForm from './AddFromIdListForm'
import { Project, SequencingGroup, APIError } from './types'

const ALLOW_STACK_TRACE = process.env.NODE_ENV === 'development' || process.env.NODE_ENV === 'dev'

const GET_PROJECTS_QUERY = gql(`
query GetProjectsForCohortBuilder {
    myProjects {
        id
        name
    }
}`)

const CohortBuilderView = () => {
    const { theme } = useContext(ThemeContext)
    const inverted = theme === 'dark-mode'

    // State for new cohort data
    const [createCohortError, setCreateCohortError] = useState<APIError | null>(null)
    const [createCohortSuccess, setCreateCohortSuccess] = useState<number | null>(null)
    const [createCohortLoading, setCreateCohortLoading] = useState<boolean>(false)
    const [selectedProject, setSelectedProject] = useState<Project>()

    // Keep the text fields and sequencing groups array separate to prevent re-rendering the
    // sequencing group table on every input change, which can be slow if there are many
    // sequencing groups
    const [sequencingGroups, setSequencingGroups] = useState<SequencingGroup[]>([])
    const [cohortDetails, setCohortDetails] = useState<Partial<CohortBody>>({})

    // Loading projects query for drop-down menu selection
    const { loading, error, data } = useQuery(GET_PROJECTS_QUERY)

    const addSequencingGroups = (sgs: SequencingGroup[]) => {
        setSequencingGroups(uniqBy([...sequencingGroups, ...sgs], 'id'))
    }

    const removeSequencingGroup = (id: string) => {
        setSequencingGroups(sequencingGroups.filter((sg) => sg.id !== id))
    }

    const allowSubmission =
        !loading &&
        !createCohortLoading &&
        selectedProject?.name &&
        sequencingGroups &&
        sequencingGroups.length > 0 &&
        cohortDetails.name &&
        cohortDetails.name.length > 0 &&
        cohortDetails.description &&
        cohortDetails.description.length > 0

    const createCohort = () => {
        if (!allowSubmission) return

        // Add these here because TypeScript cannot infer that if allowSubmission is true, then
        // these values are defined
        if (!cohortDetails.name) return
        if (!cohortDetails.description) return

        // eslint-disable-next-line no-alert
        const proceed = window.confirm(
            'Are you sure you want to create this cohort? A cohort cannot be edited once created.'
        )
        if (!proceed) return

        setCreateCohortLoading(true)
        setCreateCohortError(null)
        setCreateCohortSuccess(null)

        const client = new CohortApi()
        client
            .createCohort(selectedProject.name, {
                name: cohortDetails.name,
                description: cohortDetails.description,
                sequencing_group_ids: sequencingGroups.map((sg) => sg.id),
                derived_from: undefined,
            })
            .then((response) => {
                setCreateCohortSuccess(response.data.cohort_id)
            })
            .catch((e) => {
                setCreateCohortError(e.response.data)
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
        return (
            <Container>
                return <MuckError message={error.message} />
            </Container>
        )
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
                        id="cohort-project"
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
                            setSelectedProject({ id: project.id, name: project.name })
                        }}
                        options={projectOptions}
                    />
                    <Form.Input
                        id="cohort-name"
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
                        onChange={(e) => {
                            setCohortDetails({
                                ...cohortDetails,
                                name: e.target.value.trim(),
                            })
                        }}
                    />
                    <Form.Input
                        id="cohort-description"
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
                        onChange={(e) => {
                            setCohortDetails({
                                ...cohortDetails,
                                description: e.target.value.trim(),
                            })
                        }}
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
                        sequencingGroups={sequencingGroups}
                    />
                </section>
                <Divider />
                <section id="form-buttons">
                    {createCohortError && (
                        <Message negative hidden={!createCohortError}>
                            <h3>{createCohortError.name}</h3>
                            <p>{createCohortError.description}</p>
                            {ALLOW_STACK_TRACE && <pre>{createCohortError.stacktrace}</pre>}
                        </Message>
                    )}
                    {createCohortSuccess && (
                        <Message
                            positive
                            hidden={!createCohortSuccess}
                            content={createCohortSuccess}
                        >
                            <h3>Success!</h3>
                            <p>Created a new cohort with ID {createCohortSuccess}</p>
                        </Message>
                    )}
                    <br />
                    <Form.Group>
                        <Form.Button
                            type="submit"
                            content="Create"
                            color="green"
                            onClick={createCohort}
                            loading={createCohortLoading}
                            disabled={!allowSubmission}
                        />
                        <Form.Button
                            type="button"
                            content="Clear"
                            color="red"
                            disabled={sequencingGroups.length === 0}
                            loading={createCohortLoading}
                            onClick={() => {
                                // eslint-disable-next-line no-restricted-globals, no-alert
                                const proceed = confirm(
                                    "Remove all sequencing groups? This can't be undone."
                                )
                                if (!proceed) return
                                setSequencingGroups([])
                            }}
                        />
                    </Form.Group>
                </section>
            </Form>
        </Container>
    )
}

export default CohortBuilderView
