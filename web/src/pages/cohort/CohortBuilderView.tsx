import React, { useState, useContext } from 'react'
import { Container, Divider, Form, Tab } from 'semantic-ui-react'
import { uniqBy } from 'lodash'

import { ThemeContext } from '../../shared/components/ThemeProvider'

import SequencingGroupTable from './SequencingGroupTable'
import AddFromProjectForm from './AddFromProjectForm'
import AddFromIdListForm from './AddFromIdListForm'
import { SequencingGroup } from './types'

interface CohortFormData {
    name: string
    description: string
    sequencingGroups: SequencingGroup[]
}

const CohortBuilderView = () => {
    const { theme } = useContext(ThemeContext)
    const inverted = theme === 'dark-mode'

    // State for new cohort data
    const [cohortFormData, setCohortFormData] = useState<CohortFormData>({
        name: '',
        description: '',
        sequencingGroups: [],
    })

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
        // eslint-disable-next-line no-restricted-globals, no-alert
        const proceed = confirm(
            'Are you sure you want to create this cohort? A cohort cannot be edited once created.'
        )
        if (!proceed) return
        console.log(cohortFormData)
    }

    const tabPanes = [
        {
            menuItem: 'Project Based',
            render: () => (
                <Tab.Pane inverted={inverted}>
                    <AddFromProjectForm onAdd={addSequencingGroups} />
                </Tab.Pane>
            ),
        },
        {
            menuItem: 'Individual',
            render: () => (
                <Tab.Pane inverted={inverted}>
                    <AddFromIdListForm onAdd={addSequencingGroups} />
                </Tab.Pane>
            ),
        },
    ]

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
                    <Form.Input
                        name="name"
                        form="details-form"
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
                        from any project available to to you and specify filtering criteria to match
                        specific Sequencing Groups. You can also add sequencing groups manually by
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
                    <Form.Group>
                        <Form.Button type="submit" content="Create" onClick={createCohort} />
                        <Form.Button
                            type="button"
                            content="Clear"
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
