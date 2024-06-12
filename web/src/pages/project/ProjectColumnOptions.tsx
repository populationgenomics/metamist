import * as _ from 'lodash'
import * as React from 'react'

import {
    Accordion,
    AccordionTitle,
    Card,
    Checkbox,
    Grid,
    Message,
    Segment,
} from 'semantic-ui-react'
import { ProjectParticipantGridFilter, ProjectParticipantGridResponse } from '../../sm-api'
import { JsonEditor } from './JsonEditor'

export enum MetaSearchEntityPrefix {
    F = 'family',
    P = 'participant',
    S = 'sample',
    Sg = 'sequencing_group',
    A = 'assay',
}

export interface ProjectGridHeaderGroup {
    category: MetaSearchEntityPrefix
    fields: ProjectGridField[]
}

export interface ProjectGridField {
    name: string // key
    title: string // display name
    isVisible?: boolean
}

interface ProjectColumnOptionsProps {
    headerGroups: ProjectGridHeaderGroup[]
    filterValues: ProjectParticipantGridFilter
    participantCount: number
    // participantResponse?: ProjectParticipantGridResponse
    updateFilters: (filters: Partial<ProjectParticipantGridFilter>) => void
    setHeaderGroups: (headers: ProjectGridHeaderGroup[]) => void
}

export const defaultHeaderGroupsFromResponse: (
    summary: ProjectParticipantGridResponse
) => ProjectGridHeaderGroup[] = (summary) => {
    let headerGroups: ProjectGridHeaderGroup[] = [
        {
            category: MetaSearchEntityPrefix.F,
            fields: [
                {
                    name: 'external_id',
                    title: 'Family ID',
                    isVisible: true,
                },
            ],
        },
        {
            category: MetaSearchEntityPrefix.P,
            fields: summary.participant_keys.map((field, i) => ({
                name: field[0],
                title: field[1],
                isVisible: true,
            })),
        },
        {
            category: MetaSearchEntityPrefix.S,
            fields: summary.sample_keys.map((field, i) => ({
                name: field[0],
                title: field[1],
                isVisible: true,
            })),
        },
        {
            category: MetaSearchEntityPrefix.Sg,
            fields: summary.sequencing_group_keys.map((field, i) => ({
                name: field[0],
                title: `${field[1]}`,
                isVisible: true,
            })),
        },
        {
            category: MetaSearchEntityPrefix.A,
            fields: summary.assay_keys.map((field, i) => ({
                name: field[0],
                title: `${field[1]}`,
                isVisible: true,
            })),
        },
    ]

    return headerGroups
}

export const ProjectColumnOptions: React.FC<ProjectColumnOptionsProps> = ({
    headerGroups,
    setHeaderGroups,
    filterValues,
    updateFilters,
    participantCount,
}) => {
    const [isOpen, setIsOpen] = React.useState(false)
    // something weird about the accordian
    // const isOpen = _isOpen ? true : undefined

    const onUpdateSingleHeader = (
        category: MetaSearchEntityPrefix,
        header: ProjectGridField,
        isChecked: boolean
    ) => {
        const newHeaders = headerGroups.map((group) => {
            if (group.category !== category) {
                return group
            }
            // find the specific field, and set isVisible to isChecked
            return {
                ...group,
                fields: group.fields.map((field) => {
                    if (field.name !== header.name) {
                        return field
                    }
                    return {
                        ...field,
                        isVisible: isChecked,
                    }
                }),
            }
        })
        setHeaderGroups(newHeaders)
    }

    return (
        <Card
            style={{
                width: screen.width,
                padding: '20px',
                backgroundColor: 'var(--color-bg-card)',
            }}
        >
            <Accordion>
                <AccordionTitle active={isOpen} onClick={() => setIsOpen(!isOpen)}>
                    <h3>Filter + display options</h3>
                </AccordionTitle>
                <Accordion.Content active={isOpen}>
                    {participantCount > 200 && (
                        <Message warning>
                            There are a high number of participants ({participantCount}), showing /
                            hiding columns may take a few seconds to process, and the UI might
                            appear to freeze
                        </Message>
                    )}
                    <JsonEditor jsonObj={filterValues} onChange={updateFilters} />
                    <br />
                    <Grid container divided>
                        {headerGroups.map(({ category, fields }) => {
                            return (
                                <Segment
                                    key={`project-col-option-${category}`}
                                    style={{ marginLeft: '10px' }}
                                >
                                    <h3>{_.startCase(category.replaceAll('_', ' '))}</h3>
                                    <table
                                        style={{
                                            border: 'none', // '1px solid black',
                                        }}
                                    >
                                        <tbody>
                                            {fields.map((field) => {
                                                return (
                                                    <tr key={field.name}>
                                                        <td>
                                                            <Checkbox
                                                                key={`checkbox-${category}-${field.name}`}
                                                                type="checkbox"
                                                                checked={field.isVisible}
                                                                label={field.title}
                                                                onChange={(e, data) =>
                                                                    onUpdateSingleHeader(
                                                                        category,
                                                                        field,
                                                                        data.checked ||
                                                                            !field.isVisible
                                                                    )
                                                                }
                                                            />
                                                        </td>
                                                        {/* <td style={{ paddingLeft: '10px' }}>
                                                            <ValueFilter
                                                                category={category}
                                                                filterKey={field.name}
                                                                filterValues={filterValues}
                                                                updateFilterValues={updateFilters}
                                                                size="small"
                                                            />
                                                        </td> */}
                                                    </tr>
                                                )
                                            })}
                                        </tbody>
                                    </table>
                                </Segment>
                            )
                        })}
                    </Grid>
                </Accordion.Content>
            </Accordion>
        </Card>
    )
}
