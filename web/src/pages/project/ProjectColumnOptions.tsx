import * as _ from 'lodash'
import * as React from 'react'

import { Card, Checkbox } from 'semantic-ui-react'
import { ProjectParticipantGridFilter, ProjectParticipantGridResponse } from '../../sm-api'
import { ValueFilter } from './ValueFilter'

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
}) => {
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
        <Card>
            {headerGroups.map(({ category, fields }) => {
                return (
                    <React.Fragment key={`project-col-option-${category}`}>
                        <h3>{_.startCase(category.replaceAll('_', ' '))}</h3>
                        <table
                            style={{
                                border: '1px solid black',
                            }}
                        >
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
                                                        data.checked || !field.isVisible
                                                    )
                                                }
                                            />
                                        </td>
                                        <td style={{ paddingLeft: '10px' }}>
                                            <ValueFilter
                                                category={category}
                                                filterKey={field.name}
                                                filterValues={filterValues}
                                                updateFilterValues={updateFilters}
                                                size="small"
                                            />
                                        </td>
                                    </tr>
                                )
                            })}
                        </table>
                    </React.Fragment>
                )
            })}
        </Card>
    )
}
