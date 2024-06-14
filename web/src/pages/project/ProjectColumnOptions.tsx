import * as _ from 'lodash'
import * as React from 'react'

import FilterAltIcon from '@mui/icons-material/FilterAlt'

import {
    Accordion,
    AccordionTitle,
    Card,
    Checkbox,
    Grid,
    Message,
    Segment,
} from 'semantic-ui-react'
import {
    MetaSearchEntityPrefix,
    ProjectParticipantGridField,
    ProjectParticipantGridFilter,
    ProjectParticipantGridResponse,
} from '../../sm-api'
import { DictEditor } from './DictEditor'
import {
    headerGroupOrder,
    metaSeachEntityPrefixToFilterKey,
    metaSearchEntityToTitle,
} from './ProjectGridHeaderGroup'
import { ValueFilter } from './ValueFilter'

interface ProjectColumnOptionsProps {
    headerGroups: Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]>
    filterValues: ProjectParticipantGridFilter
    participantCount: number
    // participantResponse?: ProjectParticipantGridResponse
    updateFilters: (filters: Partial<ProjectParticipantGridFilter>) => void
    setHeaderGroups: (
        headers: Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]>
    ) => void

    isOpen: boolean
    setIsOpen: (isOpen: boolean) => void
}

// @ts-ignore
export const defaultHeaderGroupsFromResponse: (
    summary: ProjectParticipantGridResponse
) => Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]> = (summary) => {
    return summary.fields
}

export const ProjectColumnOptions: React.FC<ProjectColumnOptionsProps> = ({
    headerGroups,
    setHeaderGroups,
    filterValues,
    updateFilters,
    participantCount,
    isOpen,
    setIsOpen,
}) => {
    const onUpdateSingleHeader = (
        category: MetaSearchEntityPrefix,
        header: ProjectParticipantGridField,
        isChecked: boolean
    ) => {
        const newGroups: Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]> = {
            ...headerGroups,
        }
        newGroups[category] = newGroups[category].map((field) => {
            // find the specific field, and set isVisible to isChecked
            if (field.key !== header.key) {
                return field
            }
            return {
                ...field,
                is_visible: isChecked,
            }
        })
        setHeaderGroups(newGroups)
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
                    <Message info>
                        To filter the data, you can use the <FilterAltIcon /> button at the top of
                        each column.
                    </Message>
                    <DictEditor obj={filterValues} onChange={updateFilters} />
                    <br />
                    <Grid container divided>
                        {headerGroupOrder.map((headerGroup) => {
                            return (
                                <Segment
                                    key={`project-col-option-${headerGroup}`}
                                    style={{ marginLeft: '10px' }}
                                >
                                    <h3>{_.startCase(metaSearchEntityToTitle(headerGroup))}</h3>
                                    <table
                                        style={{
                                            border: 'none', // '1px solid black',
                                        }}
                                    >
                                        <tbody>
                                            {headerGroups[headerGroup]?.map((field) => {
                                                return (
                                                    <tr key={field.key}>
                                                        <td>
                                                            <Checkbox
                                                                // key={`checkbox-${headerGroup}-${field.key}`}
                                                                type="checkbox"
                                                                checked={field.is_visible}
                                                                label={field.label}
                                                                onChange={(e, data) =>
                                                                    onUpdateSingleHeader(
                                                                        headerGroup,
                                                                        field,
                                                                        data.checked ||
                                                                            !field.is_visible
                                                                    )
                                                                }
                                                            />
                                                        </td>
                                                        <td style={{ paddingLeft: '10px' }}>
                                                            <ValueFilter
                                                                category={metaSeachEntityPrefixToFilterKey(
                                                                    headerGroup
                                                                )}
                                                                field={field}
                                                                filterValues={filterValues}
                                                                updateFilterValues={updateFilters}
                                                                size="small"
                                                            />
                                                        </td>
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
