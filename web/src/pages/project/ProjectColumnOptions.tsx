import * as _ from 'lodash'
import * as React from 'react'

import DragHandle from '@mui/icons-material/DragHandle'
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
                                    <CategoryColumnOptions
                                        category={headerGroup}
                                        fields={headerGroups[headerGroup]}
                                        filterValues={filterValues}
                                        updateFilters={updateFilters}
                                        updateFields={(fields) => {
                                            setHeaderGroups({
                                                ...headerGroups,
                                                [headerGroup]: fields,
                                            })
                                        }}
                                    />
                                </Segment>
                            )
                        })}
                    </Grid>
                </Accordion.Content>
            </Accordion>
        </Card>
    )
}

const CategoryColumnOptions: React.FC<{
    category: MetaSearchEntityPrefix
    fields: ProjectParticipantGridField[]
    filterValues: ProjectParticipantGridFilter
    updateFilters: (filters: Partial<ProjectParticipantGridFilter>) => void
    updateFields: (fields: ProjectParticipantGridField[]) => void
}> = ({ category, fields, filterValues, updateFilters, updateFields }) => {
    const [tempFields, setTempFields] = React.useState<ProjectParticipantGridField[] | null>(null)
    const [draggedToIndex, setDraggedToIndex] = React.useState<number | null>(null)
    const [draggedRowIndex, setDraggedRowIndex] = React.useState<number | null>(null)

    const updateChecked = (header: ProjectParticipantGridField, isChecked: boolean) => {
        const newFields = fields.map((field) => {
            // find the specific field, and set isVisible to isChecked
            if (field.key !== header.key) {
                return field
            }
            return {
                ...field,
                is_visible: isChecked,
            }
        })
        updateFields(newFields)
    }

    const start = (e: React.DragEvent<HTMLTableRowElement>) => {
        console.log('start', e.target)
        setDraggedRowIndex(e.currentTarget.rowIndex)
    }
    const dragEnd = (e: React.DragEvent<HTMLTableRowElement>) => {
        console.log('end', e.target)
        setDraggedRowIndex(null)
        if (tempFields === null) return
        updateFields(tempFields)
        setTempFields(null)
    }
    const dragover = (e: React.DragEvent<HTMLTableRowElement>) => {
        if (draggedRowIndex === null) return
        e.preventDefault()

        const to = e.currentTarget.rowIndex
        if (to === draggedToIndex) return

        // update setTempFields with new order
        const newFields = fields.filter((_, idx) => idx !== draggedRowIndex)
        // insert fields[draggedRowIndex] at e.currentTarget.rowIndex
        newFields.splice(to, 0, fields[draggedRowIndex])

        setDraggedToIndex(to)
        setTempFields(newFields)
    }

    const _fields = tempFields || fields
    return (
        <>
            <h3>{_.startCase(metaSearchEntityToTitle(category))}</h3>
            <table
                style={{
                    border: 'none', // '1px solid black',
                }}
            >
                <tbody>
                    {_fields?.map((field) => {
                        return (
                            <tr
                                key={field.key}
                                onDragStart={start}
                                onDragOver={dragover}
                                onDragEnd={dragEnd}
                            >
                                <td draggable>
                                    <DragHandle />
                                </td>
                                <td>
                                    <Checkbox
                                        // key={`checkbox-${headerGroup}-${field.key}`}
                                        type="checkbox"
                                        checked={field.is_visible}
                                        label={field.label}
                                        onChange={(e, data) =>
                                            updateChecked(field, data.checked || !field.is_visible)
                                        }
                                    />
                                </td>
                                <td style={{ paddingLeft: '10px' }}>
                                    <ValueFilter
                                        category={metaSeachEntityPrefixToFilterKey(category)}
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
        </>
    )
}
