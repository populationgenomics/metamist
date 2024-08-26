import startCase from 'lodash/startCase'
import * as React from 'react'

import DragHandle from '@mui/icons-material/DragHandle'
import FilterAltIcon from '@mui/icons-material/FilterAlt'
import HelpIcon from '@mui/icons-material/Help'

import {
    Accordion,
    AccordionTitle,
    Button,
    Card,
    Checkbox,
    Grid,
    Message,
    Modal,
    Segment,
} from 'semantic-ui-react'
import {
    MetaSearchEntityPrefix,
    ProjectParticipantGridField,
    ProjectParticipantGridFilter,
    ProjectParticipantGridResponse,
} from '../../sm-api'
import { DictEditor, DictEditorInput } from './DictEditor'
import { ProjectGridFilterGuide } from './ProjectGridFilterGuide'
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
    const [isHelpOpen, setIsHelpOpen] = React.useState<boolean>(false)
    return (
        <>
            <Card
                style={{
                    width: screen.width,
                    padding: '20px',
                    backgroundColor: 'var(--color-bg-card)',
                }}
            >
                <Accordion>
                    <AccordionTitle active={isOpen} onClick={() => setIsOpen(!isOpen)}>
                        <h3>
                            Filter + display options{' '}
                            <Button
                                circular
                                style={{ padding: 0, marginLeft: '10px' }}
                                onClick={() => setIsHelpOpen(true)}
                            >
                                <HelpIcon />
                            </Button>
                        </h3>
                    </AccordionTitle>
                    <Accordion.Content active={isOpen}>
                        {participantCount > 200 && (
                            <Message warning>
                                There are a high number of participants ({participantCount}),
                                showing / hiding columns may take a few seconds to process, and the
                                UI might appear to freeze.
                            </Message>
                        )}
                        <Message info>
                            To filter the data, you can use the <FilterAltIcon /> button at the top
                            of each column. Or get{' '}
                            <Button
                                circular
                                style={{ padding: 0 }}
                                onClick={() => setIsHelpOpen(true)}
                            >
                                <HelpIcon />
                            </Button>
                        </Message>
                        <DictEditor
                            input={filterValues as DictEditorInput}
                            onChange={updateFilters}
                        />
                        <br />
                        <Grid container divided>
                            {headerGroupOrder.map((headerGroup) => {
                                return (
                                    <Segment
                                        key={`project-col-option-${headerGroup}`}
                                        style={{ width: '45%', margin: '10px' }}
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
            <Modal
                style={{ height: 'unset', top: '50px', left: 'unset' }}
                open={isHelpOpen}
                onClose={() => setIsHelpOpen(false)}
            >
                <Modal.Header>Help with project grid filtering</Modal.Header>
                <Modal.Content>
                    <div style={{ padding: '10px 40px' }}>
                        <ProjectGridFilterGuide
                            headerGroups={headerGroups}
                            filterValues={filterValues}
                            updateFilters={updateFilters}
                        />
                    </div>
                </Modal.Content>
                <Modal.Actions>
                    <Button onClick={() => setIsHelpOpen(false)}>Close</Button>
                </Modal.Actions>
            </Modal>
        </>
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

    const updateAll = (isChecked: boolean) => {
        const newFields = fields.map((field) => ({
            ...field,
            is_visible: isChecked,
        }))
        updateFields(newFields)
    }
    const updateSome = (headers: ProjectParticipantGridField[], isChecked: boolean) => {
        const fieldsToUpdate = new Set(headers.map((h) => h.key))
        const newFields = fields.map((field) => {
            if (!fieldsToUpdate.has(field.key)) {
                return field
            }
            return {
                ...field,
                is_visible: isChecked,
            }
        })
        updateFields(newFields)
    }
    const updateAllFromEvent: (isChecked: boolean) => React.MouseEventHandler<HTMLAnchorElement> =
        (isChecked: boolean) => (e?: React.MouseEvent<HTMLAnchorElement>) => {
            e?.preventDefault?.()
            updateAll(isChecked)
        }

    const updateSomeFromEvent: (
        headers: ProjectParticipantGridField[],
        isChecked: boolean
    ) => React.MouseEventHandler<HTMLAnchorElement> =
        (headers: ProjectParticipantGridField[], isChecked: boolean) =>
        (e?: React.MouseEvent<HTMLAnchorElement>) => {
            e?.preventDefault?.()
            updateSome(headers, isChecked)
        }

    const start: React.DragEventHandler<HTMLTableRowElement> = (
        e: React.DragEvent<HTMLTableRowElement>
    ) => {
        setDraggedRowIndex(e.currentTarget.rowIndex)
    }
    const dragEnd = (e: React.DragEvent<HTMLTableRowElement>) => {
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
    const _nonMetaFields = _fields?.filter((f) => !f.key.startsWith('meta.'))
    const _metaFields = _fields?.filter((f) => f.key.startsWith('meta.'))
    return (
        <>
            <h3>{startCase(metaSearchEntityToTitle(category))}</h3>
            <p>
                <a href="" onClick={updateAllFromEvent(true)}>
                    Select all
                </a>{' '}
                |{' '}
                <a href="" onClick={updateAllFromEvent(false)}>
                    Select none
                </a>
            </p>
            <table style={{ border: 'none', width: '100%' }}>
                <tbody>
                    {_nonMetaFields?.map((field) => (
                        <HeaderRow
                            category={category}
                            field={field}
                            start={start}
                            dragover={dragover}
                            dragEnd={dragEnd}
                            updateChecked={updateChecked}
                            filterValues={filterValues}
                            updateFilters={updateFilters}
                        />
                    ))}
                </tbody>
            </table>

            {_metaFields?.length > 0 && (
                <>
                    <hr />
                    <p>
                        <b>Meta fields: </b>
                        <a href="" onClick={updateSomeFromEvent(_metaFields, true)}>
                            Select all
                        </a>{' '}
                        |{' '}
                        <a href="" onClick={updateSomeFromEvent(_metaFields, false)}>
                            Select none
                        </a>
                    </p>
                    <table style={{ border: 'none', width: '100%' }}>
                        <tbody>
                            {_metaFields?.map((field) => (
                                <HeaderRow
                                    category={category}
                                    field={field}
                                    start={start}
                                    dragover={dragover}
                                    dragEnd={dragEnd}
                                    updateChecked={updateChecked}
                                    filterValues={filterValues}
                                    updateFilters={updateFilters}
                                />
                            ))}
                        </tbody>
                    </table>
                </>
            )}
        </>
    )
}

interface IFieldOptionRow {
    category: MetaSearchEntityPrefix
    field: ProjectParticipantGridField

    start: React.DragEventHandler<HTMLTableRowElement>
    dragover: React.DragEventHandler<HTMLTableRowElement>
    dragEnd: React.DragEventHandler<HTMLTableRowElement>
    updateChecked: (field: ProjectParticipantGridField, isChecked: boolean) => void
    filterValues: ProjectParticipantGridFilter
    updateFilters: (e: ProjectParticipantGridFilter) => void
}
const HeaderRow: React.FC<IFieldOptionRow> = ({
    category,
    field,
    start,
    dragover,
    dragEnd,
    updateChecked,
    filterValues,
    updateFilters,
}) => {
    return (
        <tr key={field.key} onDragStart={start} onDragOver={dragover} onDragEnd={dragEnd}>
            <td draggable>
                <DragHandle />
            </td>
            <td>
                <Checkbox
                    // key={`checkbox-${headerGroup}-${field.key}`}
                    type="checkbox"
                    checked={field.is_visible}
                    label={field.label}
                    onChange={(e, data) => updateChecked(field, data.checked || !field.is_visible)}
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
}
