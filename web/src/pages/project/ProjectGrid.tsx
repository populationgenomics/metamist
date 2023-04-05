import * as React from 'react'
import _ from 'lodash'
import { Table as SUITable, Form, Popup } from 'semantic-ui-react'

import FilterAltIcon from '@mui/icons-material/FilterAlt'
import FilterAltOutlinedIcon from '@mui/icons-material/FilterAltOutlined'
import CloseIcon from '@mui/icons-material/Close'
import { IconButton } from '@mui/material'
import Table from '../../shared/components/Table'
import SampleLink from '../../shared/components/links/SampleLink'
import FamilyLink from '../../shared/components/links/FamilyLink'
import sanitiseValue from '../../shared/utilities/sanitiseValue'
import { ProjectSummaryResponse, MetaSearchEntityPrefix } from '../../sm-api/api'

interface ProjectGridProps {
    summary: ProjectSummaryResponse
    projectName: string
    filterValues: Record<
        string,
        { value: string; category: MetaSearchEntityPrefix; title: string; field: string }
    >
    updateFilters: (e: {
        [k: string]: {
            value: string
            category: MetaSearchEntityPrefix
            title: string
            field: string
        }
    }) => void
}

const ProjectGrid: React.FunctionComponent<ProjectGridProps> = ({
    summary,
    projectName,
    filterValues,
    updateFilters,
}) => {
    let headers = [
        { name: 'external_id', title: 'Family ID', category: MetaSearchEntityPrefix.F },
        ...summary.participant_keys.map((field) => ({
            category: MetaSearchEntityPrefix.P,
            name: field[0],
            title: field[1],
        })),
        ...summary.sample_keys.map((field) => ({
            category: MetaSearchEntityPrefix.S,
            name: field[0],
            title: field[1],
        })),
        ...summary.sequence_keys.map((field) => ({
            category: MetaSearchEntityPrefix.Sq,
            name: field[0],
            title: `sequence.${field[1]}`,
        })),
    ]

    const [tempFilterValues, setTempFilterValues] =
        React.useState<
            Record<
                string,
                { value: string; category: MetaSearchEntityPrefix; title: string; field: string }
            >
        >(filterValues)

    const onFilterValueChange = (
        e: React.ChangeEvent<HTMLInputElement>,
        category: MetaSearchEntityPrefix,
        title: string
    ) => {
        const { name } = e.target
        const { value } = e.target
        setTempFilterValues({
            ...Object.keys(tempFilterValues)
                .filter((key) => `${category}.${name}` !== key)
                .reduce((res, key) => Object.assign(res, { [key]: tempFilterValues[key] }), {}),
            ...(value && { [`${category}.${name}`]: { value, category, title, field: name } }),
        })
    }

    const onClear = (column: string, category: MetaSearchEntityPrefix) => {
        updateFilters({
            ...Object.keys(tempFilterValues)
                .filter((key) => `${category}.${column}` !== key)
                .reduce((res, key) => Object.assign(res, { [key]: tempFilterValues[key] }), {}),
        })
    }

    const onSubmit = () => {
        updateFilters(tempFilterValues)
    }

    if (summary.participants.length === 0 && Object.keys(filterValues).length) {
        headers = Object.entries(filterValues).map(([, { field, category, title }]) => ({
            name: field,
            category,
            title,
        }))
    }

    return (
        <Table celled>
            <SUITable.Header>
                <SUITable.Row>
                    {headers.map(({ name, category, title }, i) => {
                        if (title === 'Sample ID' || title === 'Created date') {
                            return (
                                <SUITable.HeaderCell
                                    key={`filter-${name}-${i}`}
                                    style={{ borderBottom: 'none' }}
                                ></SUITable.HeaderCell>
                            )
                        }
                        return (
                            <SUITable.HeaderCell
                                key={`filter-${title}-${i}`}
                                style={{ borderBottom: 'none' }}
                            >
                                <div style={{ position: 'relative' }}>
                                    <div style={{ position: 'absolute', top: 0, right: 0 }}>
                                        <Popup
                                            position="top center"
                                            trigger={
                                                `${category}.${name}` in filterValues ? (
                                                    <FilterAltIcon />
                                                ) : (
                                                    <FilterAltOutlinedIcon />
                                                )
                                            }
                                            hoverable
                                        >
                                            <Form onSubmit={onSubmit}>
                                                <Form.Group
                                                    inline
                                                    style={{ padding: 0, margin: 0 }}
                                                >
                                                    <Form.Field style={{ padding: 0, margin: 0 }}>
                                                        <Form.Input
                                                            action={{ icon: 'search' }}
                                                            placeholder="Filter..."
                                                            name={name}
                                                            value={
                                                                tempFilterValues[
                                                                    `${category}.${name}`
                                                                ]?.value || ''
                                                            }
                                                            onChange={(e) =>
                                                                onFilterValueChange(
                                                                    e,
                                                                    category,
                                                                    title
                                                                )
                                                            }
                                                        />
                                                    </Form.Field>
                                                    {`${category}.${name}` in filterValues && (
                                                        <Form.Field style={{ padding: 0 }}>
                                                            <IconButton
                                                                onClick={() =>
                                                                    onClear(name, category)
                                                                }
                                                                style={{ padding: 0 }}
                                                            >
                                                                <CloseIcon />
                                                            </IconButton>
                                                        </Form.Field>
                                                    )}
                                                </Form.Group>
                                            </Form>
                                        </Popup>
                                    </div>
                                </div>
                            </SUITable.HeaderCell>
                        )
                    })}
                </SUITable.Row>
            </SUITable.Header>
            <SUITable.Header>
                <SUITable.Row>
                    {headers.map(({ name, title }, i) => (
                        <SUITable.HeaderCell key={`${name}-${i}`}>{title}</SUITable.HeaderCell>
                    ))}
                </SUITable.Row>
            </SUITable.Header>
            <SUITable.Body>
                {summary.participants.map((p, pidx) =>
                    p.samples.map((s, sidx) => {
                        const backgroundColor =
                            pidx % 2 === 0 ? 'var(--color-bg)' : 'var(--color-bg-disabled)'
                        const lengthOfParticipant = p.samples
                            .map((s_) => s_.sequences.length)
                            .reduce((a, b) => a + b, 0)
                        return s.sequences.map((seq, seqidx) => {
                            const isFirstOfGroup = sidx === 0 && seqidx === 0
                            return (
                                <SUITable.Row key={`${p.external_id}-${s.id}-${seq.id}`}>
                                    {isFirstOfGroup && (
                                        <SUITable.Cell
                                            style={{ backgroundColor }}
                                            rowSpan={lengthOfParticipant}
                                        >
                                            {
                                                <FamilyLink
                                                    id={p.families.map((f) => f.id).join(', ')}
                                                    projectName={projectName}
                                                >
                                                    {p.families
                                                        .map((f) => f.external_id)
                                                        .join(', ')}
                                                </FamilyLink>
                                            }
                                        </SUITable.Cell>
                                    )}
                                    {isFirstOfGroup &&
                                        summary.participant_keys.map(([k]) => (
                                            <SUITable.Cell
                                                style={{
                                                    backgroundColor,
                                                }}
                                                key={`${p.id}participant.${k}`}
                                                rowSpan={lengthOfParticipant}
                                            >
                                                {sanitiseValue(_.get(p, k))}
                                            </SUITable.Cell>
                                        ))}
                                    {seqidx === 0 &&
                                        summary.sample_keys.map(([k]) => (
                                            <SUITable.Cell
                                                style={{
                                                    backgroundColor,
                                                }}
                                                key={`${s.id}sample.${k}`}
                                                rowSpan={s.sequences.length}
                                            >
                                                {k === 'external_id' || k === 'id' ? (
                                                    <SampleLink id={s.id} projectName={projectName}>
                                                        {sanitiseValue(_.get(s, k))}
                                                    </SampleLink>
                                                ) : (
                                                    sanitiseValue(_.get(s, k))
                                                )}
                                            </SUITable.Cell>
                                        ))}
                                    {seq &&
                                        summary.sequence_keys.map(([k]) => (
                                            <SUITable.Cell
                                                style={{
                                                    backgroundColor,
                                                }}
                                                key={`${s.id}sequence.${k}`}
                                            >
                                                {sanitiseValue(_.get(seq, k))}
                                            </SUITable.Cell>
                                        ))}
                                </SUITable.Row>
                            )
                        })
                    })
                )}
            </SUITable.Body>
        </Table>
    )
}

export default ProjectGrid
