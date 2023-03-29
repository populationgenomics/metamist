import * as React from 'react'
import _ from 'lodash'
import { Table, Form, Popup } from 'semantic-ui-react'

import FilterAltIcon from '@mui/icons-material/FilterAlt'
import FilterAltOutlinedIcon from '@mui/icons-material/FilterAltOutlined'
import CloseIcon from '@mui/icons-material/Close'
// import FilterAltOffIcon from '@mui/icons-material/FilterAltOff'
// import { red } from '@mui/material/colors'
import { IconButton } from '@mui/material'
import SampleLink from '../../shared/components/links/SampleLink'
import FamilyLink from '../../shared/components/links/FamilyLink'
import sanitiseValue from '../../shared/utilities/sanitiseValue'
import { ProjectSummaryResponse } from '../../sm-api/api'

interface ProjectGridProps {
    summary: ProjectSummaryResponse
    projectName: string
    filterValues: Record<string, string>
    updateFilters: (e: { [k: string]: FormDataEntryValue }) => void
}

const ProjectGrid: React.FunctionComponent<ProjectGridProps> = ({
    summary,
    projectName,
    filterValues,
    updateFilters,
}) => {
    const headers = [
        'Family ID',
        ...summary.participant_keys.map((field) => field[1]),
        ...summary.sample_keys.map((field) => field[1]),
        ...summary.sequence_keys.map((field) => `sequence.${field[1]}`),
    ]

    const [tempFilterValues, setTempFilterValues] =
        React.useState<Record<string, string>>(filterValues)

    const onChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        const { name } = e.target
        const { value } = e.target
        setTempFilterValues({
            ...Object.keys(tempFilterValues)
                .filter((key) => name !== key)
                .reduce((res, key) => Object.assign(res, { [key]: tempFilterValues[key] }), {}),
            ...(value && { [name]: value }),
        })
    }

    const onClear = (column: string) => {
        updateFilters({
            ...Object.keys(tempFilterValues)
                .filter((key) => column !== key)
                .reduce((res, key) => Object.assign(res, { [key]: tempFilterValues[key] }), {}),
        })
    }

    const onSubmit = () => {
        updateFilters(tempFilterValues)
    }

    return (
        <Table celled>
            <Table.Header>
                <Table.Row>
                    {headers.map((k, i) => {
                        if (k === 'Sample ID') {
                            return (
                                <Table.HeaderCell
                                    key={`filter-${k}-${i}`}
                                    style={{ borderBottom: 'none' }}
                                ></Table.HeaderCell>
                            )
                        }
                        return (
                            <Table.HeaderCell
                                key={`filter-${k}-${i}`}
                                style={{ borderBottom: 'none' }}
                            >
                                <div style={{ position: 'relative' }}>
                                    <div style={{ position: 'absolute', top: 0, right: 0 }}>
                                        <Popup
                                            trigger={
                                                k in filterValues ? (
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
                                                            name={k}
                                                            value={
                                                                k in tempFilterValues
                                                                    ? tempFilterValues[k]
                                                                    : ''
                                                            }
                                                            onChange={onChange}
                                                        />
                                                    </Form.Field>
                                                    {k in filterValues && (
                                                        <Form.Field style={{ padding: 0 }}>
                                                            <IconButton
                                                                onClick={() => onClear(k)}
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
                            </Table.HeaderCell>
                        )
                    })}
                </Table.Row>
                <Table.Row>
                    {headers.map((k, i) => (
                        <Table.HeaderCell key={`${k}-${i}`}>{k}</Table.HeaderCell>
                    ))}
                </Table.Row>
            </Table.Header>
            <Table.Body>
                {summary.participants.map((p, pidx) =>
                    p.samples.map((s, sidx) => {
                        const backgroundColor =
                            pidx % 2 === 0 ? 'white' : 'var(--bs-table-striped-bg)'
                        const lengthOfParticipant = p.samples
                            .map((s_) => s_.sequences.length)
                            .reduce((a, b) => a + b, 0)
                        return s.sequences.map((seq, seqidx) => {
                            const isFirstOfGroup = sidx === 0 && seqidx === 0
                            return (
                                <Table.Row key={`${p.external_id}-${s.id}-${seq.id}`}>
                                    {isFirstOfGroup && (
                                        <Table.Cell
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
                                        </Table.Cell>
                                    )}
                                    {isFirstOfGroup &&
                                        summary.participant_keys.map(([k]) => (
                                            <Table.Cell
                                                style={{
                                                    backgroundColor,
                                                }}
                                                key={`${p.id}participant.${k}`}
                                                rowSpan={lengthOfParticipant}
                                            >
                                                {sanitiseValue(_.get(p, k))}
                                            </Table.Cell>
                                        ))}
                                    {seqidx === 0 &&
                                        summary.sample_keys.map(([k]) => (
                                            <Table.Cell
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
                                            </Table.Cell>
                                        ))}
                                    {seq &&
                                        summary.sequence_keys.map(([k]) => (
                                            <Table.Cell
                                                style={{
                                                    backgroundColor,
                                                }}
                                                key={`${s.id}sequence.${k}`}
                                            >
                                                {sanitiseValue(_.get(seq, k))}
                                            </Table.Cell>
                                        ))}
                                </Table.Row>
                            )
                        })
                    })
                )}
            </Table.Body>
        </Table>
    )
}

export default ProjectGrid
