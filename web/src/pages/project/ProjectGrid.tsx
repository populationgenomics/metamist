import * as React from 'react'
import _ from 'lodash'
import { Table as SUITable } from 'semantic-ui-react'
import Table from '../../shared/components/Table'

import SampleLink from '../../shared/components/links/SampleLink'
import FamilyLink from '../../shared/components/links/FamilyLink'
import sanitiseValue from '../../shared/utilities/sanitiseValue'
import { ProjectSummaryResponse } from '../../sm-api/api'

interface ProjectGridProps {
    summary: ProjectSummaryResponse
    projectName: string
}

const ProjectGrid: React.FunctionComponent<ProjectGridProps> = ({ summary, projectName }) => {
    const headers = [
        'Family ID',
        ...summary.participant_keys.map((field) => field[1]),
        ...summary.sample_keys.map((field) => field[1]),
        ...summary.sequence_keys.map((field) => `sequence.${field[1]}`),
    ]

    return (
        <Table celled>
            <SUITable.Header>
                <SUITable.Row>
                    {headers.map((k, i) => (
                        <SUITable.HeaderCell key={`${k}-${i}`}>{k}</SUITable.HeaderCell>
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
