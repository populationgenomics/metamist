import * as React from 'react'

import _ from 'lodash'

import FamilyLink from '../../shared/components/links/FamilyLink'
import SampleLink from '../../shared/components/links/SampleLink'
import SequencingGroupLink from '../../shared/components/links/SequencingGroupLink'
import sanitiseValue from '../../shared/utilities/sanitiseValue'
import { Assay, NestedParticipant, NestedSample, NestedSequencingGroup } from '../../sm-api/api'
import { ProjectGridField } from './ProjectColumnOptions'

interface IProjectGridParticipantRowProps {
    projectName: string

    participant: NestedParticipant
    familyFields: ProjectGridField[]
    participantFields: ProjectGridField[]
    sampleFields: ProjectGridField[]
    sequencingGroupFields: ProjectGridField[]
    assayFields: ProjectGridField[]

    backgroundColor?: string
}

const maxOr1 = (v: number) => Math.max(1, v)

const nRowsForParticipant = (participant: NestedParticipant) => {
    const res = participant.samples.map((s) => nRowsForSample(s)).reduce((a, b) => a + b, 0)

    return maxOr1(res)
}

const nRowsForSample = (sample: NestedSample) => {
    const res = (sample.sequencing_groups || [])
        .map((sg) => maxOr1((sg.assays || []).length))
        .reduce((a, b) => a + b, 0)
    return maxOr1(res)
}

const border = '1px solid #dee2e6'

const FamilyCells: React.FC<{
    fields: ProjectGridField[]
    participant: NestedParticipant
    backgroundColor?: string
    projectName: string
    participantRowSpan?: number
}> = ({ fields, participant, backgroundColor, projectName, participantRowSpan }) => (
    <>
        {fields.map((field) => (
            <td
                key={`${participant.id}family.${field.name}`}
                style={{
                    display: field.isVisible ? 'table-cell' : 'none',
                    backgroundColor,
                    borderRight: border,
                    borderBottom: border,
                    borderTop: border,
                    borderLeft: '2px solid var(--color-border-color)',
                }}
                rowSpan={participantRowSpan}
            >
                {field.name == 'external_id' ? (
                    <FamilyLink
                        id={`${participant.families.map((f) => f.id)[0]}`}
                        projectName={projectName}
                    >
                        {participant.families.map((f) => f.external_id).join(', ')}
                    </FamilyLink>
                ) : (
                    participant.families
                        .map((fam) => sanitiseValue(_.get(fam, field.name)))
                        .join(', ')
                )}
            </td>
        ))}
    </>
)

const ParticipantCells: React.FC<{
    fields: ProjectGridField[]
    participant: NestedParticipant
    backgroundColor?: string
    projectName: string
    participantRowSpan?: number
}> = ({ fields, participant, backgroundColor, projectName, participantRowSpan }) => (
    <>
        {fields.map((field, i) => (
            <td
                style={{
                    display: field.isVisible ? 'table-cell' : 'none',
                    backgroundColor,
                    borderRight: border,
                    borderBottom: border,
                    borderTop: border,
                    borderLeft:
                        i === 0
                            ? '2px solid var(--color-border-color)'
                            : '1px solid var(--color-border-default)',
                }}
                key={`${participant.id}participant.${field.name}`}
                rowSpan={participantRowSpan}
            >
                {sanitiseValue(_.get(participant, field.name))}
            </td>
        ))}
    </>
)

export const ProjectGridParticipantRows: React.FC<IProjectGridParticipantRowProps> = ({
    participant,
    backgroundColor,
    projectName,
    participantFields,
    sampleFields,
    sequencingGroupFields,
    assayFields,
    familyFields,
    ...props
}) => {
    const lengthOfParticipant = nRowsForParticipant(participant)
    const rows = participant.samples.flatMap((s, sidx) => {
        // const border = '1px solid #dee2e6'

        const lengthOfSample = nRowsForSample(s)

        const participantRowSpan = lengthOfParticipant > 0 ? lengthOfParticipant : undefined
        const samplesRowSpan = lengthOfSample > 0 ? lengthOfSample : undefined

        let sgs: Partial<NestedSequencingGroup>[] = s.sequencing_groups || []
        if (sgs.length === 0) {
            sgs = [{}]
        }
        return sgs.flatMap((sg, sgidx) => {
            let assays: Partial<Assay>[] = sg.assays || []
            if (assays.length === 0) {
                assays = [{}]
            }

            return assays.map((assay, assayidx) => {
                const isFirstOfGroup = sidx === 0 && sgidx === 0 && assayidx === 0
                return (
                    <tr key={`${participant.external_id}-${s.id}-${sg.id}-${assay.id}`} {...props}>
                        {isFirstOfGroup && (
                            <>
                                <FamilyCells
                                    fields={familyFields}
                                    participant={participant}
                                    projectName={projectName}
                                    backgroundColor={backgroundColor}
                                    participantRowSpan={participantRowSpan}
                                />
                                <ParticipantCells
                                    fields={participantFields}
                                    participant={participant}
                                    projectName={projectName}
                                    backgroundColor={backgroundColor}
                                    participantRowSpan={participantRowSpan}
                                />
                            </>
                        )}
                        {sgidx === 0 &&
                            assayidx === 0 &&
                            sampleFields.map((field, i) => (
                                <td
                                    style={{
                                        display: field.isVisible ? 'table-cell' : 'none',
                                        backgroundColor,
                                        borderRight: border,
                                        borderBottom: border,
                                        borderTop: border,
                                        borderLeft:
                                            i === 0
                                                ? '2px solid var(--color-border-color)'
                                                : '1px solid var(--color-border-default)',
                                        // border,
                                    }}
                                    key={`${s.id}sample.${field.name}`}
                                    rowSpan={samplesRowSpan}
                                >
                                    {field.name === 'external_id' || field.name === 'id' ? (
                                        <SampleLink id={s.id} projectName={projectName}>
                                            {sanitiseValue(_.get(s, field.name))}
                                        </SampleLink>
                                    ) : (
                                        sanitiseValue(_.get(s, field.name))
                                    )}
                                </td>
                            ))}
                        {assayidx === 0 &&
                            sequencingGroupFields.map((field, i) => (
                                <td
                                    style={{
                                        display: field.isVisible ? 'table-cell' : 'none',
                                        borderRight: border,
                                        borderBottom: border,
                                        borderTop: border,
                                        borderLeft:
                                            i === 0
                                                ? '2px solid var(--color-border-color)'
                                                : '1px solid var(--color-border-default)',
                                        backgroundColor,
                                    }}
                                    key={`${s.id}sequencing_group.${field.name}`}
                                    rowSpan={
                                        (sg.assays ?? []).length > 0
                                            ? (sg.assays ?? []).length
                                            : undefined
                                    }
                                >
                                    {field.name === 'id' ? (
                                        <SequencingGroupLink
                                            projectName={projectName}
                                            id={s.id}
                                            sg_id={_.get(sg, 'id')?.toString()}
                                        >
                                            {sanitiseValue(_.get(sg, field.name))}
                                        </SequencingGroupLink>
                                    ) : (
                                        sanitiseValue(_.get(sg, field.name))
                                    )}
                                </td>
                            ))}
                        {assayFields.map((field, i) => (
                            <td
                                style={{
                                    display: field.isVisible ? 'table-cell' : 'none',
                                    backgroundColor,
                                    borderRight: border,
                                    borderBottom: border,
                                    borderTop: border,
                                    borderLeft:
                                        i === 0
                                            ? '2px solid var(--color-border-color)'
                                            : '1px solid var(--color-border-default)',
                                    // border,
                                }}
                                key={`${s.id}assay.${field.name}`}
                            >
                                {sanitiseValue(_.get(assay, field.name))}
                            </td>
                        ))}
                    </tr>
                )
            })
        })
    })
    // return rows
    return <>{rows}</>
}
