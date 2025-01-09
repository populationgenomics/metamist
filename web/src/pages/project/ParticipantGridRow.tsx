import * as React from 'react'

import get from 'lodash/get'
import { TableCell, TableRow } from 'semantic-ui-react'
import FamilyLink from '../../shared/components/links/FamilyLink'
import ParticipantLink from '../../shared/components/links/ParticipantLink'
import SampleLink from '../../shared/components/links/SampleLink'
import SequencingGroupLink from '../../shared/components/links/SequencingGroupLink'
import sanitiseValue from '../../shared/utilities/sanitiseValue'
import {
    Assay,
    NestedParticipant,
    NestedSample,
    NestedSequencingGroup,
    ProjectParticipantGridField,
} from '../../sm-api/api'
import { firstColBorder, otherColBorder } from './ProjectGridHeaderGroup'

const getBorderStyles = (idx: number) => {
    return {
        borderBottom: otherColBorder,
        borderLeft: idx === 0 ? firstColBorder : otherColBorder,
    }
}

interface IProjectGridParticipantRowProps {
    projectName: string

    participant: NestedParticipant
    familyFields: ProjectParticipantGridField[]
    participantFields: ProjectParticipantGridField[]
    sampleFields: ProjectParticipantGridField[]
    sequencingGroupFields: ProjectParticipantGridField[]
    assayFields: ProjectParticipantGridField[]

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

const prepareExternalIds = (ids: Record<string, string>) => {
    return Object.entries(ids)
        .map(([key, value]) =>
            key.length === 0 ? sanitiseValue(value) : `${key}: ${sanitiseValue(value)}`
        )
        .join(', ')
}

const FamilyCells: React.FC<{
    fields: ProjectParticipantGridField[]
    participant: NestedParticipant
    backgroundColor?: string
    projectName: string
    participantRowSpan?: number
}> = ({ fields, participant, backgroundColor, participantRowSpan }) => {
    return (
        <>
            {fields.map((field) => (
                <TableCell
                    key={`${participant.id}family.${field.key}`}
                    style={{
                        backgroundColor,
                        ...getBorderStyles(0),
                    }}
                    rowSpan={participantRowSpan}
                >
                    {field.key == 'external_ids'
                        ? participant.families.map((f) => (
                              <FamilyLink
                                  key={`family-${participant.id}-${f.id}`}
                                  id={`${f.id ?? ''}`}
                              >
                                  {prepareExternalIds(f.external_ids || {})}
                              </FamilyLink>
                          ))
                        : participant.families
                              .map((fam) => sanitiseValue(get(fam, field.key)))
                              .join(', ')}
                </TableCell>
            ))}
        </>
    )
}

const ParticipantCells: React.FC<{
    fields: ProjectParticipantGridField[]
    participant: NestedParticipant
    backgroundColor?: string
    projectName: string
    participantRowSpan?: number
}> = ({ fields, participant, backgroundColor, participantRowSpan }) => {
    const defaultRenderer = (field: ProjectParticipantGridField) =>
        sanitiseValue(get(participant, field.key))
    const valuePreparers: Record<
        string,
        (field: ProjectParticipantGridField) => string | React.ReactNode
    > = {
        external_ids: () => prepareExternalIds(participant.external_ids || {}),
        id: (field: ProjectParticipantGridField) => (
            <ParticipantLink id={participant.id}>{defaultRenderer(field)}</ParticipantLink>
        ),
    }

    return (
        <>
            {fields.map((field, i) => (
                <TableCell
                    style={{
                        backgroundColor,
                        ...getBorderStyles(i),
                    }}
                    key={`${participant.id}participant.${field.key}`}
                    rowSpan={participantRowSpan}
                >
                    {(valuePreparers[field.key] || defaultRenderer)(field)}
                </TableCell>
            ))}
        </>
    )
}

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
    // debugger
    const rows = participant.samples.flatMap((s, sidx) => {
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
                    <TableRow key={`${participant.id}-${s.id}-${sg.id}-${assay.id}`} {...props}>
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
                                <TableCell
                                    style={{
                                        backgroundColor,
                                        ...getBorderStyles(i),
                                    }}
                                    key={`${s.id}sample.${field.key}`}
                                    rowSpan={samplesRowSpan}
                                >
                                    {field.key === 'external_ids' || field.key === 'id' ? (
                                        <SampleLink id={s.id}>
                                            {field.key === 'external_ids'
                                                ? prepareExternalIds(s.external_ids || {})
                                                : s.id}
                                        </SampleLink>
                                    ) : (
                                        sanitiseValue(get(s, field.key))
                                    )}
                                </TableCell>
                            ))}
                        {assayidx === 0 &&
                            sequencingGroupFields.map((field, i) => (
                                <TableCell
                                    style={{
                                        ...getBorderStyles(i),
                                        backgroundColor,
                                    }}
                                    key={`${sg.id}-sequencing_group.${field.key}`}
                                    rowSpan={sg.assays?.length || undefined}
                                >
                                    {field.key === 'id' && sg.id ? (
                                        <SequencingGroupLink sampleId={s.id} id={sg.id?.toString()}>
                                            {sanitiseValue(sg.id)}
                                        </SequencingGroupLink>
                                    ) : (
                                        sanitiseValue(get(sg, field.key))
                                    )}
                                </TableCell>
                            ))}
                        {assayFields.map((field, i) => (
                            <TableCell
                                style={{
                                    backgroundColor,
                                    ...getBorderStyles(i),
                                }}
                                key={`${s.id}-assay.${field.key || field.label}`}
                            >
                                {sanitiseValue(get(assay, field.key))}
                            </TableCell>
                        ))}
                    </TableRow>
                )
            })
        })
    })
    // return rows
    return <>{rows}</>
}
