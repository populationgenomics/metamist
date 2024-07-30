import * as React from 'react'

import { useParams } from 'react-router-dom'
import { Card, Table as SUITable } from 'semantic-ui-react'

import { useQuery } from '@apollo/client'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'

import { gql } from '../../__generated__/gql'

import _ from 'lodash'
import TangledTree, { PersonNode } from '../../shared/components/pedigree/TangledTree'
import Table from '../../shared/components/Table'
import { AnalysisViewModal } from '../analysis/AnalysisView'

const sampleFieldsToDisplay = ['active', 'type']
const getSeqrUrl = (projectGuid: string, familyGuid: string) =>
    `https://seqr.populationgenomics.org.au/project/${projectGuid}/family_page/${familyGuid}`

const GET_FAMILY_INFO = gql(`
query FamilyInfo($family_id: Int!) {
    family(familyId: $family_id) {
      id
      externalId
      project {
        name
        meta
        pedigree(
          internalFamilyIds: [$family_id]
          replaceWithFamilyExternalIds: true
          replaceWithParticipantExternalIds: true
        )
      }
      familyParticipants {
        affected
        participant {
          id
          externalId
          phenotypes
          meta
          samples {
            id
            externalId
            meta
            type
            sequencingGroups {
              id
              platform
              technology
              type
              assays {
                id
                meta
                type
              }
              analyses(status: {eq: COMPLETED}) {
                id
                timestampCompleted
                type
                meta
                output
              }
            }
          }
        }
      }
    }
  }`)

interface IFamilyViewProps {
    familyId: number
}

export const FamilyPage: React.FunctionComponent<Record<string, unknown>> = () => {
    const { familyId } = useParams()
    if (!familyId) return <em>No family ID</em>

    return <FamilyView familyId={parseInt(familyId)} />
}

export const FamilyView: React.FC<IFamilyViewProps> = ({ familyId }) => {
    const [highlightedIndividual, setHighlightedIndividual] = React.useState<
        string | null | undefined
    >()

    const [analysisIdToView, setAnalysisIdToView] = React.useState<number | null>(null)

    if (!familyId || isNaN(familyId)) return <em>Invalid family ID</em>

    const { loading, error, data } = useQuery(GET_FAMILY_INFO, {
        variables: { family_id: familyId },
    })

    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>
    if (!data) return <>No data!</>

    const sgs = data?.family?.familyParticipants.flatMap((fp) =>
        fp.participant.samples.flatMap((s) => s.sequencingGroups)
    )
    const sgsById = _.keyBy(sgs, (s) => s.id)

    const participantBySgId: { [sgId: string]: any } = data?.family?.familyParticipants.reduce(
        (acc, fp) => {
            for (const s of fp.participant.samples) {
                for (const sg of s.sequencingGroups) {
                    acc[sg.id] = fp.participant
                }
            }
            return acc
        },
        {}
    )

    const aById: {
        [id: number]: {
            id: number
            timestampCompleted?: any | null
            type: string
            sgs: string[]
            meta?: any | null
            output?: string | null
        }
    } = {}

    for (const fp of data?.family?.familyParticipants) {
        for (const s of fp.participant.samples) {
            for (const sg of s.sequencingGroups) {
                for (const a of sg.analyses) {
                    if (a.id in aById) {
                        aById[a.id].sgs.push(sg.id)
                    } else {
                        aById[a.id] = {
                            ...a,
                            sgs: [sg.id],
                        }
                    }
                }
            }
        }
    }
    const individualAnalysisByParticipantId = _.groupBy(
        Object.values(aById).filter((a) => a.sgs.length == 1),
        (a) => participantBySgId[a.sgs[0]]?.externalId
    )
    const familyAnalysis = Object.values(aById).filter((a) => a.sgs.length > 1)
    const analyses = _.orderBy(Object.values(aById), (a) => a.timestampCompleted)

    return (
        <div style={{ width: '100%' }}>
            <h2>
                {data?.family?.externalId} ({data?.family?.project?.name})
            </h2>
            <TangledTree
                data={data?.family?.project.pedigree}
                highlightedIndividual={highlightedIndividual}
                onHighlight={(e) => {
                    setHighlightedIndividual(e?.individual_id)
                }}
                nodeDiameter={60}
            />
            <Card
                style={{
                    display: 'inline-block',
                    padding: '20px',
                    backgroundColor: 'var(--color-bg-card)',
                    border: '1px solid var(--color-border-color)',
                }}
            >
                test
            </Card>
            <PedigreeTable
                pedigree={data?.family?.project.pedigree}
                highlightedIndividual={highlightedIndividual}
                setHighlightedIndividual={setHighlightedIndividual}
            />
            {/* @ts-ignore: remove once families have external IDs*/}
            <SeqrUrls project={data?.family?.project} family={data?.family} />

            {data?.family?.familyParticipants.flatMap((fp) => (
                <IndividualDetails
                    participant={fp.participant}
                    individualToHiglight={highlightedIndividual}
                    analyses={individualAnalysisByParticipantId[fp.participant?.externalId]}
                />
            ))}
            <hr />
            <section id="family-analyses">
                <h4>Family analyses</h4>
                <AnalysisGrid
                    analyses={familyAnalysis}
                    participantBySgId={participantBySgId}
                    setAnalysisIdToView={(aId) => setAnalysisIdToView(aId)}
                />
            </section>

            <AnalysisViewModal
                size="small"
                analysisId={analysisIdToView}
                onClose={() => setAnalysisIdToView(null)}
            />
        </div>
    )
}

interface IAnalysisGridAnalysis {
    id: number
    timestampCompleted?: any | null
    type: string
    meta?: any | null
    output?: string | null
    sgs: string[]
}

const AnalysisGrid: React.FC<{
    analyses: IAnalysisGridAnalysis[]
    participantBySgId: { [sgId: string]: { externalId: string } }
    highlightedIndividual?: string | null
    setAnalysisIdToView: (analysisId: number) => void
    showSequencingGroup?: boolean
}> = ({
    analyses,
    participantBySgId,
    highlightedIndividual,
    setAnalysisIdToView,
    showSequencingGroup,
}) => {
    return (
        <Table>
            <thead>
                <SUITable.Row>
                    {showSequencingGroup && (
                        <SUITable.HeaderCell>Sequencing group</SUITable.HeaderCell>
                    )}
                    <SUITable.HeaderCell>Created</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Type</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Sequencing type</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Output</SUITable.HeaderCell>
                </SUITable.Row>
            </thead>
            <tbody>
                {analyses?.map((a) => (
                    <SUITable.Row
                        key={a.id}
                        style={{
                            backgroundColor: a.sgs.some(
                                (sg) =>
                                    !!highlightedIndividual &&
                                    participantBySgId[sg]?.externalId === highlightedIndividual
                            )
                                ? 'var(--color-page-total-row)'
                                : 'var(--color-bg-card)',
                        }}
                    >
                        {showSequencingGroup && (
                            <SUITable.Cell>
                                {a.sgs.map((sg) => (
                                    <li>
                                        {sg}{' '}
                                        {participantBySgId && sg in participantBySgId
                                            ? `(${participantBySgId[sg]?.externalId})`
                                            : ''}
                                    </li>
                                ))}
                            </SUITable.Cell>
                        )}
                        <SUITable.Cell>{a.timestampCompleted}</SUITable.Cell>
                        <SUITable.Cell>{a.type}</SUITable.Cell>
                        <SUITable.Cell>{a.meta?.sequencing_type}</SUITable.Cell>
                        <SUITable.Cell>
                            <a href="#" onClick={() => setAnalysisIdToView(a.id)}>
                                {a.output}
                            </a>
                        </SUITable.Cell>
                    </SUITable.Row>
                ))}
            </tbody>
        </Table>
    )
}

const PedigreeTable: React.FC<{
    pedigree: any
    highlightedIndividual?: string | null
    setHighlightedIndividual?: (individualId?: string | null) => void
}> = ({ pedigree, highlightedIndividual, setHighlightedIndividual }) => {
    return (
        <Table>
            <thead>
                <SUITable.Row>
                    <SUITable.HeaderCell></SUITable.HeaderCell>
                    <SUITable.HeaderCell>Participant</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Paternal ID</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Maternal ID</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Affected</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Notes</SUITable.HeaderCell>
                </SUITable.Row>
            </thead>
            <tbody>
                {pedigree?.map((pr: any) => {
                    const isHighlighted = highlightedIndividual == pr.individual_id
                    return (
                        <SUITable.Row
                            key={pr.individual_id}
                            style={{
                                backgroundColor: isHighlighted
                                    ? 'var(--color-page-total-row)'
                                    : 'var(--color-bg-card)',
                                fontWeight: isHighlighted ? 'bold' : 'normal',
                            }}
                        >
                            <td>
                                <svg width={30} height={30}>
                                    <PersonNode
                                        showIndividualId={false}
                                        isHighlighted={highlightedIndividual == pr.individual_id}
                                        nodeSize={30}
                                        node={{ x: 15, y: 15 }}
                                        entry={pr}
                                        onHighlight={(e) =>
                                            setHighlightedIndividual?.(e?.individual_id)
                                        }
                                    />
                                </svg>
                            </td>
                            <td>{pr.individual_id}</td>
                            <td>{pr.paternal_id}</td>
                            <td>{pr.maternal_id}</td>
                            <td>{pr.affected}</td>
                            <td>{pr.notes}</td>
                        </SUITable.Row>
                    )
                })}
            </tbody>
        </Table>
    )
}

const getFamilyEidKeyForSeqrSeqType = (seqType: string) => `seqr-${seqType}`

const SeqrUrls: React.FC<{
    project: { meta: any }
    family: { externalIds: { [key: string]: string } }
}> = ({ project, family }) => {
    // meta keys for seqr projectGuids follow the format: seqr-project-{sequencing_type}
    // family.externalIds follow the format: seqr-{sequencing_type}

    const seqrProjectGuidToSequencingType: { [sequencingType: string]: string } = Object.keys(
        project.meta
    )
        .filter((k) => k.startsWith('seqr-project-'))
        .reduce(
            (acc, k) => ({
                ...acc,
                [k.replace('seqr-project-', '')]: project.meta[k],
            }),
            {}
        )

    const sequencingTypeToSeqrUrl: { [sequencingType: string]: string } = Object.keys(
        seqrProjectGuidToSequencingType
    )
        .filter(
            (sequencingType) => getFamilyEidKeyForSeqrSeqType(sequencingType) in family.externalIds
        )
        .reduce(
            (sequencingType, acc) => ({
                ...acc,
                [sequencingType]: getSeqrUrl(
                    seqrProjectGuidToSequencingType[sequencingType],
                    family.externalIds[getFamilyEidKeyForSeqrSeqType(sequencingType)]
                ),
            }),
            {}
        )

    if (Object.keys(sequencingTypeToSeqrUrl).length === 0) {
        return <></>
    }
    return (
        <Table>
            <thead>
                <tr>
                    <td>
                        <strong>Sequencing Type</strong>
                    </td>
                    <td>
                        <strong>URL</strong>
                    </td>
                </tr>
            </thead>
            <tbody>
                {Object.entries(sequencingTypeToSeqrUrl).map(([seqType, url]) => (
                    <tr key={seqType}>
                        <td>
                            <b>{seqType}</b>
                        </td>
                        <td>
                            <a href={url} target="_blank" rel="noreferrer">
                                {url}
                            </a>
                        </td>
                    </tr>
                ))}
            </tbody>
        </Table>
    )
}

const IndividualDetails: React.FC<{
    participant: any
    analyses: IAnalysisGridAnalysis[]
    individualToHiglight?: string | null
}> = ({ participant, individualToHiglight, analyses }) => {
    return (
        <div
            style={{
                border:
                    participant.externalId == individualToHiglight
                        ? '5px solid var(--color-page-total-row)'
                        : '',
                paddingBottom: '20px',
            }}
        >
            <h3>{participant.externalId}</h3>
            <div style={{ marginLeft: '40px' }}>
                <h5>Analyses</h5>
                <AnalysisGrid
                    analyses={analyses}
                    participantBySgId={{}}
                    showSequencingGroup
                    setAnalysisIdToView={(analysisId) => console.log}
                />
            </div>
        </div>
    )
}

export default FamilyView
