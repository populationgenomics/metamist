import * as React from 'react'

import { useParams } from 'react-router-dom'
import { Card, Table as SUITable } from 'semantic-ui-react'

import { useQuery } from '@apollo/client'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'

import { gql } from '../../__generated__/gql'

import groupBy from 'lodash/groupBy'
import keyBy from 'lodash/keyBy'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import { SplitPage } from '../../shared/components/Layout/SplitPage'
import TangledTree, {
    PedigreeEntry,
    PersonNode,
} from '../../shared/components/pedigree/TangledTree'
import Table from '../../shared/components/Table'
import { GraphQlParticipant } from '../../__generated__/graphql'
import { AnalysisGrid } from '../analysis/AnalysisGrid'
import { FamilyCommentsView } from '../comments/entities/FamilyCommentsView'
import { ParticipantView } from '../participant/ParticipantView'

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
          karyotype
          reportedGender
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

    const invalidFamilyId = !familyId || isNaN(familyId)

    const { loading, error, data } = useQuery(GET_FAMILY_INFO, {
        variables: { family_id: familyId },
        skip: invalidFamilyId,
    })

    if (invalidFamilyId) return <em>Invalid family ID</em>

    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>
    if (!data) return <>No data!</>

    const participantBySgId = data?.family?.familyParticipants.reduce(
        (acc, fp) => {
            for (const s of fp.participant.samples) {
                for (const sg of s.sequencingGroups) {
                    acc[sg.id] = fp.participant as GraphQlParticipant
                }
            }
            return acc
        },
        {} as { [sgId: string]: GraphQlParticipant }
    )

    const aById: {
        [id: number]: {
            id: number
            timestampCompleted?: number | null
            type: string
            sgs: string[]
            meta?: Record<string, unknown> | null
            output?: string | null
        }
    } = {}

    for (const fp of data?.family?.familyParticipants ?? []) {
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
    const individualAnalysisByParticipantId = groupBy(
        Object.values(aById).filter((a) => a.sgs.length == 1),
        (a) => participantBySgId[a.sgs[0]]?.externalId
    )
    const familyAnalysis = Object.values(aById).filter((a) => a.sgs.length > 1)
    const pedEntryByParticipantId = keyBy(data?.family?.project.pedigree, (pr) => pr.individual_id)

    const content = (
        <div style={{ width: '100%' }}>
            <h2>
                {data?.family?.externalId} ({data?.family?.project?.name})
            </h2>
            <div
                style={{
                    display: 'flex',
                    flexWrap: 'wrap', // Allows wrapping of elements
                    alignItems: 'center',
                    maxWidth: '100%', // Prevents overflow and ensures wrapping
                }}
            >
                <span style={{ padding: '20px' }}>
                    <TangledTree
                        data={data?.family?.project.pedigree}
                        highlightedIndividual={highlightedIndividual}
                        onHighlight={(e) => {
                            setHighlightedIndividual(e?.individual_id)
                        }}
                        nodeDiameter={60}
                    />
                </span>
                <Card
                    style={{
                        display: 'inline-block',
                        backgroundColor: 'var(--color-bg-card)',
                        border: '1px solid var(--color-border-color)',
                        minWidth: '480px',
                    }}
                >
                    <PedigreeTable
                        pedigree={data?.family?.project.pedigree}
                        highlightedIndividual={highlightedIndividual}
                        setHighlightedIndividual={setHighlightedIndividual}
                    />
                </Card>
            </div>
            <br />
            {/* @ts-ignore: remove once families have external IDs*/}
            <SeqrUrls project={data?.family?.project} family={data?.family} />

            {data?.family?.familyParticipants.flatMap((fp) => (
                <ParticipantView
                    participant={{
                        ...fp.participant,
                        pedEntry: pedEntryByParticipantId[fp.participant.externalId],
                    }}
                    individualToHiglight={highlightedIndividual}
                    analyses={individualAnalysisByParticipantId[fp.participant?.externalId]}
                    setHighlightedIndividual={setHighlightedIndividual}
                />
            ))}
            <hr />
            <section id="family-analyses">
                <h4>Family analyses</h4>
                <AnalysisGrid analyses={familyAnalysis} participantBySgId={participantBySgId} />
            </section>
        </div>
    )

    return (
        <SplitPage
            collapsed={true}
            collapsedWidth={64}
            main={() => <PaddedPage>{content}</PaddedPage>}
            side={({ collapsed, onToggleCollapsed }) => {
                const projectName = data.family.project.name
                const familyId = data.family.id
                if (!projectName || !familyId) return null

                return (
                    <FamilyCommentsView
                        projectName={projectName}
                        familyId={familyId}
                        collapsed={collapsed}
                        onToggleCollapsed={onToggleCollapsed}
                    />
                )
            }}
        />
    )
}

const PedigreeTable: React.FC<{
    pedigree: (PedigreeEntry & { notes?: string })[]
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
                {pedigree?.map((pr) => {
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
    project: { meta: Record<string, unknown> }
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
            (sequencingType) =>
                family.externalIds &&
                getFamilyEidKeyForSeqrSeqType(sequencingType) in family.externalIds
        )
        .reduce(
            (acc, sequencingType) => ({
                ...acc,
                [sequencingType]: getSeqrUrl(
                    seqrProjectGuidToSequencingType[sequencingType],
                    family.externalIds[getFamilyEidKeyForSeqrSeqType(sequencingType)]
                ),
            }),
            {} as { [sequencingType: string]: string }
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

export default FamilyView
