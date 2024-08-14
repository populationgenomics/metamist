import * as React from 'react'

import groupBy from 'lodash/groupBy'
import keyBy from 'lodash/keyBy'
import sortBy from 'lodash/sortBy'
import uniqBy from 'lodash/uniqBy'

import { KeyValueTable } from '../../shared/components/KeyValueTable'
import Table from '../../shared/components/Table'
import { AnalysisGrid, IAnalysisGridAnalysis } from '../analysis/AnalysisGrid'
import { AnalysisViewModal } from '../analysis/AnalysisView'

interface IParticipantViewParticipant {
    id: number
    externalId: string
    karyotype?: string | null
    meta?: any | null
    phenotypes?: { [key: string]: any }
    samples: {
        id: string
        type: string
        meta: { [key: string]: any }
        sequencingGroups: {
            id: string
            type: string
            technology: string
            platform: string
        }[]
    }[]
}

interface IParticipantViewProps {
    participant: IParticipantViewParticipant
    analyses: IAnalysisGridAnalysis[]
    individualToHiglight?: string | null
    setHighlightedIndividual?: (individualId?: string | null) => void
    showNonSingleSgAnalyses?: boolean
}

export const ParticipantView: React.FC<IParticipantViewProps> = ({
    participant,
    individualToHiglight,
    analyses,
    setHighlightedIndividual,
    showNonSingleSgAnalyses,
}) => {
    const [analysisIdToView, setAnalysisIdToView] = React.useState<number | undefined>()

    const sgsById = keyBy(
        // @ts-ignore
        participant.samples.flatMap((s) => s.sequencingGroups),
        (sg) => sg.id
    )
    const analysesBySgId = groupBy(
        analyses?.filter((a) => a?.sgs?.length === 1),
        (a) => a?.sgs?.[0]
    )
    const participantBySgId = Object.keys(sgsById).reduce(
        (acc, sgId) => ({
            ...acc,
            [sgId]: participant,
        }),
        {}
    )
    const extraAnalyses = showNonSingleSgAnalyses
        ? sortBy(
              uniqBy(
                  analyses?.filter((a) => a?.sgs?.length !== 1),
                  (a) => a.id
              ),
              (a) => a.timestampCompleted
          )
        : []
    const participantFields = {
        ID: participant.id,
        Karyotype: participant.karyotype,
        ...participant.meta,
    }
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
            <h3
                onClick={() =>
                    setHighlightedIndividual?.(
                        individualToHiglight == participant.externalId
                            ? null
                            : participant.externalId
                    )
                }
                style={{ cursor: 'hand' }}
            >
                {participant.externalId}
            </h3>

            <div style={{ marginLeft: '40px' }}>
                <KeyValueTable
                    obj={{
                        ...participantFields,
                        ...(participant.phenotypes || {}),
                    }}
                />

                <h4>Samples</h4>
                <Table>
                    <thead>
                        <tr>
                            <th>Sample ID</th>
                            <th>Type</th>
                            <th>Meta</th>
                        </tr>
                    </thead>
                    <tbody>
                        {participant.samples.map((s) => (
                            <tr>
                                <td>{s.id}</td>
                                <td>{s.type}</td>
                                <td>
                                    <KeyValueTable obj={s.meta} />
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </Table>

                <h4>Sequencing groups </h4>
                <Table>
                    <thead>
                        <tr>
                            <th>Sample ID</th>
                            <th>SG ID</th>
                            <th>Type</th>
                            <th>Technology</th>
                            <th>Platform</th>
                            <th>Analyses type</th>
                            <th>Timestamp</th>
                            <th></th>
                        </tr>
                    </thead>
                    <tbody>
                        {participant.samples.flatMap((s) =>
                            s.sequencingGroups.flatMap((sg) => {
                                const analyses = analysesBySgId[sg.id] || []
                                const nAnalysis = analyses.length + 1
                                return (
                                    <React.Fragment key={`sg-head-row-${sg.id}`}>
                                        <tr>
                                            <td rowSpan={nAnalysis}>{s.id}</td>
                                            <td rowSpan={nAnalysis}>{sg.id}</td>
                                            <td rowSpan={nAnalysis}>{sg.type}</td>
                                            <td rowSpan={nAnalysis}>{sg.technology}</td>
                                            <td rowSpan={nAnalysis}>{sg.platform}</td>
                                            <td colSpan={3}>
                                                <em style={{ color: 'var(--color-text-disabled)' }}>
                                                    Analyses
                                                </em>
                                            </td>
                                        </tr>
                                        {analyses.map((a) => (
                                            <tr key={`sg-analyses-row-${a.id}`}>
                                                <td>{a.type}</td>
                                                <td>{a.timestampCompleted}</td>
                                                <td>
                                                    <a
                                                        href="#"
                                                        onClick={(e) => {
                                                            e.preventDefault()
                                                            setAnalysisIdToView(a.id)
                                                        }}
                                                    >
                                                        {a.output}
                                                    </a>
                                                </td>
                                            </tr>
                                        ))}
                                    </React.Fragment>
                                )
                            })
                        )}
                    </tbody>
                </Table>
                {extraAnalyses.length > 0 && (
                    <AnalysisGrid
                        analyses={extraAnalyses}
                        setAnalysisIdToView={(aId) => setAnalysisIdToView(aId)}
                        participantBySgId={participantBySgId}
                    />
                )}
            </div>
            <AnalysisViewModal
                analysisId={analysisIdToView}
                onClose={() => setAnalysisIdToView(undefined)}
            />
        </div>
    )
}
