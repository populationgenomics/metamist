import * as React from 'react'

import { Table as SUITable } from 'semantic-ui-react'
import { GraphQlAnalysis } from '../../__generated__/graphql'

import AnalysisLink from '../../shared/components/links/AnalysisLink'
import Table from '../../shared/components/Table'

export interface IAnalysisGridAnalysis extends Partial<GraphQlAnalysis> {
    sgs?: string[]
}

export const AnalysisGrid: React.FC<{
    analyses: IAnalysisGridAnalysis[]
    participantBySgId: { [sgId: string]: { externalId: string } }
    sgsById?: { [sgId: string]: { technology: string; platform: string } }
    highlightedIndividual?: string | null
    setAnalysisIdToView: (analysisId?: number | null) => void
    showSequencingGroup?: boolean
}> = ({
    analyses,
    participantBySgId,
    sgsById,
    highlightedIndividual,
    setAnalysisIdToView,
    showSequencingGroup,
}) => {
    return (
        <Table>
            <thead>
                <SUITable.Row>
                    <SUITable.HeaderCell>ID</SUITable.HeaderCell>
                    {showSequencingGroup && (
                        <SUITable.HeaderCell>Sequencing group</SUITable.HeaderCell>
                    )}
                    <SUITable.HeaderCell>Created</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Type</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Sequencing type</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Sequencing technology</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Output</SUITable.HeaderCell>
                </SUITable.Row>
            </thead>
            <tbody>
                {analyses?.map((a) => {
                    const sgId = a.sgs?.length === 1 ? a.sgs[0] : null
                    const sg = sgId ? sgsById?.[sgId] : null
                    return (
                        <SUITable.Row
                            key={a.id}
                            style={{
                                backgroundColor: a.sgs?.some(
                                    (sg) =>
                                        !!highlightedIndividual &&
                                        participantBySgId[sg]?.externalId === highlightedIndividual
                                )
                                    ? 'var(--color-page-total-row)'
                                    : 'var(--color-bg-card)',
                            }}
                        >
                            <SUITable.Cell>
                                <AnalysisLink
                                    id={a.id}
                                    onClick={(e) => {
                                        e.preventDefault()
                                        e.stopPropagation()
                                        setAnalysisIdToView(a.id)
                                    }}
                                />
                            </SUITable.Cell>
                            {showSequencingGroup && (
                                <SUITable.Cell>
                                    {sg
                                        ? sgId
                                        : a.sgs?.map((sg) => (
                                              <li key={sg}>
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
                                {!!sg && `${sg?.technology} (${sg?.platform})`}
                            </SUITable.Cell>
                            <td style={{ wordBreak: 'break-all' }}>{a.output}</td>
                        </SUITable.Row>
                    )
                })}
            </tbody>
        </Table>
    )
}
