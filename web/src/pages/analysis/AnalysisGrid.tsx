import * as React from 'react'

import { Table as SUITable } from 'semantic-ui-react'
import { GraphQlAnalysis } from '../../__generated__/graphql'

import Table from '../../shared/components/Table'

export interface IAnalysisGridAnalysis extends GraphQlAnalysis {
    sgs?: string[]
}

export const AnalysisGrid: React.FC<{
    analyses: IAnalysisGridAnalysis[]
    participantBySgId: { [sgId: string]: { externalId: string } }
    sgsById?: { [sgId: string]: { technology: string; platform: string } }
    highlightedIndividual?: string | null
    setAnalysisIdToView: (analysisId: number) => void
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
                            {showSequencingGroup && (
                                <SUITable.Cell>
                                    {sg
                                        ? sgId
                                        : a.sgs?.map((sg) => (
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
                                {!!sg && `${sg?.technology} (${sg?.platform})`}
                            </SUITable.Cell>
                            <SUITable.Cell>
                                <a
                                    href="#"
                                    onClick={(e) => {
                                        e.preventDefault()
                                        setAnalysisIdToView(a.id)
                                    }}
                                >
                                    {a.output}
                                </a>
                            </SUITable.Cell>
                        </SUITable.Row>
                    )
                })}
            </tbody>
        </Table>
    )
}