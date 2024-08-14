import { useQuery } from '@apollo/client'
import React from 'react'
import { Button, Message, Modal, Table as SUITable } from 'semantic-ui-react'
import { gql } from '../../__generated__'
import { KeyValueTable } from '../../shared/components/KeyValueTable'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import Table from '../../shared/components/Table'
import { getHailBatchURL } from '../../shared/utilities/hailBatch'

interface IAnalysisViewProps {
    analysisId: number
}

const ANALYSIS_QUERY = gql(`
query Analyses($analysisId: Int!) {
  analyses(id: {eq: $analysisId}) {
    id
    meta
    output
    status
    timestampCompleted
    type
    auditLogs {
        id
        arGuid
        author
        timestamp
        meta
    }
  }
}
`)

export const AnalysisView: React.FC<IAnalysisViewProps> = ({ analysisId }) => {
    const { loading, error, data } = useQuery(ANALYSIS_QUERY, {
        variables: { analysisId: analysisId },
    })

    const analysis = data?.analyses[0]

    if (loading) {
        return <LoadingDucks />
    }

    if (!!error) {
        return <Message negative>{error}</Message>
    }

    if (!analysis) {
        return <LoadingDucks />
    }
    const sortedAuditLogs = analysis.auditLogs.sort((a, b) => {
        return new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
    })
    const attributeDict: Record<string, any> = {
        ID: analysis.id,
        Output: <pre>{analysis.output}</pre>,
        Type: analysis.type,
        Status: analysis.status,
    }
    if (analysis.timestampCompleted) {
        attributeDict.Completed = analysis.timestampCompleted
    }

    return (
        <>
            <h3>Attributes</h3>
            {/* <DictEditor input={analysis.meta} readOnly /> */}

            <KeyValueTable
                obj={{ ...attributeDict, ...analysis.meta }}
                tableClass={Table}
                rightPadding="0px"
            />

            <h3>History</h3>
            <AuditLogHistory auditLogs={sortedAuditLogs} />
        </>
    )
}

interface IAuditLogHistoryProps {
    auditLogs: {
        id: number
        arGuid?: string | null
        author: string
        timestamp: string
        meta: Record<string, any>
    }[]
}

const getBatchInformationFromLog = (meta: Record<string, any>) => {
    // { "path": "/api/v1/analysis/vcgs-clinical/", "ip": "169.254.169.126", "HAIL_ATTEMPT_ID": "Y5QQS3", "HAIL_BATCH_ID": "474353", "HAIL_JOB_ID": "33"
    const batchId = meta.HAIL_BATCH_ID
    const jobId = meta.HAIL_JOB_ID

    if (!batchId) {
        return null
    }

    return {
        link: getHailBatchURL(batchId, jobId),
        text: `batches/${batchId}/jobs/${jobId}`,
    }
}
export const AuditLogHistory: React.FC<IAuditLogHistoryProps> = ({ auditLogs }) => {
    return (
        <Table>
            <SUITable.Header>
                <SUITable.Row>
                    <SUITable.HeaderCell>Timestamp</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Author</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Hail Batch</SUITable.HeaderCell>
                </SUITable.Row>
            </SUITable.Header>
            <SUITable.Body>
                {auditLogs.map((log) => {
                    const batchInformation = getBatchInformationFromLog(log.meta)

                    return (
                        <SUITable.Row key={log.id}>
                            <SUITable.Cell>{log.timestamp}</SUITable.Cell>
                            <SUITable.Cell>{log.author}</SUITable.Cell>
                            <SUITable.Cell>
                                {batchInformation && (
                                    <a href={batchInformation.link} target="_blank">
                                        {batchInformation.text}
                                    </a>
                                )}
                            </SUITable.Cell>
                        </SUITable.Row>
                    )
                })}
            </SUITable.Body>
        </Table>
    )
}

interface AnalysisViewModalProps {
    analysisId?: number | null
    onClose: () => void
    size?: 'mini' | 'tiny' | 'small' | 'large' | 'fullscreen'
}

export const AnalysisViewModal: React.FC<AnalysisViewModalProps> = ({
    analysisId,
    onClose,
    size,
}) => {
    const isOpen = !!analysisId
    return (
        <Modal
            size={size}
            onClose={onClose}
            open={isOpen}
            style={{ height: 'unset', top: '50px', left: 'unset' }}
        >
            <Modal.Header>Analysis</Modal.Header>
            <Modal.Content>
                {!!analysisId && <AnalysisView analysisId={analysisId!} />}
            </Modal.Content>
            <Modal.Actions>
                <Button onClick={() => onClose()}>Close</Button>
            </Modal.Actions>
        </Modal>
    )
}
