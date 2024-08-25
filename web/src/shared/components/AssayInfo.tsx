import * as React from 'react'
import { Table as SUITable } from 'semantic-ui-react'

import capitalize from 'lodash/capitalize'
import { GraphQlAssay } from '../../__generated__/graphql'
import formatBytes from '../utilities/formatBytes'
import safeValue from '../utilities/safeValue'
import Table from './Table'

interface File {
    location: string
    basename: string
    class: string
    checksum: string | null
    size: number
    secondaryFiles?: File[]
}

interface SequenceMeta {
    reads?: File | Array<File> | Array<Array<File>>
    [key: string]: any
}

const excludedSequenceFields = ['id', '__typename']

const AssayInfo: React.FunctionComponent<{
    data: Partial<GraphQlAssay>
}> = ({ data }) => {
    const renderReadsMetadata = (readsMetadata: File[], key: number | string) => (
        <Table celled key={key}>
            <SUITable.Body>
                {readsMetadata.map((item: File) => (
                    <SUITable.Row key={item.location}>
                        <SUITable.Cell collapsing>{item.location}</SUITable.Cell>
                        <SUITable.Cell collapsing>{formatBytes(item.size)}</SUITable.Cell>
                    </SUITable.Row>
                ))}
            </SUITable.Body>
        </Table>
    )

    const prepReadMetadata = (metadata: SequenceMeta) => {
        if (!metadata.reads) return <></>
        if (!Array.isArray(metadata.reads))
            return (
                <SUITable.Row>
                    <SUITable.Cell>
                        <b>Reads</b>
                    </SUITable.Cell>
                    <SUITable.Cell>{renderReadsMetadata([metadata.reads], 1)}</SUITable.Cell>
                </SUITable.Row>
            )
        return (
            <SUITable.Row>
                <SUITable.Cell>
                    <b>Reads</b>
                </SUITable.Cell>
                <SUITable.Cell>
                    {metadata.reads.map((v, i) =>
                        renderReadsMetadata(Array.isArray(v) ? v : [v], i)
                    )}
                </SUITable.Cell>
            </SUITable.Row>
        )
    }

    const renderSeqInfo = (seqInfo: Partial<GraphQlAssay>) =>
        Object.entries(seqInfo)
            .filter(([key]) => !excludedSequenceFields.includes(key))
            .map(([key, value]) => {
                if (key === 'externalIds') {
                    if (Object.keys(seqInfo.externalIds ?? {}).length) {
                        return (
                            <SUITable.Row key={key}>
                                <SUITable.Cell>
                                    <b>External Ids</b>
                                </SUITable.Cell>
                                <SUITable.Cell>
                                    {Object.entries(seqInfo.externalIds ?? {})
                                        .map(([k1, v1]) => `${v1} (${k1})`)
                                        .join(', ')}
                                </SUITable.Cell>
                            </SUITable.Row>
                        )
                    }
                    return <React.Fragment key="ExternalID"></React.Fragment>
                }
                if (key === 'meta') {
                    return Object.entries((seqInfo.meta as SequenceMeta) ?? {})
                        .filter(([k1]) => k1 !== 'reads')
                        .map(([k1, v1]) => {
                            if (
                                Array.isArray(v1) &&
                                v1.filter((v) => !!v.location && !!v.size).length === v1.length
                            ) {
                                // all are files coincidentally
                                return (
                                    <SUITable.Row key={`${k1}`}>
                                        <SUITable.Cell>
                                            <b>{capitalize(k1)}</b>
                                        </SUITable.Cell>
                                        <SUITable.Cell>
                                            {renderReadsMetadata(v1 as File[], key)}
                                        </SUITable.Cell>
                                    </SUITable.Row>
                                )
                            }
                            if (v1 && typeof v1 === 'object' && !Array.isArray(v1)) {
                                if (!!v1.location && !!v1.size) {
                                    return (
                                        <SUITable.Row key={`${k1}`}>
                                            <SUITable.Cell>
                                                <b>{capitalize(k1)}:</b>
                                            </SUITable.Cell>
                                            <SUITable.Cell>
                                                {renderReadsMetadata([v1] as File[], k1)}
                                            </SUITable.Cell>
                                        </SUITable.Row>
                                    )
                                }
                            }
                            const stringifiedValue = safeValue(v1)
                            return (
                                <SUITable.Row key={`${k1}-${stringifiedValue}`}>
                                    <SUITable.Cell>
                                        <b>{capitalize(k1)}</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>
                                        {stringifiedValue ?? <em>no-value</em>}
                                    </SUITable.Cell>
                                </SUITable.Row>
                            )
                        })
                }

                const stringifiedValue = safeValue(value)
                return (
                    <SUITable.Row key={`${key}-${stringifiedValue}`}>
                        <SUITable.Cell>
                            <b>{capitalize(key)}</b>
                        </SUITable.Cell>
                        <SUITable.Cell>{stringifiedValue ?? <em>no-value</em>}</SUITable.Cell>
                    </SUITable.Row>
                )
            })

    return (
        <Table celled collapsing>
            <SUITable.Body>
                {renderSeqInfo(data)}
                {prepReadMetadata(data.meta || {})}
            </SUITable.Body>
        </Table>
    )
}

export default AssayInfo
