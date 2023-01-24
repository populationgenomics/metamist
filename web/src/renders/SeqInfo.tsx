import * as React from 'react'
import { Table } from 'semantic-ui-react'

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

interface SampleSequencing {
    id: number
    external_ids?: { [name: string]: string }
    sample_id: string
    type: string
    meta?: SequenceMeta
    status: string
}

export const SeqInfo_: React.FunctionComponent<{
    data: SampleSequencing
}> = ({ data }) => {
    const formatBytes = (bytes: number, decimals = 2) => {
        if (!+bytes) return '0 Bytes'

        const k = 1024
        const dm = decimals < 0 ? 0 : decimals
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']

        const i = Math.floor(Math.log(bytes) / Math.log(k))

        return `${parseFloat((bytes / k ** i).toFixed(dm))} ${sizes[i]}`
    }

    const renderReadsMetadata = (readsMetadata: File[], key: number | string) => (
        <Table celled key={key}>
            <Table.Body>
                {readsMetadata.map((item: File) => (
                    <Table.Row key={item.location}>
                        <Table.Cell collapsing>{item.location}</Table.Cell>
                        <Table.Cell collapsing>{formatBytes(item.size)}</Table.Cell>
                    </Table.Row>
                ))}
            </Table.Body>
        </Table>
    )

    const prepReadMetadata = (metadata: SequenceMeta) => {
        if (!metadata.reads) return <></>
        if (!Array.isArray(metadata.reads))
            return (
                <Table.Row>
                    <Table.Cell>
                        <b>reads</b>
                    </Table.Cell>
                    <Table.Cell>{renderReadsMetadata([metadata.reads], 1)}</Table.Cell>
                </Table.Row>
            )
        return (
            <Table.Row>
                <Table.Cell>
                    <b>reads</b>
                </Table.Cell>
                <Table.Cell>
                    {metadata.reads.map((v, i) =>
                        renderReadsMetadata(Array.isArray(v) ? v : [v], i)
                    )}
                </Table.Cell>
            </Table.Row>
        )
    }

    const safeValue = (value: any): string => {
        if (!value) return value
        if (Array.isArray(value)) {
            return value.map(safeValue).join(', ')
        }
        if (typeof value === 'number') {
            return value.toString()
        }
        if (typeof value === 'string') {
            return value
        }
        if (value && typeof value === 'object' && !Array.isArray(value)) {
            if (!!value.location && !!value.size) {
                return `${value.location} (${formatBytes(value.size)})`
            }
        }
        return JSON.stringify(value)
    }

    const renderSeqInfo = (seqInfo: SampleSequencing) =>
        Object.entries(seqInfo)
            .filter(([key]) => key !== 'id')
            .map(([key, value]) => {
                if (key === 'external_ids') {
                    if (Object.keys(seqInfo.external_ids ?? {}).length) {
                        return (
                            <Table.Row key={key}>
                                <Table.Cell>
                                    <b>External Ids</b>
                                </Table.Cell>
                                <Table.Cell>
                                    {Object.entries(seqInfo.external_ids ?? {})
                                        .map(([k1, v1]) => (
                                            <React.Fragment
                                                key={`${v1} (${k1})`}
                                            >{`${v1} (${k1})`}</React.Fragment>
                                        ))
                                        .join()}
                                </Table.Cell>
                            </Table.Row>
                        )
                    }
                    return <React.Fragment key="ExternalID"></React.Fragment>
                }
                if (key === 'meta') {
                    return Object.entries(seqInfo.meta ?? {})
                        .filter(([k1]) => k1 !== 'reads')
                        .map(([k1, v1]) => {
                            if (
                                Array.isArray(v1) &&
                                v1.filter((v) => !!v.location && !!v.size).length === v1.length
                            ) {
                                // all are files coincidentally
                                return (
                                    <Table.Row key={`${k1}`}>
                                        <Table.Cell>
                                            <b>{k1}</b>
                                        </Table.Cell>
                                        <Table.Cell>
                                            {renderReadsMetadata(v1 as File[], key)}
                                        </Table.Cell>
                                    </Table.Row>
                                )
                            }
                            if (v1 && typeof v1 === 'object' && !Array.isArray(v1)) {
                                if (!!v1.location && !!v1.size) {
                                    return (
                                        <Table.Row key={`${k1}`}>
                                            <Table.Cell>
                                                <b>{k1}:</b>
                                            </Table.Cell>
                                            <Table.Cell>
                                                {renderReadsMetadata([v1] as File[], k1)}
                                            </Table.Cell>
                                        </Table.Row>
                                    )
                                }
                            }
                            const stringifiedValue = safeValue(v1)
                            return (
                                <Table.Row key={`${k1}-${stringifiedValue}`}>
                                    <Table.Cell>
                                        <b>{k1}</b>
                                    </Table.Cell>
                                    <Table.Cell>{stringifiedValue ?? <em>no-value</em>}</Table.Cell>
                                </Table.Row>
                            )
                        })
                }

                const stringifiedValue = safeValue(value)
                return (
                    <Table.Row key={`${key}-${stringifiedValue}`}>
                        <Table.Cell>
                            <b>{key}</b>
                        </Table.Cell>
                        <Table.Cell>{stringifiedValue ?? <em>no-value</em>}</Table.Cell>
                    </Table.Row>
                )
            })

    return (
        <Table celled collapsing>
            <Table.Body>
                {renderSeqInfo(data)}
                {prepReadMetadata(data.meta || {})}
            </Table.Body>
        </Table>
    )
}

export class SeqInfo extends React.Component<{ data: SampleSequencing }, { error?: Error }> {
    constructor(props: Record<'data', SampleSequencing>) {
        super(props)
        this.state = {}
    }

    static getDerivedStateFromError(error: Error): { error: Error } {
        // Update state so the next render will show the fallback UI.
        return { error }
    }

    render(): React.ReactNode {
        if (this.state.error) {
            return <p>{this.state.error.toString()}</p>
        }
        return <SeqInfo_ data={this.props.data} /> /* eslint react/jsx-pascal-case: 0 */
    }
}
