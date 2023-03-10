import * as React from 'react'
import _ from 'lodash'
import { Table } from 'semantic-ui-react'

interface BatchStatisticsProps {
    cramSeqrStats: Record<string, Record<string, string>>
    batchSequenceStats: Record<string, Record<string, string>>
    projectName: string
}

const BatchStatistics: React.FunctionComponent<BatchStatisticsProps> = ({
    cramSeqrStats,
    batchSequenceStats,
    projectName,
}) => {
    if (!cramSeqrStats.length || !batchSequenceStats.length) {
        return <></>
    }
    const seqTypes = Object.keys(cramSeqrStats)
    const batchEntries = Object.entries(batchSequenceStats)

    return (
        <Table celled compact>
            <Table.Header>
                <Table.Row>
                    <Table.HeaderCell>Batch</Table.HeaderCell>
                    {seqTypes.map((item) => (
                        <Table.HeaderCell key={`header-${item}-${projectName}`}>
                            {_.capitalize(item)}
                        </Table.HeaderCell>
                    ))}
                    <Table.HeaderCell>Total</Table.HeaderCell>
                </Table.Row>
            </Table.Header>

            <Table.Body>
                {batchEntries
                    .sort((a, b) => {
                        if (a[0] === b[0]) {
                            return 0
                        }
                        if (a[0] === 'no-batch') {
                            return 1
                        }
                        if (b[0] === 'no-batch') {
                            return -1
                        }
                        return a[0].localeCompare(b[0], undefined, {
                            numeric: true,
                        })
                    })
                    .map(([key, value]) => (
                        <Table.Row key={`body-${key}-${projectName}`}>
                            <Table.Cell>{_.capitalize(key)}</Table.Cell>
                            {seqTypes.map((seq) => (
                                <Table.Cell key={`${key}-${seq}`}>{`${value[seq]}`}</Table.Cell>
                            ))}
                            <Table.Cell>
                                {Object.values(value).reduce((a, b) => +a + +b, 0)}
                            </Table.Cell>
                        </Table.Row>
                    ))}
            </Table.Body>
        </Table>
    )
}

export default BatchStatistics
