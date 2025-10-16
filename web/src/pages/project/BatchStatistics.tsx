import capitalize from 'lodash/capitalize'
import * as React from 'react'
import { Table as SUITable } from 'semantic-ui-react'
import Table from '../../shared/components/Table'

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
    const seqTypes = Object.keys(cramSeqrStats)
    const batchEntries = Object.entries(batchSequenceStats)

    if (!seqTypes.length || !batchEntries.length) {
        return <></>
    }

    return (
        <Table celled compact>
            <SUITable.Header>
                <SUITable.Row>
                    <SUITable.HeaderCell>Batch</SUITable.HeaderCell>
                    {seqTypes.map((item) => (
                        <SUITable.HeaderCell key={`header-${item}-${projectName}`}>
                            {capitalize(item)}
                        </SUITable.HeaderCell>
                    ))}
                    <SUITable.HeaderCell>Total</SUITable.HeaderCell>
                </SUITable.Row>
            </SUITable.Header>

            <SUITable.Body>
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
                        <SUITable.Row key={`body-${key}-${projectName}`}>
                            <SUITable.Cell>{capitalize(key)}</SUITable.Cell>
                            {seqTypes.map((seq) => (
                                <SUITable.Cell
                                    key={`${key}-${seq}`}
                                >{`${value[seq]}`}</SUITable.Cell>
                            ))}
                            <SUITable.Cell>
                                {Object.values(value).reduce((a, b) => +a + +b, 0)}
                            </SUITable.Cell>
                        </SUITable.Row>
                    ))}
            </SUITable.Body>
        </Table>
    )
}

export default BatchStatistics
