import * as React from 'react'
import _ from 'lodash'
import { Table } from 'semantic-ui-react'

interface SummaryStatisticsProps {
    cramSeqrStats: Record<string, Record<string, string>>
    projectName: string
}

const SummaryStatistics: React.FunctionComponent<SummaryStatisticsProps> = ({
    cramSeqrStats,
    projectName,
}) => {
    const statsEntries = Object.entries(cramSeqrStats)
    if (!statsEntries.length) return <></>

    return (
        <Table celled>
            <Table.Header>
                <Table.Row>
                    <Table.HeaderCell>Type</Table.HeaderCell>
                    <Table.HeaderCell>Sequences</Table.HeaderCell>
                    <Table.HeaderCell>CRAMs</Table.HeaderCell>
                    <Table.HeaderCell>Seqr</Table.HeaderCell>
                </Table.Row>
            </Table.Header>
            <Table.Body>
                {statsEntries.map(([key, value]) => (
                    <React.Fragment key={`${key}-${projectName}`}>
                        <Table.Row>
                            <Table.Cell>{_.capitalize(key)}</Table.Cell>
                            {Object.entries(value).map(([k1, v1]) => (
                                <Table.Cell key={`${key}-${k1}-${projectName}`}>
                                    {`${v1}`}
                                </Table.Cell>
                            ))}
                        </Table.Row>
                    </React.Fragment>
                ))}
            </Table.Body>
        </Table>
    )
}

export default SummaryStatistics
