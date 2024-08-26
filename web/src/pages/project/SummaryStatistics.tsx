import * as React from 'react'
import capitalize from 'lodash/capitalize'
import { Table as SUITable } from 'semantic-ui-react'
import Table from '../../shared/components/Table'

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
            <SUITable.Header>
                <SUITable.Row>
                    <SUITable.HeaderCell>Type</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Sequences</SUITable.HeaderCell>
                    <SUITable.HeaderCell>CRAMs</SUITable.HeaderCell>
                    <SUITable.HeaderCell>Seqr</SUITable.HeaderCell>
                </SUITable.Row>
            </SUITable.Header>
            <SUITable.Body>
                {statsEntries.map(([key, value]) => (
                    <React.Fragment key={`${key}-${projectName}`}>
                        <SUITable.Row>
                            <SUITable.Cell>{capitalize(key)}</SUITable.Cell>
                            {Object.entries(value).map(([k1, v1]) => (
                                <SUITable.Cell key={`${key}-${k1}-${projectName}`}>
                                    {`${v1}`}
                                </SUITable.Cell>
                            ))}
                        </SUITable.Row>
                    </React.Fragment>
                ))}
            </SUITable.Body>
        </Table>
    )
}

export default SummaryStatistics
