import * as React from 'react'
import { Table } from 'semantic-ui-react'

import _ from 'lodash'
import { GraphQlSample } from '../../__generated__/graphql'

const SampleInfo: React.FunctionComponent<{
    sample: Partial<GraphQlSample>
}> = ({ sample }) => (
    <Table celled collapsing>
        <Table.Body>
            {Object.entries(sample).map(([key, value]) => (
                <Table.Row key={`${key}-${value}`}>
                    <Table.Cell>
                        <b>{_.capitalize(key)}</b>
                    </Table.Cell>
                    <Table.Cell>{value?.toString() ?? <em>no value</em>}</Table.Cell>
                </Table.Row>
            ))}
        </Table.Body>
    </Table>
)
export default SampleInfo
