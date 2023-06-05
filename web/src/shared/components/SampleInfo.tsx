import * as React from 'react'
import { Table as SUITable } from 'semantic-ui-react'
import _ from 'lodash'
import Table from './Table'

import { GraphQlSample } from '../../__generated__/graphql'

const SampleInfo: React.FunctionComponent<{
    sample: Partial<GraphQlSample>
}> = ({ sample }) => (
    <Table celled collapsing>
        <SUITable.Body>
            {Object.entries(sample).map(([key, value]) => (
                <SUITable.Row key={`${key}-${value}`}>
                    <SUITable.Cell>
                        <b>{_.capitalize(key)}</b>
                    </SUITable.Cell>
                    <SUITable.Cell>{value?.toString() ?? <em>no value</em>}</SUITable.Cell>
                </SUITable.Row>
            ))}
        </SUITable.Body>
    </Table>
)
export default SampleInfo
