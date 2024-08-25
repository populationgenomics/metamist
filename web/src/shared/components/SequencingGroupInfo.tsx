import * as React from 'react'
import { Table as SUITable } from 'semantic-ui-react'

import capitalize from 'lodash/capitalize'
import { GraphQlSequencingGroup, GraphQlAssay } from '../../__generated__/graphql'
import Table from './Table'
import { DeepPartial } from '../utilities/deepPartial'
import AssayInfo from './AssayInfo'

const excludedSequencingGroupFields = ['id', '__typename']

const SequencingGroupInfo: React.FunctionComponent<{
    data: DeepPartial<GraphQlSequencingGroup>
}> = ({ data }) => (
    <Table celled collapsing>
        <SUITable.Body>
            {Object.entries(data)
                .filter(([key]) => !excludedSequencingGroupFields.includes(key))
                .map(([key, value]) => {
                    if (key === 'assays') {
                        return (
                            <SUITable.Row key={`${key}-${value}`}>
                                <SUITable.Cell>
                                    <b>{capitalize(key)}</b>
                                </SUITable.Cell>
                                <SUITable.Cell>
                                    {value.map((assay: GraphQlAssay) => (
                                        <React.Fragment key={assay.id}>
                                            <h4>ID: {assay.id}</h4>
                                            <AssayInfo data={assay} />
                                        </React.Fragment>
                                    ))}
                                </SUITable.Cell>
                            </SUITable.Row>
                        )
                    }
                    return (
                        <SUITable.Row key={`${key}-${value}`}>
                            <SUITable.Cell>
                                <b>{capitalize(key)}</b>
                            </SUITable.Cell>
                            <SUITable.Cell>{value?.toString() ?? <em>no value</em>}</SUITable.Cell>
                        </SUITable.Row>
                    )
                })}
        </SUITable.Body>
    </Table>
)

export default SequencingGroupInfo
