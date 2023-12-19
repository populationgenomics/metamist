import * as React from 'react'
import { Table as SUITable, Popup, Checkbox } from 'semantic-ui-react'
import _ from 'lodash'
import Table from '../../../shared/components/Table'
import sanitiseValue from '../../../shared/utilities/sanitiseValue'

import { Filter } from '../../project/AnalysisRunnerView/Filter'
import '../../project/AnalysisRunnerView/AnalysisGrid.css'

const EXCLUDED_FIELDS = [
    'id',
    'commit',
    'source',
    'position',
    'batch_url',
    'repo',
    'email',
    'timestamp',
]

interface Field {
    category: string
    title: string
    width?: string
    className?: string
    dataMap?: (data: any, value: string) => any
}

const HailBatchGrid: React.FunctionComponent<{
    data: any[]
    filters: Filter[]
    updateFilter: (value: string, category: string) => void
    handleSort: (clickedColumn: string) => void
    sort: { column: string | null; direction: string | null }
    idColumn?: string
}> = ({ data, filters, updateFilter, handleSort, sort, idColumn }) => {
    console.log(data)

    // data contains ar_guid, batch_ids, costs
    // sum cost per batch_id
    const aggData = data.reduce((acc, curr) => {
        const { batch_id, batch_name, namespace, topic, day, url, cost } = curr
        const ar_guid = curr['ar-guid']
        const idx = acc.findIndex((d) => d.batch_id === batch_id && topic === d.topic)
        if (idx === -1) {
            acc.push({ batch_id, ar_guid, batch_name, day, url, namespace, topic, cost })
        } else {
            // add cost to existing batch_id
            // treat credits as cost
            acc[idx].cost += cost // Math.abs(cost)
        }
        return acc
    }, [])

    console.log('aggData', aggData)

    const [openRows, setOpenRows] = React.useState<number[]>([])

    const handleToggle = (position: number) => {
        if (!openRows.includes(position)) {
            setOpenRows([...openRows, position])
        } else {
            setOpenRows(openRows.filter((i) => i !== position))
        }
    }

    const checkDirection = (category: string) => {
        if (sort.column === category && sort.direction !== null) {
            return sort.direction === 'ascending' ? 'ascending' : 'descending'
        }
        return undefined
    }

    const MAIN_FIELDS: Field[] = [
        {
            category: 'ar_guid',
            title: 'AR GUID',
        },
        {
            category: 'url',
            title: 'Hail Batch',
            dataMap: (data: any, value: string) => (
                console.log(value, 'dt:', data),
                (
                    <a href={`${value}`} rel="noopener noreferrer" target="_blank">
                        {data.batch_id}
                    </a>
                )
            ),
        },
        {
            category: 'batch_name',
            title: 'Script',
            // className: 'scriptField',
            // dataMap: (data: any, value: string) => (
            //     <code
            //         onClick={() => handleToggle(data.position)}
            //         style={{
            //             cursor: 'pointer',
            //         }}
            //     >
            //         {value}
            //     </code>
            // ),
        },
        {
            category: 'day',
            title: 'Day',
        },
        {
            category: 'namespace',
            title: 'Namespace',
        },
        {
            category: 'topic',
            title: 'Topic',
        },
        {
            category: 'cost',
            title: 'Cost',
        },
    ]

    const expandedRow = (log: any) =>
        MAIN_FIELDS.map(({ category, title, width, dataMap, className }) => (
            <SUITable.Cell
                key={category}
                className={className}
                // style={{ width: width ?? '100px' }}
            >
                {dataMap ? dataMap(log, log[category]) : sanitiseValue(log[category])}
            </SUITable.Cell>
        ))

    return (
        <Table celled compact sortable>
            <SUITable.Header>
                <SUITable.Row>
                    <SUITable.HeaderCell style={{ borderBottom: 'none' }} />
                    {MAIN_FIELDS.map(({ category, title }, i) => (
                        <SUITable.HeaderCell
                            key={`${category}-${i}`}
                            sorted={checkDirection(category)}
                            onClick={() => handleSort(category)}
                            style={{
                                borderBottom: 'none',
                                position: 'sticky',
                                resize: 'horizontal',
                            }}
                        >
                            {title}
                        </SUITable.HeaderCell>
                    ))}
                </SUITable.Row>
                <SUITable.Row>
                    <SUITable.Cell
                        style={{
                            borderBottom: 'none',
                            borderTop: 'none',
                            backgroundColor: 'var(--color-table-header)',
                        }}
                    />
                    {MAIN_FIELDS.map(({ category }) => (
                        <SUITable.Cell
                            key={`${category}-filter`}
                            style={{
                                borderBottom: 'none',
                                borderTop: 'none',
                                backgroundColor: 'var(--color-table-header)',
                            }}
                        >
                            <input
                                type="text"
                                key={category}
                                id={category}
                                onChange={(e) => updateFilter(e.target.value, category)}
                                placeholder="Filter..."
                                value={
                                    filters.find(
                                        ({ category: FilterCategory }) =>
                                            FilterCategory === category
                                    )?.value ?? ''
                                }
                                style={{ border: 'none', width: '100%', borderRadius: '25px' }}
                            />
                        </SUITable.Cell>
                    ))}
                </SUITable.Row>
                <SUITable.Row>
                    <SUITable.Cell
                        style={{
                            borderTop: 'none',
                            backgroundColor: 'var(--color-table-header)',
                        }}
                    />
                    {MAIN_FIELDS.map(({ category }) => (
                        <SUITable.Cell
                            className="sizeRow"
                            key={`${category}-resize`}
                            style={{
                                borderTop: 'none',
                                backgroundColor: 'var(--color-table-header)',
                            }}
                        ></SUITable.Cell>
                    ))}
                </SUITable.Row>
            </SUITable.Header>
            <SUITable.Body>
                {aggData.map((log, idx) => (
                    <React.Fragment key={idColumn ? log[idColumn] : idx}>
                        <SUITable.Row>
                            <SUITable.Cell collapsing>
                                <Checkbox
                                    checked={openRows.includes(log.position)}
                                    slider
                                    onChange={() => handleToggle(log.position)}
                                />
                            </SUITable.Cell>
                            {expandedRow(log)}
                        </SUITable.Row>
                        {Object.entries(log)
                            // .filter(
                            //     ([c]) =>
                            //         (!MAIN_FIELDS.map(({ category }) => category).includes(c) ||
                            //             c === 'script') &&
                            //         !EXCLUDED_FIELDS.includes(c)
                            // )
                            .map(([category, value], i) => (
                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(log.position)
                                            ? 'table-row'
                                            : 'none',
                                    }}
                                    key={i}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell>
                                        <b>{_.capitalize(category)}</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell colSpan={MAIN_FIELDS.length - 1}>
                                        <code>{value}</code>
                                    </SUITable.Cell>
                                </SUITable.Row>
                            ))}
                    </React.Fragment>
                ))}
            </SUITable.Body>
        </Table>
    )
}

export default HailBatchGrid
