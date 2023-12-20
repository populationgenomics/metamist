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
    console.log('data', data)

    const aggArGUIDData = data.reduce((acc, curr) => {
        const { cost } = curr
        const ar_guid = curr['ar-guid']
        const idx = acc.findIndex((d) => d.ar_guid === ar_guid)
        if (cost >= 0) {
            // do not include credits, should be filter out at API?
            if (idx === -1) {
                acc.push({ type: 'ar_guid', key: ar_guid, ar_guid, cost })
            } else {
                acc[idx].cost += cost
            }
        }
        return acc
    }, [])

    // data contains ar_guid, batch_ids, costs
    // sum cost per batch_id
    const aggBatchData = data.reduce((acc, curr) => {
        const { batch_id, url, cost } = curr
        const ar_guid = curr['ar-guid']
        const idx = acc.findIndex((d) => d.batch_id === batch_id)
        if (cost >= 0) {
            // do not include credits, should be filter out at API?
            if (idx === -1) {
                acc.push({ type: 'batch_id', key: batch_id, ar_guid, batch_id, url, cost })
            } else {
                acc[idx].cost += cost
            }
        }
        return acc
    }, [])

    const aggBatchJobData = data.reduce((acc, curr) => {
        const { batch_id, url, cost, job_id } = curr
        const ar_guid = curr['ar-guid']
        const idx = acc.findIndex((d) => d.batch_id === batch_id && d.job_id === job_id)
        if (cost >= 0) {
            if (idx === -1) {
                acc.push({
                    type: 'batch_id/job_id',
                    key: batch_id + '/' + job_id,
                    batch_id,
                    job_id,
                    ar_guid,
                    url,
                    cost,
                })
            } else {
                acc[idx].cost += cost
            }
        }
        return acc
    }, [])

    const aggData = [...aggArGUIDData, ...aggBatchData, ...aggBatchJobData]

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
            title: 'HAIL BATCH ID',
            dataMap: (data: any, value: string) => (
                <a href={`${value}`} rel="noopener noreferrer" target="_blank">
                    {data.batch_id}
                </a>
            ),
        },
        {
            category: 'job_id',
            title: 'JOB ID',
        },
        {
            category: 'cost',
            title: 'COST',
            dataMap: (data: any, value: string) => (
                <Popup
                    content={data.cost}
                    trigger={<span>${data.cost.toFixed(6)}</span>}
                    position="top center"
                />
            ),
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
                {/* <SUITable.Row>
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
                </SUITable.Row> */}
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
