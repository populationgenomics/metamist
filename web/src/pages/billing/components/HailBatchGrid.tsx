import * as React from 'react'
import { Table as SUITable, Popup, Checkbox } from 'semantic-ui-react'
import _ from 'lodash'
import Table from '../../../shared/components/Table'
import sanitiseValue from '../../../shared/utilities/sanitiseValue'
import '../../project/AnalysisRunnerView/AnalysisGrid.css'

interface Field {
    category: string
    title: string
    width?: string
    className?: string
    dataMap?: (data: any, value: string) => any
}

const HailBatchGrid: React.FunctionComponent<{
    data: any[]
}> = ({ data }) => {
    // prepare aggregated data by ar_guid, batch_id, job_id and coresponding batch_resource
    const aggArGUIDData = data.reduce((acc, curr) => {
        const { cost } = curr
        const ar_guid = curr['ar-guid']
        const idx = acc.findIndex((d) => d.ar_guid === ar_guid)
        if (cost >= 0) {
            // do not include credits, should be filter out at API?
            if (idx === -1) {
                acc.push({
                    type: 'ar_guid',
                    key: ar_guid,
                    ar_guid,
                    batch_id: ' TOTAL',
                    job_id: ' ALL JOBS',
                    cost,
                })
            } else {
                acc[idx].cost += cost
            }
        }
        return acc
    }, [])

    const aggArGUIDResource = data.reduce((acc, curr) => {
        const { cost, batch_resource } = curr
        const ar_guid = curr['ar-guid']
        const idx = acc.findIndex(
            (d) => d.ar_guid === ar_guid && d.batch_resource === batch_resource
        )
        if (cost >= 0) {
            // do not include credits, should be filter out at API?
            if (idx === -1) {
                acc.push({ type: 'ar_guid', key: ar_guid, ar_guid, batch_resource, cost })
            } else {
                acc[idx].cost += cost
            }
        }
        return acc
    }, [])

    const aggBatchData = data.reduce((acc, curr) => {
        const { batch_id, url, topic, namespace, batch_name, cost } = curr
        const ar_guid = curr['ar-guid']
        const idx = acc.findIndex(
            (d) =>
                d.batch_id === batch_id &&
                d.batch_name === batch_name &&
                d.topic === topic &&
                d.namespace === namespace
        )
        if (cost >= 0) {
            // do not include credits, should be filter out at API?
            if (idx === -1) {
                acc.push({
                    type: 'batch_id',
                    key: batch_id,
                    ar_guid,
                    batch_id,
                    url,
                    topic,
                    namespace,
                    batch_name,
                    job_id: ' ALL JOBS',
                    cost,
                })
            } else {
                acc[idx].cost += cost
            }
        }
        return acc
    }, [])

    const aggBatchResource = data.reduce((acc, curr) => {
        const { batch_id, batch_resource, topic, namespace, batch_name, cost } = curr
        const ar_guid = curr['ar-guid']
        const idx = acc.findIndex(
            (d) =>
                d.batch_id === batch_id &&
                d.batch_name === batch_name &&
                d.batch_resource === batch_resource &&
                d.topic === topic &&
                d.namespace === namespace
        )
        if (cost >= 0) {
            // do not include credits, should be filter out at API?
            if (idx === -1) {
                acc.push({
                    type: 'batch_id',
                    key: batch_id,
                    ar_guid,
                    batch_id,
                    batch_resource,
                    topic,
                    namespace,
                    batch_name,
                    cost,
                })
            } else {
                acc[idx].cost += cost
            }
        }
        return acc
    }, [])

    const aggBatchJobData = data.reduce((acc, curr) => {
        const { batch_id, url, cost, topic, namespace, job_id } = curr
        const ar_guid = curr['ar-guid']
        const idx = acc.findIndex(
            (d) =>
                d.batch_id === batch_id &&
                d.job_id === job_id &&
                d.topic === topic &&
                d.namespace === namespace
        )
        if (cost >= 0) {
            if (idx === -1) {
                acc.push({
                    type: 'batch_id/job_id',
                    key: `${batch_id}/${job_id}`,
                    batch_id,
                    job_id,
                    ar_guid,
                    url,
                    topic,
                    namespace,
                    cost,
                })
            } else {
                acc[idx].cost += cost
            }
        }
        return acc
    }, [])

    const aggBatchJobResource = data.reduce((acc, curr) => {
        const { batch_id, batch_resource, topic, namespace, cost, job_id } = curr
        const ar_guid = curr['ar-guid']
        const idx = acc.findIndex(
            (d) =>
                d.batch_id === batch_id &&
                d.job_id === job_id &&
                d.batch_resource === batch_resource &&
                d.topic === topic &&
                d.namespace === namespace
        )
        if (cost >= 0) {
            if (idx === -1) {
                acc.push({
                    type: 'batch_id/job_id',
                    key: `${batch_id}/${job_id}`,
                    batch_id,
                    job_id,
                    ar_guid,
                    batch_resource,
                    topic,
                    namespace,
                    cost,
                })
            } else {
                acc[idx].cost += cost
            }
        }
        return acc
    }, [])

    const aggData = [...aggArGUIDData, ...aggBatchData, ...aggBatchJobData]
    const aggResource = [...aggArGUIDResource, ...aggBatchResource, ...aggBatchJobResource]

    // combine data and resource for each ar_guid, batch_id, job_id
    const combinedData = aggData.map((dataItem) => {
        const details = aggResource.filter(
            (resourceItem) =>
                resourceItem.key === dataItem.key && resourceItem.type === dataItem.type
        )
        return { ...dataItem, details }
    })

    const [openRows, setOpenRows] = React.useState<number[]>([])

    const handleToggle = (position: number) => {
        if (!openRows.includes(position)) {
            setOpenRows([...openRows, position])
        } else {
            setOpenRows(openRows.filter((i) => i !== position))
        }
    }

    const MAIN_FIELDS: Field[] = [
        {
            category: 'ar_guid',
            title: 'AR GUID',
        },
        {
            category: 'url',
            title: 'HAIL BATCH',
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
                    trigger={<span>${data.cost.toFixed(4)}</span>}
                    position="top center"
                />
            ),
        },
    ]

    const DETAIL_FIELDS: Field[] = [
        {
            category: 'topic',
            title: 'TOPIC',
        },
        {
            category: 'namespace',
            title: 'NAMESPACE',
        },
        {
            category: 'batch_name',
            title: 'NAME/SCRIPT',
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
                {combinedData
                    .sort((a, b) => {
                        if (a.batch_id < b.batch_id) {
                            return -1
                        }
                        if (a.batch_id > b.batch_id) {
                            return 1
                        }
                        if (a.job_id < b.job_id) {
                            return -1
                        }
                        if (a.job_id > b.job_id) {
                            return 1
                        }
                        return 0
                    })
                    .map((log, idx) => (
                        <React.Fragment key={log.key}>
                            <SUITable.Row>
                                <SUITable.Cell collapsing>
                                    <Checkbox
                                        checked={openRows.includes(log.key)}
                                        slider
                                        onChange={() => handleToggle(log.key)}
                                    />
                                </SUITable.Cell>
                                {expandedRow(log)}
                            </SUITable.Row>
                            {Object.entries(log)
                                .filter(([c]) =>
                                    DETAIL_FIELDS.map(({ category }) => category).includes(c)
                                )
                                .map(([k, v]) => {
                                    const detailField = DETAIL_FIELDS.find(
                                        ({ category }) => category === k
                                    )
                                    const title = detailField ? detailField.title : k
                                    return (
                                        <SUITable.Row
                                            style={{
                                                display: openRows.includes(log.key)
                                                    ? 'table-row'
                                                    : 'none',
                                                backgroundColor: 'var(--color-bg)',
                                            }}
                                            key={`${log.key}-detail-${k}`}
                                        >
                                            <SUITable.Cell style={{ border: 'none' }} />
                                            <SUITable.Cell>
                                                <b>{title}</b>
                                            </SUITable.Cell>
                                            <SUITable.Cell colSpan="3">{v}</SUITable.Cell>
                                        </SUITable.Row>
                                    )
                                })}
                            <SUITable.Row
                                style={{
                                    display: openRows.includes(log.key) ? 'table-row' : 'none',
                                    backgroundColor: 'var(--color-bg)',
                                }}
                                key={`${log.key}-lbl`}
                            >
                                <SUITable.Cell style={{ border: 'none' }} />
                                <SUITable.Cell colSpan="4">
                                    <b>COST BREAKDOWN</b>
                                </SUITable.Cell>
                            </SUITable.Row>
                            {typeof log === 'object' &&
                                'details' in log &&
                                _.orderBy(log?.details, ['cost'], ['desc']).map((dk) => (
                                    <SUITable.Row
                                        style={{
                                            display: openRows.includes(log.key)
                                                ? 'table-row'
                                                : 'none',
                                            backgroundColor: 'var(--color-bg)',
                                        }}
                                        key={`${log.key}-${dk.batch_resource}`}
                                    >
                                        <SUITable.Cell style={{ border: 'none' }} />
                                        <SUITable.Cell colSpan="3">
                                            {dk.batch_resource}
                                        </SUITable.Cell>
                                        <SUITable.Cell>${dk.cost.toFixed(4)}</SUITable.Cell>
                                    </SUITable.Row>
                                ))}
                        </React.Fragment>
                    ))}
            </SUITable.Body>
        </Table>
    )
}

export default HailBatchGrid
