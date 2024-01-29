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
    const aggArGUIDData: any[] = []
    data.forEach((curr) => {
        const { cost, topic, usage_start_time, usage_end_time } = curr
        const ar_guid = curr['ar-guid']
        const usageStartDate = new Date(usage_start_time)
        const usageEndDate = new Date(usage_end_time)
        const idx = aggArGUIDData.findIndex((d) => d.ar_guid === ar_guid && d.topic === topic)
        if (cost >= 0) {
            // do not include credits, should be filter out at API?
            if (idx === -1) {
                aggArGUIDData.push({
                    type: 'ar_guid',
                    key: ar_guid,
                    ar_guid,
                    batch_id: undefined,
                    job_id: undefined,
                    topic,
                    cost,
                    start_time: usageStartDate,
                    end_time: usageEndDate,
                })
            } else {
                aggArGUIDData[idx].cost += cost
                aggArGUIDData[idx].start_time = new Date(
                    Math.min(usageStartDate.getTime(), aggArGUIDData[idx].start_time.getTime())
                )
                aggArGUIDData[idx].end_time = new Date(
                    Math.max(usageEndDate.getTime(), aggArGUIDData[idx].end_time.getTime())
                )
            }
        }
    })

    const aggArGUIDResource: any[] = []
    data.forEach((curr) => {
        const { cost, batch_resource } = curr
        const ar_guid = curr['ar-guid']
        const idx = aggArGUIDResource.findIndex(
            (d) => d.ar_guid === ar_guid && d.batch_resource === batch_resource
        )
        if (cost >= 0) {
            // do not include credits, should be filter out at API?
            if (idx === -1) {
                aggArGUIDResource.push({
                    type: 'ar_guid',
                    key: ar_guid,
                    ar_guid,
                    batch_resource,
                    cost,
                })
            } else {
                aggArGUIDResource[idx].cost += cost
            }
        }
    })
    const aggBatchData: any[] = []
    data.forEach((curr) => {
        const {
            batch_id,
            url,
            topic,
            namespace,
            batch_name,
            cost,
            usage_start_time,
            usage_end_time,
        } = curr
        const ar_guid = curr['ar-guid']
        const usageStartDate = new Date(usage_start_time)
        const usageEndDate = new Date(usage_end_time)
        const idx = aggBatchData.findIndex(
            (d) =>
                d.batch_id === batch_id &&
                d.batch_name === batch_name &&
                d.topic === topic &&
                d.namespace === namespace
        )
        if (cost >= 0) {
            // do not include credits, should be filter out at API?
            if (idx === -1) {
                aggBatchData.push({
                    type: 'batch_id',
                    key: batch_id,
                    ar_guid,
                    batch_id,
                    url,
                    topic,
                    namespace,
                    batch_name,
                    job_id: undefined,
                    cost,
                    start_time: usageStartDate,
                    end_time: usageEndDate,
                })
            } else {
                aggBatchData[idx].cost += cost
                aggBatchData[idx].start_time = new Date(
                    Math.min(usageStartDate.getTime(), aggBatchData[idx].start_time.getTime())
                )
                aggBatchData[idx].end_time = new Date(
                    Math.max(usageEndDate.getTime(), aggBatchData[idx].end_time.getTime())
                )
            }
        }
    })

    const aggBatchResource: any[] = []
    data.forEach((curr) => {
        const { batch_id, batch_resource, topic, namespace, batch_name, cost } = curr
        const ar_guid = curr['ar-guid']
        const idx = aggBatchResource.findIndex(
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
                aggBatchResource.push({
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
                aggBatchResource[idx].cost += cost
            }
        }
    })

    const aggBatchJobData: any[] = []
    data.forEach((curr) => {
        const { batch_id, url, cost, topic, namespace, job_id, usage_start_time, usage_end_time } =
            curr
        const ar_guid = curr['ar-guid']
        const usageStartDate = new Date(usage_start_time)
        const usageEndDate = new Date(usage_end_time)
        const idx = aggBatchJobData.findIndex(
            (d) =>
                d.batch_id === batch_id &&
                d.job_id === job_id &&
                d.topic === topic &&
                d.namespace === namespace
        )
        if (cost >= 0) {
            if (idx === -1) {
                aggBatchJobData.push({
                    type: 'batch_id/job_id',
                    key: `${batch_id}/${job_id}`,
                    batch_id,
                    job_id,
                    ar_guid,
                    url,
                    topic,
                    namespace,
                    cost,
                    start_time: usageStartDate,
                    end_time: usageEndDate,
                })
            } else {
                aggBatchJobData[idx].cost += cost
                aggBatchJobData[idx].start_time = new Date(
                    Math.min(usageStartDate.getTime(), aggBatchJobData[idx].start_time.getTime())
                )
                aggBatchJobData[idx].end_time = new Date(
                    Math.max(usageEndDate.getTime(), aggBatchJobData[idx].end_time.getTime())
                )
            }
        }
    })

    const aggBatchJobResource: any[] = []
    data.forEach((curr) => {
        const { batch_id, batch_resource, topic, namespace, cost, job_id, job_name } = curr
        const ar_guid = curr['ar-guid']
        const idx = aggBatchJobResource.findIndex(
            (d) =>
                d.batch_id === batch_id &&
                d.job_id === job_id &&
                d.batch_resource === batch_resource &&
                d.topic === topic &&
                d.namespace === namespace
        )
        if (cost >= 0) {
            if (idx === -1) {
                aggBatchJobResource.push({
                    type: 'batch_id/job_id',
                    key: `${batch_id}/${job_id}`,
                    batch_id,
                    job_id,
                    ar_guid,
                    batch_resource,
                    topic,
                    namespace,
                    cost,
                    job_name,
                })
            } else {
                aggBatchJobResource[idx].cost += cost
            }
        }
    })

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

    const prepareBatchUrl = (url: string, txt: string) => (
        <a href={`${url}`} rel="noopener noreferrer" target="_blank">
            {txt}
        </a>
    )

    const prepareBgColor = (log: any) => {
        if (log.batch_id === undefined) {
            return 'var(--color-border-color)'
        }
        if (log.job_id === undefined) {
            return 'var(--color-border-default)'
        }
        return 'var(--color-bg)'
    }

    const MAIN_FIELDS: Field[] = [
        {
            category: 'job_id',
            title: 'ID',
            dataMap: (dataItem: any, value: string) => {
                if (dataItem.batch_id === undefined) {
                    return `AR GUID: ${dataItem.ar_guid}`
                }
                if (dataItem.job_id === undefined) {
                    return prepareBatchUrl(dataItem.url, `BATCH ID: ${dataItem.batch_id}`)
                }
                return prepareBatchUrl(dataItem.url, `JOB: ${value}`)
            },
        },
        {
            category: 'start_time',
            title: 'TIME STARTED',
            dataMap: (dataItem: any, value: string) => {
                const dateValue = new Date(value)
                return (
                    <span>
                        {Number.isNaN(dateValue.getTime()) ? '' : dateValue.toLocaleString()}
                    </span>
                )
            },
        },
        {
            category: 'end_time',
            title: 'TIME COMPLETED',
            dataMap: (dataItem: any, value: string) => {
                const dateValue = new Date(value)
                return (
                    <span>
                        {Number.isNaN(dateValue.getTime()) ? '' : dateValue.toLocaleString()}
                    </span>
                )
            },
        },
        {
            category: 'duration',
            title: 'DURATION',
            dataMap: (dataItem: any, _value: string) => {
                const duration = new Date(
                    dataItem.end_time.getTime() - dataItem.start_time.getTime()
                )
                const seconds = Math.floor((duration / 1000) % 60)
                const minutes = Math.floor((duration / (1000 * 60)) % 60)
                const hours = Math.floor((duration / (1000 * 60 * 60)) % 24)
                const formattedDuration = `${hours}h ${minutes}m ${seconds}s`
                return <span>{formattedDuration}</span>
            },
        },
        {
            category: 'cost',
            title: 'COST',
            dataMap: (dataItem: any, _value: string) => (
                <Popup
                    content={dataItem.cost}
                    trigger={<span>${dataItem.cost.toFixed(4)}</span>}
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
        {
            category: 'job_name',
            title: 'NAME',
        },
    ]

    const expandedRow = (log: any, idx: any) =>
        MAIN_FIELDS.map(({ category, dataMap, className }) => (
            <SUITable.Cell key={`${category}-${idx}`} className={className}>
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
                                textAlign: 'center',
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
                    {MAIN_FIELDS.map(({ category }, i) => (
                        <SUITable.Cell
                            className="sizeRow"
                            key={`${category}-resize-${i}`}
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
<<<<<<< HEAD
=======
                        // Sorts an array of objects first by 'batch_id' and then by 'job_id' in ascending order.
>>>>>>> dev
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
                            <SUITable.Row
                                className={log.job_id === undefined ? 'bold-text' : ''}
                                style={{
                                    backgroundColor: prepareBgColor(log),
                                    textAlign: 'center',
                                }}
                            >
                                <SUITable.Cell collapsing>
                                    <Checkbox
                                        checked={openRows.includes(log.key)}
                                        toggle
                                        onChange={() => handleToggle(log.key)}
                                    />
                                </SUITable.Cell>
                                {expandedRow(log, idx)}
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
                                            <SUITable.Cell colSpan="4">{v}</SUITable.Cell>
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
                                <SUITable.Cell colSpan="5">
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
                                        <SUITable.Cell colSpan="4">
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
