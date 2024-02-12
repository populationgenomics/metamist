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

const HailBatchGridTemp: React.FunctionComponent<{
    data: any[]
}> = ({ data }) => {
    // prepare aggregated data by ar_guid, batch_id, job_id and coresponding batch_resource
    
    // combine data and resource for each ar_guid, batch_id, job_id
    const combinedData = data

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
                if (dataItem.batch_id === undefined || dataItem.batch_id === null) {
                    return `AR GUID: ${dataItem.ar_guid}`
                }
                if (dataItem.job_id === undefined || dataItem.job_id === null) {
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
                // const duration = new Date(
                //     dataItem.end_time.getTime() - dataItem.start_time.getTime()
                // )

                const duration = new Date(dataItem.end_time) - new Date(dataItem.start_time)
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
            category: 'jobs',
            title: 'JOBS COUNT',
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
                        // Sorts an array of objects first by 'batch_id' and then by 'job_id' in ascending order.
                        if (a.batch_id < b.batch_id) {
                            return -1
                        }
                        if (a.batch_id > b.batch_id) {
                            return 1
                        }
                        if (a.cost < b.cost) {
                            return 1
                        }
                        if (a.cost > b.cost) {
                            return -1
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
                        <React.Fragment key={idx}>
                            <SUITable.Row
                                className={(log.job_id === undefined || log.job_id === null) ? 'bold-text' : ''}
                                style={{
                                    backgroundColor: prepareBgColor(log),
                                    textAlign: 'center',
                                }}
                            >
                                <SUITable.Cell collapsing>
                                    <Checkbox
                                        checked={openRows.includes(idx)}
                                        toggle
                                        onChange={() => handleToggle(idx)}
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
                                                display: openRows.includes(idx)
                                                    ? 'table-row'
                                                    : 'none',
                                                backgroundColor: 'var(--color-bg)',
                                            }}
                                            key={`${idx}-detail-${k}`}
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
                                    display: openRows.includes(idx) ? 'table-row' : 'none',
                                    backgroundColor: 'var(--color-bg)',
                                }}
                                key={`${idx}-lbl`}
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
                                            display: openRows.includes(idx)
                                                ? 'table-row'
                                                : 'none',
                                            backgroundColor: 'var(--color-bg)',
                                        }}
                                        key={`${idx}-${dk.sku}`}
                                    >
                                        <SUITable.Cell style={{ border: 'none' }} />
                                        <SUITable.Cell colSpan="4">
                                            {dk.sku}
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

export default HailBatchGridTemp
