import * as React from 'react'
import { Table as SUITable, Popup, Checkbox } from 'semantic-ui-react'
import _ from 'lodash'
// import Table from '../../../shared/components/Table'
import sanitiseValue from '../../../shared/utilities/sanitiseValue'
import '../../project/AnalysisRunnerView/AnalysisGrid.css'
import { TableVirtuoso } from 'react-virtuoso'

import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Paper from "@mui/material/Paper";


interface Field {
    category: string
    title: string
    width?: string
    className?: string
    dataMap?: (data: any, value: string) => any
}

const hailBatchUrl = 'https://batch.hail.populationgenomics.org.au/batches'

const HailBatchGridTempVirtuoso: React.FunctionComponent<{
    data: any[]
}> = ({ data }) => {
    // prepare aggregated data by ar_guid, batch_id, job_id and coresponding batch_resource

    // combine data and resource for each ar_guid, batch_id, job_id
    const combinedData = data

    console.log('combinedData', combinedData)

    const [openRows, setOpenRows] = React.useState<number[]>([])

    const handleToggle = (position: number) => {
        if (!openRows.includes(position)) {
            setOpenRows([...openRows, position])
        } else {
            setOpenRows(openRows.filter((i) => i !== position))
        }
    }

    const prepareBatchUrl = (batch_id: string) => (
        <a href={`${hailBatchUrl}/${batch_id}`} rel="noopener noreferrer" target="_blank">
            BATCH ID: {batch_id}
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
                if (dataItem.batch_id !== undefined && dataItem.batch_id !== null) {
                    if (dataItem.job_id !== undefined && dataItem.job_id !== null) {
                        return `JOB: ${value}`
                    }
                    return prepareBatchUrl(dataItem.batch_id)
                }

                if (dataItem.wdl_task_name !== undefined && dataItem.wdl_task_name !== null) {
                    return `WDL TASK: ${dataItem.wdl_task_name}`
                }

                if (
                    dataItem.cromwell_sub_workflow_name !== undefined &&
                    dataItem.cromwell_sub_workflow_name !== null
                ) {
                    return `CROMWELL SUB WORKLOW: ${dataItem.cromwell_sub_workflow_name}`
                }

                if (
                    dataItem.cromwell_workflow_id !== undefined &&
                    dataItem.cromwell_workflow_id !== null
                ) {
                    return `CROMWELL WORKLOW ID: ${dataItem.cromwell_workflow_id}`
                }

                return `AR GUID: ${dataItem.ar_guid}`
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
        {
            category: 'stage',
            title: 'STAGE',
        },
        {
            category: 'tool',
            title: 'TOOL',
        },
        {
            category: 'sequencing_type',
            title: 'SEQUENCING TYPE',
        }
    ]

    const expandedRow = (item: any, idx: any) =>
        MAIN_FIELDS.map(({ category, dataMap, className }) => (
            <SUITable.Cell key={`${category}-${idx}`} className={className} style={{width: 350}}>
                {dataMap ? dataMap(item, item[category]) : sanitiseValue(item[category])}
            </SUITable.Cell>
        ))

    const ExpandableRow = ({ item, ...props }) => {
        const index = props['data-index'];
        return ( 
            <React.Fragment key={index}>
                <TableRow {...props}
                    className={item.job_id === null ? 'bold-text' : ''}
                    style={{
                        backgroundColor: prepareBgColor(item),
                        textAlign: 'center',
                    }}
                >

                <SUITable.Cell style={{width: 50}}>
                <Checkbox
                        checked={openRows.includes(index)}
                        toggle
                        onChange={() => handleToggle(index)}
                    />
                </SUITable.Cell>
                {expandedRow(item, index)}
                </TableRow>

                {Object.entries(item)
                    .filter(([c]) =>
                        DETAIL_FIELDS.map(({ category }) => category).includes(c)
                    )
                    .map(([k, v]) => {
                        if (v === null) {
                            return null; // Exclude rows with null value
                        }
                        const detailField = DETAIL_FIELDS.find(
                            ({ category }) => category === k
                        )
                        const title = detailField ? detailField.title : k
                        return (
                            <SUITable.Row
                                style={{
                                    display: openRows.includes(index) ? 'table-row' : 'none',
                                    backgroundColor: 'var(--color-bg)',
                                }}
                                key={`${index}-detail-${k}`}
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
                            display: openRows.includes(index) ? 'table-row' : 'none',
                            backgroundColor: 'var(--color-bg)',
                        }}
                        key={`${index}-lbl`}
                    >
                        <SUITable.Cell style={{ border: 'none' }} />
                        <SUITable.Cell colSpan="5">
                            <b>COST BREAKDOWN</b>
                        </SUITable.Cell>
                    </SUITable.Row>

                    {typeof item === 'object' &&
                        'details' in item &&
                        _.orderBy(item?.details, ['cost'], ['desc']).map((dk) => (
                            <SUITable.Row
                                style={{
                                    display: openRows.includes(index) ? 'table-row' : 'none',
                                    backgroundColor: 'var(--color-bg)',
                                }}
                                key={`${index}-${dk.sku}`}
                            >
                                <SUITable.Cell style={{ border: 'none' }} />
                                <SUITable.Cell colSpan="4">{dk.sku}</SUITable.Cell>
                                <SUITable.Cell>${dk.cost.toFixed(4)}</SUITable.Cell>
                            </SUITable.Row>
                        ))}
            </React.Fragment>
        )
    }


        
        const TableComponents = {
        Scroller: React.forwardRef((props, ref) => (
            <TableContainer component={Paper} {...props} ref={ref} />
        )),
        Table: (props) => <Table {...props} style={{ borderCollapse: "separate" }} />,
        TableHead: TableHead,
        TableRow: ExpandableRow, 
        TableBody: React.forwardRef((props, ref) => (
            <TableBody {...props} ref={ref} />
        ))
        };
    
    return (
        <TableVirtuoso
            style={{ height: 600 }}
            useWindowScroll
            class='ui celled table compact'
            data={combinedData .sort((a, b) => {
                // Sorts an array of objects first by 'batch_id' and then by 'job_id' in ascending order.
                if (a.ar_guid < b.ar_guid) {
                    return -1
                }
                if (a.ar_guid > b.ar_guid) {
                    return 1
                }
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
                // if (a.cost < b.cost) {
                //     return 1
                // }
                // if (a.cost > b.cost) {
                //     return -1
                // }


                if (a.wdl_task_name < b.wdl_task_name) {
                    return -1
                }
                if (a.wdl_task_name > b.wdl_task_name) {
                    return 1
                }
                if (a.cromwell_sub_workflow_name < b.cromwell_sub_workflow_name) {
                    return -1
                }
                if (a.cromwell_sub_workflow_name > b.cromwell_sub_workflow_name) {
                    return 1
                }
                if (a.cromwell_workflow_id < b.cromwell_workflow_id) {
                    return -1
                }
                if (a.cromwell_workflow_id > b.cromwell_workflow_id) {
                    return 1
                }

                return 0
            })}
            components={TableComponents}
            fixedHeaderContent={() => (
                <SUITable.Row style={{ z_index: 999}}>
                    <SUITable.HeaderCell style={{ width: 50 }} />
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
    
            )}
            context={{
                combinedData,
            }}
        />
    )
}

export default HailBatchGridTempVirtuoso
