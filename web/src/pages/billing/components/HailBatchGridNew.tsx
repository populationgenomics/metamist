import * as React from 'react'
import { Table as SUITable, Popup, Checkbox } from 'semantic-ui-react'
import _ from 'lodash'
// import Table from '../../../shared/components/Table'
import { DonutChart } from '../../../shared/components/Graphs/DonutChart'
import sanitiseValue from '../../../shared/utilities/sanitiseValue'
import '../../project/AnalysisRunnerView/AnalysisGrid.css'
import { TableVirtuoso } from 'react-virtuoso'

import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableContainer from '@mui/material/TableContainer'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Paper from '@mui/material/Paper'
import formatMoney from '../../../shared/utilities/formatMoney'

interface Field {
    category: string
    title: string
    width?: string
    className?: string
    dataMap?: (data: any, value: string) => any
}

const hailBatchUrl = 'https://batch.hail.populationgenomics.org.au/batches'

const HailBatchGridNew: React.FunctionComponent<{
    data: any
}> = ({ data }) => {
    // prepare aggregated data by ar_guid, batch_id, job_id and coresponding batch_resource

    // combine data and resource for each ar_guid, batch_id, job_id
    console.log('data', data)

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
            category: 'compute_category',
            title: 'COMPUTE CATEGORY',
        },
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
        },
    ]

    const expandedRow = (item: any, idx: any) =>
        MAIN_FIELDS.map(({ category, dataMap, className }) => (
            <SUITable.Cell key={`${category}-${idx}`} className={className} style={{ width: 350 }}>
                {dataMap ? dataMap(item, item[category]) : sanitiseValue(item[category])}
            </SUITable.Cell>
        ))

    const calcDuration = (dataItem) => {
        const duration = new Date(dataItem.usage_end_time) - new Date(dataItem.usage_start_time)
        const seconds = Math.floor((duration / 1000) % 60)
        const minutes = Math.floor((duration / (1000 * 60)) % 60)
        const hours = Math.floor((duration / (1000 * 60 * 60)) % 24)
        const formattedDuration = `${hours}h ${minutes}m ${seconds}s`
        return <span>{formattedDuration}</span>
    }

    const ExpandableRow = ({ item, ...props }) => {
        const index = props['data-index']
        // console.log('item', item, 'props', props, 'index', index)
        return (
            <React.Fragment key={`${item.batch_id}-${item.job_id}`}>
                <TableRow
                    {...props}
                    className={item.job_id === null ? 'bold-text' : ''}
                    style={{
                        backgroundColor: prepareBgColor(item),
                        textAlign: 'center',
                    }}
                >
                    <SUITable.Cell style={{ width: 50 }}>
                        <Checkbox
                            checked={openRows.includes(`${item.batch_id}-${item.job_id}`)}
                            toggle
                            onChange={() => handleToggle(`${item.batch_id}-${item.job_id}`)}
                        />
                    </SUITable.Cell>
                    <SUITable.Cell>{item.job_id}</SUITable.Cell>
                    <SUITable.Cell>{item.job_name}</SUITable.Cell>
                    <SUITable.Cell>{item.usage_start_time}</SUITable.Cell>
                    <SUITable.Cell>{calcDuration(item)}</SUITable.Cell>
                    <SUITable.Cell>{formatMoney(item.cost, 4)}</SUITable.Cell>
                </TableRow>

                {/* {Object.entries(item)
                    .filter(([c]) => DETAIL_FIELDS.map(({ category }) => category).includes(c))
                    .map(([k, v]) => {
                        if (v === null) {
                            return null // Exclude rows with null value
                        }
                        const detailField = DETAIL_FIELDS.find(({ category }) => category === k)
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
                    ))} */}
            </React.Fragment>
        )
    }

    const TableComponents = {
        Scroller: React.forwardRef((props, ref) => (
            <TableContainer component={Paper} {...props} ref={ref} />
        )),
        Table: (props) => <Table {...props} style={{ borderCollapse: 'separate' }} />,
        TableHead: TableHead,
        TableRow: ExpandableRow,
        TableBody: React.forwardRef((props, ref) => <TableBody {...props} ref={ref} />),
    }

    const idx = 0

    return (
        <>
            <SUITable celled compact>
                <SUITable.Body>
                    <>
                        <SUITable.Row key={idx}>
                            <SUITable.Cell style={{ width: 50 }}>
                                <Checkbox
                                    checked={openRows.includes(idx)}
                                    toggle
                                    onChange={() => handleToggle(idx)}
                                />
                            </SUITable.Cell>

                            <SUITable.Cell colspan="2">
                                AR-GUID: {data.total.ar_guid}
                            </SUITable.Cell>
                        </SUITable.Row>

                        <SUITable.Row
                            style={{
                                display: openRows.includes(idx) ? 'table-row' : 'none',
                                backgroundColor: 'var(--color-bg)',
                            }}
                            key={`${idx}-detail-2`}
                        >
                            <SUITable.Cell style={{ border: 'none' }} />
                            <SUITable.Cell style={{ width: 250 }}>
                                <b>Start</b>
                            </SUITable.Cell>
                            <SUITable.Cell>${data.total.usage_start_time}</SUITable.Cell>
                        </SUITable.Row>

                        <SUITable.Row
                            style={{
                                display: openRows.includes(idx) ? 'table-row' : 'none',
                                backgroundColor: 'var(--color-bg)',
                            }}
                            key={`${idx}-detail-3`}
                        >
                            <SUITable.Cell style={{ border: 'none' }} />
                            <SUITable.Cell style={{ width: 250 }}>
                                <b>End</b>
                            </SUITable.Cell>
                            <SUITable.Cell>${data.total.usage_end_time}</SUITable.Cell>
                        </SUITable.Row>

                        <SUITable.Row
                            style={{
                                display: openRows.includes(idx) ? 'table-row' : 'none',
                                backgroundColor: 'var(--color-bg)',
                            }}
                            key={`${idx}-detail-3`}
                        >
                            <SUITable.Cell style={{ border: 'none' }} />
                            <SUITable.Cell style={{ width: 250 }}>
                                <b>Status</b>
                            </SUITable.Cell>
                            <SUITable.Cell>{data.analysisRunnerLog.status}</SUITable.Cell>
                        </SUITable.Row>

                        {/* all meta */}

                        {Object.keys(data.analysisRunnerLog.meta).map((key) => {
                            const mcat = data.analysisRunnerLog.meta[key];
                            return (
                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(idx) ? 'table-row' : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${idx}-detail-${key}`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>{key}</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>{mcat}</SUITable.Cell>
                                </SUITable.Row>
                            );
                        })}
    
                        <SUITable.Row
                            style={{
                                display: openRows.includes(idx) ? 'table-row' : 'none',
                                backgroundColor: 'var(--color-bg)',
                            }}
                            key={`${idx}-detail-1`}
                        >
                            <SUITable.Cell style={{ border: 'none' }} />
                            <SUITable.Cell style={{ width: 250 }}>
                                <b>Total Cost</b>
                            </SUITable.Cell>
                            <SUITable.Cell>{formatMoney(data.total.cost, 2)}</SUITable.Cell>
                        </SUITable.Row>

                        <SUITable.Row
                            style={{
                                display: openRows.includes(idx) ? 'table-row' : 'none',
                                backgroundColor: 'var(--color-bg)',
                            }}
                            key={`${idx}-detail-1`}
                        >
                            <SUITable.Cell style={{ border: 'none' }} />
                            <SUITable.Cell style={{ width: 250 }}></SUITable.Cell>
                            <SUITable.Cell>
                                <SUITable celled compact>
                                    <SUITable.Header>
                                        <SUITable.Row>
                                            <SUITable.HeaderCell>Topic</SUITable.HeaderCell>
                                            <SUITable.HeaderCell>Cost</SUITable.HeaderCell>
                                        </SUITable.Row>
                                    </SUITable.Header>
                                    <SUITable.Body>
                                        {data.topics.map((trec, tidx) => (
                                            <SUITable.Row>
                                                <SUITable.Cell>{trec.topic}</SUITable.Cell>
                                                <SUITable.Cell>
                                                    {formatMoney(trec.cost, 2)}
                                                </SUITable.Cell>
                                            </SUITable.Row>
                                        ))}
                                    </SUITable.Body>
                                </SUITable>
                            </SUITable.Cell>
                        </SUITable.Row>

                        {data.categories.map((tcat, cidx) => (
                            <SUITable.Row
                                style={{
                                    display: openRows.includes(idx) ? 'table-row' : 'none',
                                    backgroundColor: 'var(--color-bg)',
                                }}
                                key={`${idx}-detail-1`}
                            >
                                <SUITable.Cell style={{ border: 'none' }} />
                                <SUITable.Cell style={{ width: 250 }}>
                                    <b>{tcat.category}</b>
                                </SUITable.Cell>
                                <SUITable.Cell>
                                    {formatMoney(tcat.cost, 2)}{' '}
                                    {tcat.workflows !== null
                                        ? `(across ${tcat.workflows} workflows)`
                                        : ''}
                                </SUITable.Cell>
                            </SUITable.Row>
                        ))}

                        <SUITable.Row
                            style={{
                                display: openRows.includes(idx) ? 'table-row' : 'none',
                                backgroundColor: 'var(--color-bg)',
                            }}
                            key={`BySeqGrp${idx}`}
                        >
                            <SUITable.Cell style={{ border: 'none' }} />
                            <SUITable.Cell style={{ width: 50 }}>
                                <Checkbox
                                    checked={openRows.includes(`BySeqGrp${idx}`)}
                                    toggle
                                    onChange={() => handleToggle(`BySeqGrp${idx}`)}
                                />
                            </SUITable.Cell>
                            <SUITable.Cell>Cost By Sequencing Group</SUITable.Cell>
                        </SUITable.Row>

                        <SUITable.Row
                            style={{
                                display: openRows.includes(`BySeqGrp${idx}`)
                                    ? 'table-row'
                                    : 'none',
                                backgroundColor: 'var(--color-bg)',
                            }}
                            key={`BySeqGrp${idx}-details`}
                        >
                            <SUITable.Cell style={{ border: 'none' }} />
                            <SUITable.Cell style={{ width: 250 }}>
                            </SUITable.Cell>
                            <SUITable.Cell>
                                <SUITable celled compact>
                                    <SUITable.Header>
                                        <SUITable.Row>
                                            <SUITable.HeaderCell>SEQ GROUP</SUITable.HeaderCell>
                                            <SUITable.HeaderCell>STAGE</SUITable.HeaderCell>
                                            <SUITable.HeaderCell>COST</SUITable.HeaderCell>
                                        </SUITable.Row>
                                    </SUITable.Header>
                                    <SUITable.Body>
                                        {data.seq_groups
                                            .sort((a, b) => b.cost - a.cost) // Sort by cost in descending order
                                            .map((gcat, gidx) => (
                                            <SUITable.Row>
                                                <SUITable.Cell>{gcat.sequencing_group}</SUITable.Cell>
                                                <SUITable.Cell>{gcat.stage}</SUITable.Cell>
                                                <SUITable.Cell>
                                                    {formatMoney(gcat.cost, 4)}
                                                </SUITable.Cell>
                                            </SUITable.Row>
                                        ))}
                                    </SUITable.Body>
                                </SUITable>
                            </SUITable.Cell>
                        </SUITable.Row>

                        <SUITable.Row
                            style={{
                                display: openRows.includes(idx) ? 'table-row' : 'none',
                                backgroundColor: 'var(--color-bg)',
                            }}
                            key={`ByLabels${idx}`}
                        >
                            <SUITable.Cell style={{ border: 'none' }} />
                            <SUITable.Cell style={{ width: 50 }}>
                                <Checkbox
                                    checked={openRows.includes(`ByLabels${idx}`)}
                                    toggle
                                    onChange={() => handleToggle(`ByLabels${idx}`)}
                                />
                            </SUITable.Cell>
                            <SUITable.Cell>Cost By SKU</SUITable.Cell>
                        </SUITable.Row>

                        <SUITable.Row
                            style={{
                                display: openRows.includes(`ByLabels${idx}`)
                                    ? 'table-row'
                                    : 'none',
                                backgroundColor: 'var(--color-bg)',
                            }}
                            key={`ByLabels${idx}-details`}
                        >
                            <SUITable.Cell style={{ border: 'none' }} />
                            <SUITable.Cell />
                            <SUITable.Cell>
                                <DonutChart
                                    data={data.skus.map((srec) => ({
                                        label: srec.sku,
                                        value: srec.cost,
                                    }))}
                                    maxSlices={data.skus.length}
                                    legendSize={0.6}
                                />

                                <SUITable celled compact>
                                    <SUITable.Header>
                                        <SUITable.Row>
                                            <SUITable.HeaderCell>SKU</SUITable.HeaderCell>
                                            <SUITable.HeaderCell>COST</SUITable.HeaderCell>
                                        </SUITable.Row>
                                    </SUITable.Header>
                                    <SUITable.Body>
                                        {data.skus.map((srec, sidx) => (
                                            <SUITable.Row>
                                                <SUITable.Cell>{srec.sku}</SUITable.Cell>
                                                <SUITable.Cell>
                                                    {formatMoney(srec.cost, 4)}
                                                </SUITable.Cell>
                                            </SUITable.Row>
                                        ))}
                                    </SUITable.Body>
                                </SUITable>
                            </SUITable.Cell>
                        </SUITable.Row>
                    </>
                </SUITable.Body>
            </SUITable>

            <SUITable celled compact>
                <SUITable.Body>
                    <>
                        {data.batches.map((brec, bidx) => (
                            <>
                                <SUITable.Row key={brec.batch_id}>
                                    <SUITable.Cell style={{ width: 50 }}>
                                        <Checkbox
                                            checked={openRows.includes(brec.batch_id)}
                                            toggle
                                            onChange={() => handleToggle(brec.batch_id)}
                                        />
                                    </SUITable.Cell>

                                    <SUITable.Cell colspan="2">
                                        BATCH: {brec.batch_id}
                                    </SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.batch_id)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${brec.batch_id}-detail-2`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>Batch Name</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>{brec.batch_name}</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.batch_id)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${brec.batch_id}-detail-2`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>Start</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>{brec.usage_start_time}</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.batch_id)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${brec.batch_id}-detail-3`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>End</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>{brec.usage_end_time}</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.batch_id)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${brec.batch_id}-detail-1`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>Total Cost</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>
                                        {formatMoney(brec.cost, 4)}{' '}
                                        {brec.jobs_cnt !== null
                                            ? `(across ${brec.jobs_cnt} jobs)`
                                            : ''}
                                    </SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.batch_id)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`ByLabels${brec.batch_id}`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 50 }}>
                                        <Checkbox
                                            checked={openRows.includes(
                                                `ByLabels${brec.batch_id}`
                                            )}
                                            toggle
                                            onChange={() =>
                                                handleToggle(`ByLabels${brec.batch_id}`)
                                            }
                                        />
                                    </SUITable.Cell>
                                    <SUITable.Cell>Cost By SKU</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(`ByLabels${brec.batch_id}`)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`ByLabels${brec.batch_id}-details`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell />
                                    <SUITable.Cell>
                                        <DonutChart
                                            data={brec.skus.map((srec) => ({
                                                label: srec.sku,
                                                value: srec.cost,
                                            }))}
                                            maxSlices={brec.skus.length}
                                            legendSize={0.6}
                                        />

                                        <SUITable celled compact>
                                            <SUITable.Header>
                                                <SUITable.Row>
                                                    <SUITable.HeaderCell>
                                                        SKU
                                                    </SUITable.HeaderCell>
                                                    <SUITable.HeaderCell>
                                                        COST
                                                    </SUITable.HeaderCell>
                                                </SUITable.Row>
                                            </SUITable.Header>
                                            <SUITable.Body>
                                                {brec.skus.map((srec, sidx) => (
                                                    <SUITable.Row>
                                                        <SUITable.Cell>
                                                            {srec.sku}
                                                        </SUITable.Cell>
                                                        <SUITable.Cell>
                                                            {formatMoney(srec.cost, 4)}
                                                        </SUITable.Cell>
                                                    </SUITable.Row>
                                                ))}
                                            </SUITable.Body>
                                        </SUITable>
                                    </SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.batch_id)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`BySeqGrp${brec.batch_id}`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 50 }}>
                                        <Checkbox
                                            checked={openRows.includes(
                                                `BySeqGrp${brec.batch_id}`
                                            )}
                                            toggle
                                            onChange={() =>
                                                handleToggle(`BySeqGrp${brec.batch_id}`)
                                            }
                                        />
                                    </SUITable.Cell>
                                    <SUITable.Cell>Cost By Sequencing Group</SUITable.Cell>
                                </SUITable.Row>
                                
                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(`BySeqGrp${brec.batch_id}`)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`BySeqGrp${brec.batch_id}-details`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                    </SUITable.Cell>
                                    <SUITable.Cell>
                                        <SUITable celled compact>
                                            <SUITable.Header>
                                                <SUITable.Row>
                                                    <SUITable.HeaderCell>SEQ GROUP</SUITable.HeaderCell>
                                                    <SUITable.HeaderCell>STAGE</SUITable.HeaderCell>
                                                    <SUITable.HeaderCell>COST</SUITable.HeaderCell>
                                                </SUITable.Row>
                                            </SUITable.Header>
                                            <SUITable.Body>
                                                {brec.seq_groups
                                                    .sort((a, b) => b.cost - a.cost) // Sort by cost in descending order
                                                    .map((gcat, gidx) => (
                                                    <SUITable.Row>
                                                        <SUITable.Cell>{gcat.sequencing_group}</SUITable.Cell>
                                                        <SUITable.Cell>{gcat.stage}</SUITable.Cell>
                                                        <SUITable.Cell>
                                                            {formatMoney(gcat.cost, 4)}
                                                        </SUITable.Cell>
                                                    </SUITable.Row>
                                                ))}
                                            </SUITable.Body>
                                        </SUITable>
                                    </SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.batch_id)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`ByLabels${brec.batch_id}`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 50 }}>
                                        <Checkbox
                                            checked={openRows.includes(
                                                `ByJobs${brec.batch_id}`
                                            )}
                                            toggle
                                            onChange={() =>
                                                handleToggle(`ByJobs${brec.batch_id}`)
                                            }
                                        />
                                    </SUITable.Cell>
                                    <SUITable.Cell>Cost By JOBS</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(`ByJobs${brec.batch_id}`)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`ByJobs${brec.batch_id}-details`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell />
                                    <SUITable.Cell>
                                        <TableVirtuoso
                                            style={{ height: 600 }}
                                            useWindowScroll
                                            class="ui celled table compact"
                                            data={brec.jobs.sort((a, b) => {
                                                // Sorts an array of objects first by 'batch_id' and then by 'job_id' in ascending order.
                                                if (a.job_id < b.job_id) {
                                                    return -1
                                                }
                                                if (a.job_id > b.job_id) {
                                                    return 1
                                                }
                                                return 0
                                            })}
                                            fixedHeaderContent={() => (
                                                <SUITable.Row
                                                    style={{
                                                        z_index: 999,
                                                        textAlign: 'center',
                                                    }}
                                                >
                                                    <SUITable.HeaderCell
                                                        style={{ width: 50 }}
                                                    />
                                                    <SUITable.HeaderCell>
                                                        JOB ID
                                                    </SUITable.HeaderCell>
                                                    <SUITable.HeaderCell>
                                                        NAME
                                                    </SUITable.HeaderCell>
                                                    <SUITable.HeaderCell>
                                                        START
                                                    </SUITable.HeaderCell>
                                                    <SUITable.HeaderCell>
                                                        DURATION
                                                    </SUITable.HeaderCell>
                                                    <SUITable.HeaderCell>
                                                        COST
                                                    </SUITable.HeaderCell>
                                                </SUITable.Row>
                                            )}
                                            components={TableComponents}
                                        />
                                    </SUITable.Cell>
                                </SUITable.Row>
                            </>
                        ))}

                        {data.wdl_tasks.map((brec, bidx) => (
                            <>
                                <SUITable.Row key={brec.wdl_task_name}>
                                    <SUITable.Cell style={{ width: 50 }}>
                                        <Checkbox
                                            checked={openRows.includes(brec.wdl_task_name)}
                                            toggle
                                            onChange={() => handleToggle(brec.wdl_task_name)}
                                        />
                                    </SUITable.Cell>

                                    <SUITable.Cell colspan="2">
                                        WDL TASK NAME: {brec.wdl_task_name}
                                    </SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.wdl_task_name)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${brec.wdl_task_name}-detail-2`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>Start</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>{brec.usage_start_time}</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.wdl_task_name)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${brec.wdl_task_name}-detail-3`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>End</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>{brec.usage_end_time}</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.wdl_task_name)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${brec.wdl_task_name}-detail-1`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>Total Cost</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>
                                        {formatMoney(brec.cost, 4)}{' '}
                                        {brec.jobs_cnt !== null
                                            ? `(across ${brec.jobs_cnt} jobs)`
                                            : ''}
                                    </SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.wdl_task_name)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`ByLabels${brec.wdl_task_name}`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 50 }}>
                                        <Checkbox
                                            checked={openRows.includes(
                                                `ByLabels${brec.wdl_task_name}`
                                            )}
                                            toggle
                                            onChange={() =>
                                                handleToggle(`ByLabels${brec.wdl_task_name}`)
                                            }
                                        />
                                    </SUITable.Cell>
                                    <SUITable.Cell>Cost By SKU</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(
                                            `ByLabels${brec.wdl_task_name}`
                                        )
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`ByLabels${brec.wdl_task_name}-details`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell />
                                    <SUITable.Cell>
                                        <DonutChart
                                            data={brec.skus.map((srec) => ({
                                                label: srec.sku,
                                                value: srec.cost,
                                            }))}
                                            maxSlices={brec.skus.length}
                                            legendSize={0.6}
                                        />

                                        <SUITable celled compact>
                                            <SUITable.Header>
                                                <SUITable.Row>
                                                    <SUITable.HeaderCell>
                                                        SKU
                                                    </SUITable.HeaderCell>
                                                    <SUITable.HeaderCell>
                                                        COST
                                                    </SUITable.HeaderCell>
                                                </SUITable.Row>
                                            </SUITable.Header>
                                            <SUITable.Body>
                                                {brec.skus.map((srec, sidx) => (
                                                    <SUITable.Row>
                                                        <SUITable.Cell>
                                                            {srec.sku}
                                                        </SUITable.Cell>
                                                        <SUITable.Cell>
                                                            {formatMoney(srec.cost, 4)}
                                                        </SUITable.Cell>
                                                    </SUITable.Row>
                                                ))}
                                            </SUITable.Body>
                                        </SUITable>
                                    </SUITable.Cell>
                                </SUITable.Row>
                            </>
                        ))}

                        {data.cromwell_sub_workflows.map((brec, bidx) => (
                            <>
                                <SUITable.Row key={brec.cromwell_sub_workflow_name}>
                                    <SUITable.Cell style={{ width: 50 }}>
                                        <Checkbox
                                            checked={openRows.includes(
                                                brec.cromwell_sub_workflow_name
                                            )}
                                            toggle
                                            onChange={() =>
                                                handleToggle(brec.cromwell_sub_workflow_name)
                                            }
                                        />
                                    </SUITable.Cell>

                                    <SUITable.Cell colspan="2">
                                        CROMWELL SUB WORKFLOW NAME:{' '}
                                        {brec.cromwell_sub_workflow_name}
                                    </SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(
                                            brec.cromwell_sub_workflow_name
                                        )
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${brec.cromwell_sub_workflow_name}-detail-2`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>Start</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>{brec.usage_start_time}</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(
                                            brec.cromwell_sub_workflow_name
                                        )
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${brec.cromwell_sub_workflow_name}-detail-3`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>End</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>{brec.usage_end_time}</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(
                                            brec.cromwell_sub_workflow_name
                                        )
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${brec.cromwell_sub_workflow_name}-detail-1`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>Total Cost</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>
                                        {formatMoney(brec.cost, 4)}{' '}
                                        {brec.jobs_cnt !== null
                                            ? `(across ${brec.jobs_cnt} jobs)`
                                            : ''}
                                    </SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(
                                            brec.cromwell_sub_workflow_name
                                        )
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`ByLabels${brec.cromwell_sub_workflow_name}`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 50 }}>
                                        <Checkbox
                                            checked={openRows.includes(
                                                `ByLabels${brec.cromwell_sub_workflow_name}`
                                            )}
                                            toggle
                                            onChange={() =>
                                                handleToggle(
                                                    `ByLabels${brec.cromwell_sub_workflow_name}`
                                                )
                                            }
                                        />
                                    </SUITable.Cell>
                                    <SUITable.Cell>Cost By SKU</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(
                                            `ByLabels${brec.cromwell_sub_workflow_name}`
                                        )
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`ByLabels${brec.cromwell_sub_workflow_name}-details`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell />
                                    <SUITable.Cell>
                                        <DonutChart
                                            data={brec.skus.map((srec) => ({
                                                label: srec.sku,
                                                value: srec.cost,
                                            }))}
                                            maxSlices={brec.skus.length}
                                            legendSize={0.6}
                                        />

                                        <SUITable celled compact>
                                            <SUITable.Header>
                                                <SUITable.Row>
                                                    <SUITable.HeaderCell>
                                                        SKU
                                                    </SUITable.HeaderCell>
                                                    <SUITable.HeaderCell>
                                                        COST
                                                    </SUITable.HeaderCell>
                                                </SUITable.Row>
                                            </SUITable.Header>
                                            <SUITable.Body>
                                                {brec.skus.map((srec, sidx) => (
                                                    <SUITable.Row>
                                                        <SUITable.Cell>
                                                            {srec.sku}
                                                        </SUITable.Cell>
                                                        <SUITable.Cell>
                                                            {formatMoney(srec.cost, 4)}
                                                        </SUITable.Cell>
                                                    </SUITable.Row>
                                                ))}
                                            </SUITable.Body>
                                        </SUITable>
                                    </SUITable.Cell>
                                </SUITable.Row>
                            </>
                        ))}

                        {data.cromwell_workflows.map((brec, bidx) => (
                            <>
                                <SUITable.Row key={brec.cromwell_workflow_id}>
                                    <SUITable.Cell style={{ width: 50 }}>
                                        <Checkbox
                                            checked={openRows.includes(
                                                brec.cromwell_workflow_id
                                            )}
                                            toggle
                                            onChange={() =>
                                                handleToggle(brec.cromwell_workflow_id)
                                            }
                                        />
                                    </SUITable.Cell>

                                    <SUITable.Cell colspan="2">
                                        CROMWELL WORKFLOW ID: {brec.cromwell_workflow_id}
                                    </SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.cromwell_workflow_id)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${brec.cromwell_workflow_id}-detail-2`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>Start</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>{brec.usage_start_time}</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.cromwell_workflow_id)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${brec.cromwell_workflow_id}-detail-3`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>End</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>{brec.usage_end_time}</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.cromwell_workflow_id)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`${brec.cromwell_workflow_id}-detail-1`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 250 }}>
                                        <b>Total Cost</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell>
                                        {formatMoney(brec.cost, 4)}{' '}
                                        {brec.jobs_cnt !== null
                                            ? `(across ${brec.jobs_cnt} jobs)`
                                            : ''}
                                    </SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(brec.cromwell_workflow_id)
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`ByLabels${brec.cromwell_workflow_id}`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell style={{ width: 50 }}>
                                        <Checkbox
                                            checked={openRows.includes(
                                                `ByLabels${brec.cromwell_workflow_id}`
                                            )}
                                            toggle
                                            onChange={() =>
                                                handleToggle(
                                                    `ByLabels${brec.cromwell_workflow_id}`
                                                )
                                            }
                                        />
                                    </SUITable.Cell>
                                    <SUITable.Cell>Cost By SKU</SUITable.Cell>
                                </SUITable.Row>

                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(
                                            `ByLabels${brec.cromwell_workflow_id}`
                                        )
                                            ? 'table-row'
                                            : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`ByLabels${brec.cromwell_workflow_id}-details`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell />
                                    <SUITable.Cell>
                                        <DonutChart
                                            data={brec.skus.map((srec) => ({
                                                label: srec.sku,
                                                value: srec.cost,
                                            }))}
                                            maxSlices={brec.skus.length}
                                            legendSize={0.6}
                                        />

                                        <SUITable celled compact>
                                            <SUITable.Header>
                                                <SUITable.Row>
                                                    <SUITable.HeaderCell>
                                                        SKU
                                                    </SUITable.HeaderCell>
                                                    <SUITable.HeaderCell>
                                                        COST
                                                    </SUITable.HeaderCell>
                                                </SUITable.Row>
                                            </SUITable.Header>
                                            <SUITable.Body>
                                                {brec.skus.map((srec, sidx) => (
                                                    <SUITable.Row>
                                                        <SUITable.Cell>
                                                            {srec.sku}
                                                        </SUITable.Cell>
                                                        <SUITable.Cell>
                                                            {formatMoney(srec.cost, 4)}
                                                        </SUITable.Cell>
                                                    </SUITable.Row>
                                                ))}
                                            </SUITable.Body>
                                        </SUITable>
                                    </SUITable.Cell>
                                </SUITable.Row>
                            </>
                        ))}
                    </>
                </SUITable.Body>
            </SUITable>
        </>
    )
}

export default HailBatchGridNew
