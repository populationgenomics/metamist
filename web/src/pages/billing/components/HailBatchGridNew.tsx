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

    const [openRows, setOpenRows] = React.useState<string[]>([])

    const handleToggle = (position: string) => {
        if (!openRows.includes(position)) {
            setOpenRows([...openRows, position])
        } else {
            setOpenRows(openRows.filter((value) => value !== position))
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

    const calcDuration = (dataItem) => {
        const duration = new Date(dataItem.usage_end_time) - new Date(dataItem.usage_start_time)
        const seconds = Math.floor((duration / 1000) % 60)
        const minutes = Math.floor((duration / (1000 * 60)) % 60)
        const hours = Math.floor((duration / (1000 * 60 * 60)) % 24)
        const formattedDuration = `${hours}h ${minutes}m ${seconds}s`
        return <span>{formattedDuration}</span>
    }

    const idx = 0

    const displayCheckBoxRow = (parentToggle:string, key:string, toggle:string, text:string) => (
        <SUITable.Row
                style={{
                    display: openRows.includes(parentToggle) ? 'table-row' : 'none',
                    backgroundColor: 'var(--color-bg)',
                }}
                key={key}
            >
                <SUITable.Cell style={{ border: 'none' }} />
                <SUITable.Cell style={{ width: 50 }}>
                    <Checkbox
                        checked={openRows.includes(toggle)}
                        toggle
                        onChange={() => handleToggle(toggle)}
                    />
                </SUITable.Cell>
                <SUITable.Cell>{text}</SUITable.Cell>
            </SUITable.Row>
    )

    const displayTopLevelCheckBoxRow = (key:string, text:string) => (
        <SUITable.Row key={key}>
                <SUITable.Cell style={{ width: 50 }}>
                    <Checkbox
                        checked={openRows.includes(key)}
                        toggle
                        onChange={() => handleToggle(key)}
                    />
                </SUITable.Cell>
                <SUITable.Cell colSpan="2">{text}</SUITable.Cell>
            </SUITable.Row>
    )

    const displayRow = (toggle:string, key:string, label:string, text:string) => (
        <SUITable.Row 
            style={{
                display: openRows.includes(toggle) ? 'table-row' : 'none',
                backgroundColor: 'var(--color-bg)',
            }}
            key={key}
            >
                <SUITable.Cell style={{ border: 'none' }} />
                <SUITable.Cell style={{ width: 250 }}>
                    <b>{label}</b>
                </SUITable.Cell>
                <SUITable.Cell>{text}</SUITable.Cell>
            </SUITable.Row>
    )

    const displayCostBySkuRow = (parentToggles:list, toggle:string, chartId:string, chartMaxWidth:number, colSpan:number, data:any) => (<>
        <SUITable.Row
            style={{
                display: (parentToggles.every((p) => openRows.includes(p)) && openRows.includes(toggle))
                    ? 'table-row'
                    : 'none',
                backgroundColor: 'var(--color-bg)',
            }}
            key={toggle}
        >
            <SUITable.Cell style={{ border: 'none' }} />
            <SUITable.Cell />
            <SUITable.Cell style={{textAlign: 'center'}} colSpan={`${colSpan}`}>
                {chartId && (
                    <DonutChart
                        id={`${chartId}`}
                        data={data.skus.map((srec) => ({
                            label: srec.sku,
                            value: srec.cost,
                        }))}
                        maxSlices={data.skus.length}
                        showLegend={false}
                        isLoading={false}
                        maxWidth={chartMaxWidth}
                    />
                )}
                <SUITable celled compact>
                    <SUITable.Header>
                        <SUITable.Row>
                            <SUITable.HeaderCell>SKU</SUITable.HeaderCell>
                            <SUITable.HeaderCell>COST</SUITable.HeaderCell>
                        </SUITable.Row>
                    </SUITable.Header>
                    <SUITable.Body>
                        {data.skus.map((srec, sidx) => (
                            <SUITable.Row key={`${toggle}-sku-${sidx}`} id={`${chartId}-lgd${sidx}`}>
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
        </>)

    const displayCostBySeqGrpRow = (parentToggle:string, key:string, toggle:string, textCheckbox:string, data:any) => (<>
            {displayCheckBoxRow(parentToggle, key, toggle, textCheckbox)}
            <SUITable.Row
                style={{
                    display: (openRows.includes(parentToggle) && openRows.includes(toggle))
                        ? 'table-row'
                        : 'none',
                    backgroundColor: 'var(--color-bg)',
                }}
                key={toggle}
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
                                <SUITable.Row key={`${toggle}-seq-grp-${gidx}`}>
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
        </>)

    const displayCommonSection = (key:string, header:string, data:any) => (
        <>
            {displayTopLevelCheckBoxRow(`row-${key}`, `${header}`)}

            {displayRow(`row-${key}`, `${key}-detail-2`, 'Start', data.usage_start_time)}
            {displayRow(`row-${key}`, `${key}-detail-3`, 'End', data.usage_end_time)}
            {displayRow(`row-${key}`, `${key}-detail-4`, 'Total Cost', `${formatMoney(data.cost, 4)}`)}

            {displayCheckBoxRow(`row-${key}`, `sku-toggle-${key}`, `sku-${key}`, 'Cost By SKU')}
            {displayCostBySkuRow([`row-${key}`], `sku-${key}`, `donut-chart-${key}`, 600, 1, data)}
        </>
    )

    const ExpandableRow = ({ item, ...props }) => {
        const index = props['data-index']
        return (
            <React.Fragment key={`${item.batch_id}-${item.job_id}`}>
                <TableRow
                    {...props}
                    className={item.job_id === null ? 'bold-text' : ''}
                    style={{
                        backgroundColor: prepareBgColor(item)
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

                {/* cost by SKU */}
                {displayCostBySkuRow([`row-${item.batch_id}`, `jobs-${item.batch_id}`], `${item.batch_id}-${item.job_id}`, undefined, undefined, 4, item)}

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

    const displayJobsTable = (brec) => (
        <TableVirtuoso
            style={{ height: brec.jobs.length > 1 ? 800 : 400, backgroundColor: 'var(--color-bg)'}}
            className="ui celled table compact"
            useWindowScroll={false}
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
                        z_index: 999
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
    )


    return (
        <>
            <SUITable celled compact>
                <SUITable.Body>
                    <>
                        {displayTopLevelCheckBoxRow(`row-${idx}`, `AR-GUID: ${data.total.ar_guid}`)}

                        {displayRow(`row-${idx}`, `${idx}-detail-1`, 'Start', data.total.usage_start_time)}
                        {displayRow(`row-${idx}`, `${idx}-detail-2`, 'End', data.total.usage_end_time)}
                        {displayRow(`row-${idx}`, `${idx}-detail-3`, 'Total Cost', formatMoney(data.total.cost, 2))}

                        {/* all meta */}
                        {Object.keys(data.analysisRunnerLog.meta).map((key) => {
                            const mcat = data.analysisRunnerLog.meta[key];
                            return displayRow(`row-${idx}`, `${idx}-meta-${key}`, key, mcat);
                        })}

                        {/* cost by categories */}
                        {data.categories.map((tcat, cidx) => {
                            const workflows = tcat.workflows !== null ? ` (across ${tcat.workflows} workflows)` : '';
                            return displayRow(`row-${idx}`, `categories-${idx}-${cidx}`, tcat.category, `${formatMoney(tcat.cost, 2)} ${workflows}`)
                        })}

                        {/* cost by topics */}
                        {displayCheckBoxRow(`row-${idx}`, `topics-toggle-${idx}`, `topics-${idx}`, 'Cost By Topic')}
                        <SUITable.Row
                            style={{
                                display: (openRows.includes(`row-${idx}`) && openRows.includes(`topics-${idx}`)) ? 'table-row' : 'none',
                                backgroundColor: 'var(--color-bg)',
                            }}
                            key={`topics-${idx}`}
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

                        {/* cost by seq groups */}
                        {displayCostBySeqGrpRow(`row-${idx}`, `seq-grp-toggle-${idx}`, `seq-grp-${idx}`, 'Cost By Sequencing Group', data)}

                        {/* cost by SKU */}
                        {displayCheckBoxRow(`row-${idx}`, `sku-toggle-${idx}`, `sku-${idx}`, 'Cost By SKU')}
                        {displayCostBySkuRow([`row-${idx}`], `sku-${idx}`, 'total-donut-chart', 600, 1, data)}
                    </>
                </SUITable.Body>
            </SUITable>

            <SUITable celled compact>
                <SUITable.Body>
                    <>
                        {data.batches.map((brec, bidx) => (
                            <>
                                {displayCommonSection(brec.batch_id, `BATCH: ${brec.batch_id}`, brec)}
                                {/* // {displayTopLevelCheckBoxRow(`row-${brec.batch_id}`, `BATCH: ${brec.batch_id}`)}

                                // {displayRow(`row-${brec.batch_id}`, `${brec.batch_id}-detail-1`, 'Batch Name', brec.batch_name)}
                                // {displayRow(`row-${brec.batch_id}`, `${brec.batch_id}-detail-2`, 'Start', brec.usage_start_time)}
                                // {displayRow(`row-${brec.batch_id}`, `${brec.batch_id}-detail-3`, 'End', brec.usage_end_time)}
                                // {displayRow(`row-${brec.batch_id}`, `${brec.batch_id}-detail-4`, 'Total Cost',
                                //     `${formatMoney(brec.cost, 4)} ${brec.jobs_cnt !== null ? ` (across ${brec.jobs_cnt} jobs)` : ''}`)
                                // } */}

                                {/* cost by seq groups */}
                                {displayCostBySeqGrpRow(`row-${brec.batch_id}`, `seq-grp-toggle-${brec.batch_id}`, `seq-grp-${brec.batch_id}`, 'Cost By Sequencing Group', brec)}

                                {/* cost by jobs */}
                                {displayCheckBoxRow(`row-${brec.batch_id}`, `jobs-toggle-${brec.batch_id}`, `jobs-${brec.batch_id}`, 'Cost By JOBS')}
                                <SUITable.Row
                                    style={{
                                        display: (openRows.includes(`row-${brec.batch_id}`) && openRows.includes(`jobs-${brec.batch_id}`))
                                        ? 'table-row'
                                        : 'none',
                                        backgroundColor: 'var(--color-bg)',
                                    }}
                                    key={`jobs-${brec.batch_id}`}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell />
                                    <SUITable.Cell>
                                        {displayJobsTable(brec)}
                                    </SUITable.Cell>
                                </SUITable.Row>
                            </>
                        ))}

                        {data.dataproc.map((dproc) => (
                            <>
                                {displayCommonSection(dproc.dataproc, `DATAPROC`, dproc)}
                            </>
                        ))}


                        {data.wdl_tasks.map((brec) => (
                            <>
                                {displayCommonSection(brec.wdl_task_name, `WDL TASK NAME: ${brec.wdl_task_name}`, brec)}
                            </>
                        ))}

                        {data.cromwell_sub_workflows.map((brec) => (
                            <>
                                {displayCommonSection(brec.cromwell_sub_workflow_name, `CROMWELL SUB WORKFLOW NAME: ${brec.cromwell_sub_workflow_name}`, brec)}
                            </>
                        ))}

                        {data.cromwell_workflows.map((brec) => (
                            <>
                                {displayCommonSection(brec.cromwell_workflow_id, `CROMWELL WORKFLOW ID: ${brec.cromwell_workflow_id}`, brec)}
                            </>
                        ))}
                    </>
                </SUITable.Body>
            </SUITable>
        </>
    )
}

export default HailBatchGridNew
