import * as React from 'react'
import { Table as SUITable, Card, Checkbox } from 'semantic-ui-react'
import _ from 'lodash'
import { DonutChart } from '../../../shared/components/Graphs/DonutChart'
import '../../project/AnalysisRunnerView/AnalysisGrid.css'
import { TableVirtuoso } from 'react-virtuoso'

import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableContainer from '@mui/material/TableContainer'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Paper from '@mui/material/Paper'
import formatMoney from '../../../shared/utilities/formatMoney'

const hailBatchUrl = 'https://batch.hail.populationgenomics.org.au/batches'

const BatchGrid: React.FunctionComponent<{
    data: any
}> = ({ data }) => {
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

    const displayCheckBoxRow = (
        parentToggle: string,
        key: string,
        toggle: string,
        text: string
    ) => (
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

    const displayTopLevelCheckBoxRow = (key: string, text: string) => (
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

    const displayRow = (toggle: string, key: string, label: string, text: string) => (
        <SUITable.Row
            style={{
                display: toggle ? (openRows.includes(toggle) ? 'table-row' : 'none') : 'table-row',
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

    const displayCostBySkuRow = (
        parentToggles: list,
        toggle: string,
        chartId: string,
        chartMaxWidth: number,
        colSpan: number,
        data: any
    ) => (
        <>
            <SUITable.Row
                style={{
                    display:
                        parentToggles.every((p) => openRows.includes(p)) &&
                        openRows.includes(toggle)
                            ? 'table-row'
                            : 'none',
                    backgroundColor: 'var(--color-bg)',
                }}
                key={toggle}
            >
                <SUITable.Cell style={{ border: 'none' }} />
                <SUITable.Cell />
                <SUITable.Cell style={{ textAlign: 'center' }} colSpan={`${colSpan}`}>
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
                                <SUITable.Row
                                    key={`${toggle}-sku-${sidx}`}
                                    id={`${chartId}-lgd${sidx}`}
                                >
                                    <SUITable.Cell>{srec.sku}</SUITable.Cell>
                                    <SUITable.Cell>{formatMoney(srec.cost, 4)}</SUITable.Cell>
                                </SUITable.Row>
                            ))}
                        </SUITable.Body>
                    </SUITable>
                </SUITable.Cell>
            </SUITable.Row>
        </>
    )

    const displayCostBySeqGrpRow = (
        parentToggle: string,
        key: string,
        toggle: string,
        textCheckbox: string,
        data: any
    ) => (
        <>
            {displayCheckBoxRow(parentToggle, key, toggle, textCheckbox)}
            <SUITable.Row
                style={{
                    display:
                        openRows.includes(parentToggle) && openRows.includes(toggle)
                            ? 'table-row'
                            : 'none',
                    backgroundColor: 'var(--color-bg)',
                }}
                key={toggle}
            >
                <SUITable.Cell style={{ border: 'none' }} />
                <SUITable.Cell style={{ width: 250 }}></SUITable.Cell>
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
                                        <SUITable.Cell>{formatMoney(gcat.cost, 4)}</SUITable.Cell>
                                    </SUITable.Row>
                                ))}
                        </SUITable.Body>
                    </SUITable>
                </SUITable.Cell>
            </SUITable.Row>
        </>
    )

    const displayCommonSection = (key: string, header: string, data: any) => (
        <>
            {displayTopLevelCheckBoxRow(`row-${key}`, `${header}`)}

            {displayRow(
                '',
                `${key}-detail-cost`,
                'Cost',
                `${formatMoney(data.cost, 4)} ${
                    data.jobs_cnt > 0 ? ` (across ${data.jobs_cnt} jobs)` : ''
                }`
            )}

            {displayRow(`row-${key}`, `${key}-detail-start`, 'Start', data.usage_start_time)}
            {displayRow(`row-${key}`, `${key}-detail-end`, 'End', data.usage_end_time)}

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
                        backgroundColor: prepareBgColor(item),
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
                {displayCostBySkuRow(
                    [`row-${item.batch_id}`, `jobs-${item.batch_id}`],
                    `${item.batch_id}-${item.job_id}`,
                    undefined,
                    undefined,
                    4,
                    item
                )}
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

    const displayJobsTable = (item) => (
        <TableVirtuoso
            style={{ height: item.jobs.length > 1 ? 800 : 400, backgroundColor: 'var(--color-bg)' }}
            className="ui celled table compact"
            useWindowScroll={false}
            data={item.jobs.sort((a, b) => {
                // Sorts an array of objects first by 'job_id' in ascending order.
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
                    }}
                >
                    <SUITable.HeaderCell style={{ width: 50 }} />
                    <SUITable.HeaderCell>JOB ID</SUITable.HeaderCell>
                    <SUITable.HeaderCell>NAME</SUITable.HeaderCell>
                    <SUITable.HeaderCell>START</SUITable.HeaderCell>
                    <SUITable.HeaderCell>DURATION</SUITable.HeaderCell>
                    <SUITable.HeaderCell>COST</SUITable.HeaderCell>
                </SUITable.Row>
            )}
            components={TableComponents}
        />
    )

    const arGuidCard = (idx, data) => (
        <Card fluid style={{ padding: '20px' }}>
            <SUITable celled compact>
                <SUITable.Body>
                    <>
                        {displayTopLevelCheckBoxRow(`row-${idx}`, `AR-GUID: ${data.total.ar_guid}`)}

                        {displayRow(
                            '',
                            `${idx}-detail-cost`,
                            'Total cost',
                            formatMoney(data.total.cost, 2)
                        )}

                        {/* cost by categories */}
                        {data.categories.map((tcat, cidx) => {
                            const workflows =
                                tcat.workflows !== null
                                    ? ` (across ${tcat.workflows} workflows)`
                                    : ''
                            return displayRow(
                                '',
                                `categories-${idx}-${cidx}`,
                                tcat.category,
                                `${formatMoney(tcat.cost, 2)} ${workflows}`
                            )
                        })}

                        {displayRow(
                            '',
                            `${idx}-detail-start`,
                            'Start',
                            data.total.usage_start_time
                        )}
                        {displayRow('', `${idx}-detail-end`, 'End', data.total.usage_end_time)}

                        {/* all meta if present */}
                        {data.analysisRunnerLog &&
                            Object.keys(data.analysisRunnerLog.meta).map((key) => {
                                const mcat = data.analysisRunnerLog.meta[key]
                                return displayRow(`row-${idx}`, `${idx}-meta-${key}`, key, mcat)
                            })}

                        {/* cost by topics */}
                        {displayCheckBoxRow(
                            `row-${idx}`,
                            `topics-toggle-${idx}`,
                            `topics-${idx}`,
                            'Cost By Topic'
                        )}
                        <SUITable.Row
                            style={{
                                display:
                                    openRows.includes(`row-${idx}`) &&
                                    openRows.includes(`topics-${idx}`)
                                        ? 'table-row'
                                        : 'none',
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
                                            <SUITable.Row key={`row-${idx}-topic-${tidx}`}>
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
                        {displayCostBySeqGrpRow(
                            `row-${idx}`,
                            `seq-grp-toggle-${idx}`,
                            `seq-grp-${idx}`,
                            'Cost By Sequencing Group',
                            data
                        )}

                        {/* cost by SKU */}
                        {displayCheckBoxRow(
                            `row-${idx}`,
                            `sku-toggle-${idx}`,
                            `sku-${idx}`,
                            'Cost By SKU'
                        )}
                        {displayCostBySkuRow(
                            [`row-${idx}`],
                            `sku-${idx}`,
                            'total-donut-chart',
                            600,
                            1,
                            data
                        )}
                    </>
                </SUITable.Body>
            </SUITable>
        </Card>
    )

    const batchCard = (item) => (
        <Card fluid style={{ padding: '20px' }}>
            <SUITable celled compact>
                <SUITable.Body>
                    {displayTopLevelCheckBoxRow(
                        `row-${item.batch_id}`,
                        prepareBatchUrl(item.batch_id)
                    )}

                    {displayRow('', `${item.batch_id}-detail-name`, 'Batch Name', item.batch_name)}

                    {item.jobs_cnt === 1
                        ? displayRow(
                              '',
                              `${item.batch_id}-detail-job-name`,
                              'Job Name',
                              item.jobs[0].job_name
                          )
                        : null}

                    {displayRow(
                        '',
                        `${item.batch_id}-detail-cost`,
                        'Cost',
                        `${formatMoney(item.cost, 4)} ${
                            item.jobs_cnt !== null ? ` (across ${item.jobs_cnt} jobs)` : ''
                        }`
                    )}

                    {displayRow(
                        `row-${item.batch_id}`,
                        `${item.batch_id}-detail-start`,
                        'Start',
                        data.total.usage_start_time
                    )}
                    {displayRow(
                        `row-${item.batch_id}`,
                        `${item.batch_id}-detail-end`,
                        'End',
                        data.total.usage_end_time
                    )}

                    {/* cost by seq groups */}
                    {displayCostBySeqGrpRow(
                        `row-${item.batch_id}`,
                        `seq-grp-toggle-${item.batch_id}`,
                        `seq-grp-${item.batch_id}`,
                        'Cost By Sequencing Group',
                        item
                    )}

                    {/* cost by SKU */}
                    {displayCheckBoxRow(
                        `row-${item.batch_id}`,
                        `sku-toggle-${item.batch_id}`,
                        `sku-${item.batch_id}`,
                        'Cost By SKU'
                    )}
                    {displayCostBySkuRow(
                        [`row-${item.batch_id}`],
                        `sku-${item.batch_id}`,
                        `donut-chart-${item.batch_id}`,
                        600,
                        1,
                        item
                    )}

                    {/* cost by jobs */}
                    {item.jobs_cnt > 1 && (
                        <>
                            {displayCheckBoxRow(
                                `row-${item.batch_id}`,
                                `jobs-toggle-${item.batch_id}`,
                                `jobs-${item.batch_id}`,
                                'Cost By JOBS'
                            )}
                            <SUITable.Row
                                style={{
                                    display:
                                        openRows.includes(`row-${item.batch_id}`) &&
                                        openRows.includes(`jobs-${item.batch_id}`)
                                            ? 'table-row'
                                            : 'none',
                                    backgroundColor: 'var(--color-bg)',
                                }}
                                key={`jobs-${item.batch_id}`}
                            >
                                <SUITable.Cell style={{ border: 'none' }} />
                                <SUITable.Cell />
                                <SUITable.Cell>{displayJobsTable(item)}</SUITable.Cell>
                            </SUITable.Row>
                        </>
                    )}
                </SUITable.Body>
            </SUITable>
        </Card>
    )

    const genericCard = (item, data, label) => (
        <Card fluid style={{ padding: '20px' }}>
            <SUITable celled compact>
                <SUITable.Body>{displayCommonSection(data, label, item)}</SUITable.Body>
            </SUITable>
        </Card>
    )

    return (
        <>
            {arGuidCard(idx, data)}

            {data.batches.map((item) => batchCard(item))}

            {data.dataproc.map((item) => genericCard(item, item.dataproc, `DATAPROC`))}

            {data.wdl_tasks.map((item) =>
                genericCard(item, item.wdl_task_name, `WDL TASK NAME: ${item.wdl_task_name}`)
            )}

            {data.cromwell_sub_workflows.map((item) =>
                genericCard(
                    item,
                    item.cromwell_sub_workflow_name,
                    `CROMWELL SUB WORKFLOW NAME: ${item.cromwell_sub_workflow_name}`
                )
            )}

            {data.cromwell_workflows.map((item) =>
                genericCard(
                    item,
                    item.cromwell_workflow_id,
                    `CROMWELL WORKFLOW ID: ${item.cromwell_workflow_id}`
                )
            )}
        </>
    )
}

export default BatchGrid
