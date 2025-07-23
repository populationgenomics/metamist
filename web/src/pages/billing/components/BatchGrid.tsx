import orderBy from 'lodash/orderBy'
import startCase from 'lodash/startCase'
import * as React from 'react'
import { Card, Dropdown } from 'semantic-ui-react'
import '../../project/AnalysisRunnerView/AnalysisGrid.css'

import { Table as SUITable } from 'semantic-ui-react'

import Table, { CheckboxRow, DisplayRow } from '../../../shared/components/Table'
import { exportTable } from '../../../shared/utilities/exportTable'

import { useQuery } from '@apollo/client'
import { gql } from '../../../__generated__'
import MuckTheDuck from '../../../shared/components/MuckTheDuck'
import formatMoney from '../../../shared/utilities/formatMoney'
import { getHailBatchUrl } from '../../../shared/utilities/hailBatch'
import { AnalysisCostRecord, AnalysisCostRecordBatch } from '../../../sm-api'
import { BatchJobsTable } from './BatchJobGrid'
import { CostBySkuRow, SeqGrpDisplay } from './BillingByAnalysisComponents'

interface IGenericCardData {
    cost: number
    jobs_cnt?: number
    usage_start_time: string
    usage_end_time: string
    skus: {
        sku: string
        cost: number
    }[]
}

const BatchUrlLink: React.FC<{ batch_id: string }> = ({ batch_id }) => (
    <a href={getHailBatchUrl(batch_id)} rel="noopener noreferrer" target="_blank">
        BATCH ID: {batch_id}
    </a>
)

// Export functions
const exportAnalysisRunnerData = (data: AnalysisCostRecord, format: 'csv' | 'tsv') => {
    const matrix: string[][] = []

    // Summary section
    matrix.push(['ANALYSIS RUNNER SUMMARY', '', '', ''])
    matrix.push(['AR-GUID', data.total?.ar_guid || '', '', ''])
    matrix.push(['Total Cost', (data.total?.cost ?? 0).toFixed(2), '', ''])
    matrix.push(['Start Time', data.total?.usage_start_time || '', '', ''])
    matrix.push(['End Time', data.total?.usage_end_time || '', '', ''])
    matrix.push(['', '', '', '']) // Empty row

    // Cost by Categories
    if (data.categories && data.categories.length > 0) {
        matrix.push(['COST BY CATEGORIES', '', '', ''])
        matrix.push(['Category', 'Cost', 'Workflows', ''])
        data.categories.forEach((cat) => {
            matrix.push([cat.category, cat.cost.toFixed(2), cat.workflows?.toString() || '', ''])
        })
        matrix.push(['', '', '', '']) // Empty row
    }

    // Cost by Topics
    if (data.topics && data.topics.length > 0) {
        matrix.push(['COST BY TOPICS', '', '', ''])
        matrix.push(['Topic', 'Cost', '', ''])
        data.topics.forEach((topic) => {
            matrix.push([topic.topic, topic.cost.toFixed(2), '', ''])
        })
        matrix.push(['', '', '', '']) // Empty row
    }

    // Cost by Sequencing Groups
    if (data.seq_groups && data.seq_groups.length > 0) {
        matrix.push(['COST BY SEQUENCING GROUPS', '', '', ''])
        matrix.push(['Stage', 'Sequencing Group', 'Cost', ''])
        data.seq_groups.forEach((sg) => {
            matrix.push([sg.stage || '', sg.sequencing_group || '', sg.cost.toFixed(2), ''])
        })
        matrix.push(['', '', '', '']) // Empty row
    }

    // Cost by SKUs
    if (data.skus && data.skus.length > 0) {
        matrix.push(['COST BY SKUS', '', '', ''])
        matrix.push(['SKU', 'Cost', '', ''])
        data.skus.forEach((sku) => {
            matrix.push([sku.sku, sku.cost.toFixed(2), '', ''])
        })
    }

    const arGuid = data.total?.ar_guid || 'unknown'
    exportTable({ headerFields: [], matrix }, format, `analysis_runner_${arGuid}`)
}

const exportBatchData = (batch: AnalysisCostRecordBatch, format: 'csv' | 'tsv') => {
    const matrix: string[][] = []

    // Batch Summary
    matrix.push(['BATCH SUMMARY', '', '', ''])
    matrix.push(['Batch ID', batch.batch_id, '', ''])
    matrix.push(['Batch Name', batch.batch_name || '', '', ''])
    matrix.push(['Total Cost', batch.cost.toFixed(2), '', ''])
    matrix.push(['Jobs Count', batch.jobs_cnt?.toString() || '0', '', ''])
    matrix.push(['Start Time', batch.usage_start_time || '', '', ''])
    matrix.push(['End Time', batch.usage_end_time || '', '', ''])
    matrix.push(['Driver Batch', batch.jobs[0]?.job_name === 'driver' ? 'True' : 'False', '', ''])
    matrix.push(['', '', '', '']) // Empty row

    // Cost by Sequencing Groups
    if (batch.seq_groups && batch.seq_groups.length > 0) {
        matrix.push(['COST BY SEQUENCING GROUPS', '', '', ''])
        matrix.push(['Stage', 'Sequencing Group', 'Cost', ''])
        batch.seq_groups.forEach((sg) => {
            matrix.push([sg.stage || '', sg.sequencing_group || '', sg.cost.toFixed(2), ''])
        })
        matrix.push(['', '', '', '']) // Empty row
    }

    // Cost by SKUs
    if (batch.skus && batch.skus.length > 0) {
        matrix.push(['COST BY SKUS', '', '', ''])
        matrix.push(['SKU', 'Cost', '', ''])
        batch.skus.forEach((sku) => {
            matrix.push([sku.sku, sku.cost.toFixed(2), '', ''])
        })
        matrix.push(['', '', '', '']) // Empty row
    }

    // Cost by Jobs
    if (batch.jobs && batch.jobs.length > 0) {
        matrix.push(['COST BY JOBS', '', '', ''])
        matrix.push(['Job Name', 'Cost', '', ''])
        batch.jobs.forEach((job) => {
            matrix.push([job.job_name || '', job.cost.toFixed(2), '', ''])
        })
    }

    exportTable({ headerFields: [], matrix }, format, `batch_${batch.batch_id}`)
}

const exportGenericData = (
    data: IGenericCardData,
    label: string,
    identifier: string,
    format: 'csv' | 'tsv'
) => {
    const matrix: string[][] = []

    // Summary section
    matrix.push([`${label.toUpperCase()} SUMMARY`, '', ''])
    matrix.push(['Identifier', identifier, ''])
    matrix.push(['Total Cost', data.cost.toFixed(2), ''])
    matrix.push(['Jobs Count', data.jobs_cnt?.toString() || '0', ''])
    matrix.push(['Start Time', data.usage_start_time || '', ''])
    matrix.push(['End Time', data.usage_end_time || '', ''])
    matrix.push(['', '', '']) // Empty row

    // Cost by SKUs
    if (data.skus && data.skus.length > 0) {
        matrix.push(['COST BY SKUS', '', ''])
        matrix.push(['SKU', 'Cost', ''])
        data.skus.forEach((sku) => {
            matrix.push([sku.sku, sku.cost.toFixed(2), ''])
        })
    }

    const filename = `${label.toLowerCase().replace(/\s+/g, '_')}_${identifier.replace(/[^a-zA-Z0-9]/g, '_')}`
    exportTable({ headerFields: [], matrix }, format, filename)
}

// Export Button Component
const ExportButton: React.FC<{ onExport: (format: 'csv' | 'tsv') => void }> = ({ onExport }) => (
    <Dropdown
        button
        compact
        floating
        icon="download"
        className="icon"
        style={{ marginLeft: '10px', height: '28px', minWidth: '28px' }}
    >
        <Dropdown.Menu>
            <Dropdown.Item
                key="csv"
                text="Export to CSV"
                icon="file excel"
                onClick={() => onExport('csv')}
            />
            <Dropdown.Item
                key="tsv"
                text="Export to TSV"
                icon="file text outline"
                onClick={() => onExport('tsv')}
            />
        </Dropdown.Menu>
    </Dropdown>
)

const GET_AR_RECORDS = gql(`
    query BillingByAnalysisRunnerLog($arGuid: String!) {
        analysisRunner(arGuid: $arGuid) {
            arGuid
            timestamp
            accessLevel
            repository
            commit
            script
            description
            driverImage
            configPath
            cwd
            environment
            hailVersion
            batchUrl
            submittingUser
            meta
            outputPath
        }
    }
`)

const AnalysisRunnerRecordCard: React.FC<{ data: AnalysisCostRecord }> = ({ data, ...props }) => {
    const [isOpen, setIsOpen] = React.useState(false)
    const [isTopicsOpen, setIsTopicsOpen] = React.useState(false)
    const [isSeqGroupOpen, setIsSeqGroupOpen] = React.useState(false)
    const [isSkuOpen, setIsSkuOpen] = React.useState(false)

    const arGuid = data?.total?.ar_guid
    const queryResponse = useQuery(GET_AR_RECORDS, {
        skip: !arGuid,
        variables: { arGuid: arGuid! },
    })

    const arRecord = queryResponse?.data?.analysisRunner

    const handleExport = (format: 'csv' | 'tsv') => {
        exportAnalysisRunnerData(data, format)
    }

    return (
        <Card fluid style={{ padding: '20px' }} {...props}>
            <Table celled compact>
                <SUITable.Body>
                    <>
                        <CheckboxRow
                            isChecked={isOpen}
                            setIsChecked={setIsOpen}
                            colSpan={2}
                            rowStyle={{ backgroundColor: 'var(--color-bg-card)' }}
                        >
                            <div
                                style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'space-between',
                                    width: '100%',
                                }}
                            >
                                <span>AR-GUID: {arGuid}</span>
                                <ExportButton onExport={handleExport} />
                            </div>
                        </CheckboxRow>

                        <DisplayRow label="Total cost">
                            {formatMoney(data.total?.cost ?? 0, 2)}
                        </DisplayRow>

                        {/* cost by categories */}
                        {data?.categories?.map((tcat) => {
                            const workflows =
                                tcat.workflows !== null
                                    ? ` (across ${tcat.workflows} workflows)`
                                    : ''
                            return (
                                <DisplayRow
                                    label={startCase(tcat.category)}
                                    key={`ar-guid-${arGuid}-category-${tcat.category}`}
                                >
                                    {formatMoney(tcat.cost, 2)} {workflows}
                                </DisplayRow>
                            )
                        })}

                        <DisplayRow label="Start">{data?.total?.usage_start_time}</DisplayRow>
                        <DisplayRow label="End">{data?.total?.usage_end_time}</DisplayRow>

                        {/* cost by topics */}

                        {/* all meta if present */}
                        {queryResponse.loading && (
                            <DisplayRow label="Loading...">
                                <MuckTheDuck />
                            </DisplayRow>
                        )}
                        {queryResponse.error && (
                            <DisplayRow label="AR fetch error">
                                {queryResponse.error.message}
                            </DisplayRow>
                        )}
                        {!!arRecord && (
                            <>
                                {/* submitingUser, repository, commit, script, description, outputPath, configPath, cwd, hailVersion */}

                                <DisplayRow label="Submitting User" isVisible={isOpen}>
                                    {arRecord.submittingUser}
                                </DisplayRow>
                                <DisplayRow label="Access level" isVisible={isOpen}>
                                    {arRecord.accessLevel}
                                </DisplayRow>
                                <DisplayRow label="Repository" isVisible={isOpen}>
                                    {arRecord.repository}
                                </DisplayRow>
                                <DisplayRow label="Commit" isVisible={isOpen}>
                                    {arRecord.commit}
                                </DisplayRow>
                                <DisplayRow label="Script" isVisible={isOpen}>
                                    {arRecord.script}
                                </DisplayRow>
                                <DisplayRow label="Description" isVisible={isOpen}>
                                    {arRecord.description}
                                </DisplayRow>
                                <DisplayRow label="Output path" isVisible={isOpen}>
                                    {arRecord.outputPath}
                                </DisplayRow>
                                <DisplayRow label="Config Path" isVisible={isOpen}>
                                    {arRecord.configPath}
                                </DisplayRow>
                                <DisplayRow label="CWD" isVisible={isOpen}>
                                    {arRecord.cwd}
                                </DisplayRow>
                                {/* hail version */}
                                <DisplayRow label="Hail Version" isVisible={isOpen}>
                                    {arRecord.hailVersion}
                                </DisplayRow>
                            </>
                        )}

                        {/* cost by topics */}

                        <CheckboxRow
                            isChecked={isTopicsOpen}
                            setIsChecked={setIsTopicsOpen}
                            isVisible={isOpen}
                            leadingCells={1}
                        >
                            Cost by Topics - {data.topics?.length || 0} topic(s)
                        </CheckboxRow>
                        <DisplayRow label="" isVisible={isOpen && isTopicsOpen}>
                            <Table celled compact>
                                <SUITable.Header>
                                    <SUITable.Row>
                                        <SUITable.HeaderCell>Topic</SUITable.HeaderCell>
                                        <SUITable.HeaderCell>Cost</SUITable.HeaderCell>
                                    </SUITable.Row>
                                </SUITable.Header>
                                <SUITable.Body>
                                    {data.topics?.map((trec, tidx) => (
                                        <SUITable.Row key={`row-topic-${tidx}`}>
                                            <SUITable.Cell>{trec.topic}</SUITable.Cell>
                                            <SUITable.Cell>
                                                {formatMoney(trec.cost, 2)}
                                            </SUITable.Cell>
                                        </SUITable.Row>
                                    ))}
                                </SUITable.Body>
                            </Table>
                        </DisplayRow>

                        {/* cost by seq groups */}

                        <CheckboxRow
                            isChecked={isSeqGroupOpen}
                            setIsChecked={setIsSeqGroupOpen}
                            isVisible={isOpen}
                            leadingCells={1}
                        >
                            Cost by Sequencing Groups -{' '}
                            {data.seq_groups?.filter((s) => !!s.sequencing_group)?.length || 0}{' '}
                            sequencing group(s) across{' '}
                            {new Set(
                                data.seq_groups
                                    ?.filter((s) => s.stage && s.stage.trim() !== '')
                                    .map((s) => s.stage)
                            ).size || 0}{' '}
                            stage(s)
                        </CheckboxRow>
                        <DisplayRow label="" isVisible={isOpen && isSeqGroupOpen}>
                            <SeqGrpDisplay seq_groups={data.seq_groups || []} />
                        </DisplayRow>

                        {/* cost by SKU */}
                        <CheckboxRow
                            isChecked={isSkuOpen}
                            setIsChecked={setIsSkuOpen}
                            isVisible={isOpen}
                            leadingCells={1}
                        >
                            Cost by SKU
                        </CheckboxRow>
                        <DisplayRow label="" isVisible={isOpen && isSkuOpen}>
                            <CostBySkuRow
                                skus={data.skus || []}
                                colSpan={1}
                                chartMaxWidth="600px"
                                chartId="total-donut-chart"
                            />
                        </DisplayRow>
                    </>
                </SUITable.Body>
            </Table>
        </Card>
    )
}

const BatchCard: React.FC<{ item: AnalysisCostRecordBatch }> = ({ item }) => {
    const [isOpen, setIsOpen] = React.useState(false)

    const [isSeqGroupOpen, setIsSeqGroupOpen] = React.useState(false)
    const [isSkuOpen, setIsSkuOpen] = React.useState(false)
    const [isJobsOpen, setIsJobsOpen] = React.useState(false)

    const isDriverBatch = item.jobs[0]?.job_name === 'driver'

    const handleExport = (format: 'csv' | 'tsv') => {
        exportBatchData(item, format)
    }

    return (
        <Card fluid style={{ padding: '20px' }}>
            <Table celled compact>
                <SUITable.Body>
                    <CheckboxRow
                        isChecked={isOpen}
                        setIsChecked={setIsOpen}
                        colSpan={2}
                        rowStyle={{ backgroundColor: 'var(--color-bg-card)' }}
                    >
                        <div
                            style={{
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'space-between',
                                width: '100%',
                            }}
                        >
                            <BatchUrlLink batch_id={item.batch_id} />
                            <ExportButton onExport={handleExport} />
                        </div>
                    </CheckboxRow>
                    {isDriverBatch && <DisplayRow label="Driver Batch">True</DisplayRow>}
                    <DisplayRow label="Batch Name">{item.batch_name}</DisplayRow>
                    {item.jobs?.length === 1 && item.jobs[0].job_name && (
                        <DisplayRow label="Job Name">{item.jobs[0].job_name}</DisplayRow>
                    )}

                    <DisplayRow label="Cost">
                        {formatMoney(item.cost, 4)}{' '}
                        {(item?.jobs_cnt || 0) > 0 && <em>- across {item.jobs_cnt} job(s)</em>}
                    </DisplayRow>

                    <DisplayRow label="Start" isVisible={isOpen}>
                        {item.usage_start_time}
                    </DisplayRow>
                    <DisplayRow label="End" isVisible={isOpen}>
                        {item.usage_end_time}
                    </DisplayRow>

                    {/* cost by seq groups */}
                    <CheckboxRow
                        isChecked={isSeqGroupOpen}
                        setIsChecked={setIsSeqGroupOpen}
                        isVisible={isOpen}
                        leadingCells={1}
                    >
                        Cost by Sequencing Groups -{' '}
                        {item.seq_groups?.filter((s) => !!s.sequencing_group)?.length || 0}{' '}
                        sequencing group(s) across{' '}
                        {new Set(
                            item.seq_groups
                                ?.filter((s) => s.stage && s.stage.trim() !== '')
                                .map((s) => s.stage)
                        ).size || 0}{' '}
                        stage(s)
                    </CheckboxRow>
                    <DisplayRow
                        label="Cost By Sequencing Group"
                        isVisible={isOpen && isSeqGroupOpen}
                    >
                        <SeqGrpDisplay seq_groups={item.seq_groups} />
                    </DisplayRow>

                    {/* cost by SKU */}
                    <CheckboxRow
                        isChecked={isSkuOpen}
                        setIsChecked={setIsSkuOpen}
                        isVisible={isOpen}
                        leadingCells={1}
                    >
                        Cost by SKU
                    </CheckboxRow>
                    <DisplayRow label="Cost By SKU" isVisible={isOpen && isSkuOpen}>
                        <CostBySkuRow
                            chartId={`donut-chart-${item.batch_id}`}
                            skus={item.skus}
                            colSpan={1}
                            chartMaxWidth="600px"
                        />
                    </DisplayRow>

                    {/* cost by jobs */}
                    {item.jobs_cnt > 1 && (
                        <>
                            <CheckboxRow
                                isChecked={isJobsOpen}
                                setIsChecked={setIsJobsOpen}
                                isVisible={isOpen}
                                leadingCells={1}
                            >
                                Cost by Jobs - {item.jobs_cnt} job(s)
                            </CheckboxRow>
                            <DisplayRow label="" isVisible={isOpen && isJobsOpen}>
                                <BatchJobsTable batch={item} />
                                {/* <em> Batch jobs table</em> */}
                            </DisplayRow>
                        </>
                    )}
                </SUITable.Body>
            </Table>
        </Card>
    )
}

const BatchGrid: React.FunctionComponent<{
    data: AnalysisCostRecord
}> = ({ data }) => {
    const GenericCard: React.FC<{
        data: IGenericCardData
        label: string
        pkey: string
    }> = ({ data, label, pkey }) => {
        const [isOpen, setIsOpen] = React.useState(false)
        const [skuIsOpen, setSkuIsOpen] = React.useState(false)

        const handleExport = (format: 'csv' | 'tsv') => {
            exportGenericData(data, label, pkey, format)
        }

        return (
            <Card fluid style={{ padding: '20px' }}>
                <Table celled compact>
                    <SUITable.Body>
                        <CheckboxRow
                            isChecked={isOpen}
                            setIsChecked={setIsOpen}
                            colSpan={2}
                            rowStyle={{ backgroundColor: 'var(--color-bg-card)' }}
                        >
                            <div
                                style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'space-between',
                                    width: '100%',
                                }}
                            >
                                <span>{label}</span>
                                <ExportButton onExport={handleExport} />
                            </div>
                        </CheckboxRow>

                        <DisplayRow label="Cost">
                            {formatMoney(data.cost, 4)}{' '}
                            {(data?.jobs_cnt || 0) > 0 && <em>- across {data.jobs_cnt} job(s)</em>}
                        </DisplayRow>
                        <DisplayRow label="start">{data.usage_start_time}</DisplayRow>
                        <DisplayRow label="End">{data.usage_end_time}</DisplayRow>

                        {/* cost by SKU */}
                        <CheckboxRow
                            isChecked={skuIsOpen}
                            setIsChecked={setSkuIsOpen}
                            isVisible={isOpen}
                            leadingCells={1}
                        >
                            Cost by SKU
                        </CheckboxRow>
                        <DisplayRow isVisible={isOpen && skuIsOpen} label="SKU">
                            <CostBySkuRow
                                chartId={`donut-chart-${pkey}`}
                                skus={data.skus}
                                colSpan={1}
                                chartMaxWidth="600px"
                            />
                        </DisplayRow>
                    </SUITable.Body>
                </Table>
            </Card>
        )
    }

    return (
        <>
            <AnalysisRunnerRecordCard data={data} />

            {orderBy(data?.batches || [], (b) => b.batch_id).map((batchRecord) => (
                <BatchCard item={batchRecord} key={`batch-card-${batchRecord.batch_id}`} />
            ))}

            {data.dataproc?.map((item, idx) => (
                <GenericCard
                    key={`dataproc-${idx}`}
                    data={item as IGenericCardData}
                    label="DATAPROC"
                    pkey={`dataproc-${idx}`}
                />
            ))}

            {data.wdl_tasks?.map((item) => (
                <GenericCard
                    key={`wdl-task-${item.wdl_task_name}`}
                    data={item}
                    label={`WDL TASK NAME: ${item.wdl_task_name}`}
                    pkey={`wdl-task-${item.wdl_task_name}`}
                />
            ))}

            {data.cromwell_sub_workflows?.map((item) => (
                <GenericCard
                    key={`cromwell-sub-workflow-${item.cromwell_sub_workflow_name}`}
                    data={item}
                    label={`CROMWELL SUB WORKFLOW NAME: ${item.cromwell_sub_workflow_name}`}
                    pkey={`cromwell-sub-workflow-${item.cromwell_sub_workflow_name}`}
                />
            ))}

            {data.cromwell_workflows?.map((item) => (
                <GenericCard
                    key={`cromwell-workflow-${item.cromwell_workflow_id}`}
                    data={item}
                    label={`CROMWELL WORKFLOW ID: ${item.cromwell_workflow_id}`}
                    pkey={`cromwell-workflow-${item.cromwell_workflow_id}`}
                />
            ))}
        </>
    )
}

export default BatchGrid
