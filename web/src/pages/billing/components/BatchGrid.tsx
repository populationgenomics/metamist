import * as React from 'react'
import { Card, Checkbox } from 'semantic-ui-react'
import _, { range } from 'lodash'
import { DonutChart } from '../../../shared/components/Graphs/DonutChart'
import '../../project/AnalysisRunnerView/AnalysisGrid.css'

import { Table as SUITable, TableProps } from 'semantic-ui-react'

import Table, { CheckboxRow, DisplayRow } from '../../../shared/components/Table'

import formatMoney from '../../../shared/utilities/formatMoney'
import {
    AnalysisCostRecord,
    AnalysisCostRecordTotal,
    AnalysisCostRecordBatch,
    AnalysisCostRecordBatchJob,
    Analysis,
    AnalysisCostRecordSku,
    AnalysisCostRecordSeqGroup,
} from '../../../sm-api'
import { Check } from '@mui/icons-material'
import { CostBySkuRow, SeqGrpDisplay } from './BillingByAnalysisComponents'
import { BatchJobsTable } from './BatchJobGrid'
import { useQuery } from '@apollo/client'
import { gql } from '../../../__generated__'
import MuckTheDuck from '../../../shared/components/MuckTheDuck'

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

const hailBatchUrl = 'https://batch.hail.populationgenomics.org.au/batches'

const BatchUrlLink: React.FC<{ batch_id: string }> = ({ batch_id }) => (
    <a href={`${hailBatchUrl}/${batch_id}`} rel="noopener noreferrer" target="_blank">
        BATCH ID: {batch_id}
    </a>
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
                            AR-GUID: {arGuid}
                        </CheckboxRow>

                        <DisplayRow label="Total cost">
                            {formatMoney(data.total?.cost, 2)}
                        </DisplayRow>

                        {/* cost by categories */}
                        {data?.categories?.map((tcat, cidx) => {
                            const workflows =
                                tcat.workflows !== null
                                    ? ` (across ${tcat.workflows} workflows)`
                                    : ''
                            return (
                                <DisplayRow
                                    label={_.startCase(tcat.category)}
                                    key={`ar-guid-${arGuid}-category-${tcat}`}
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
                            sequencing groups
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
                                chartMaxWidth="600"
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
    const [jobOpenSet, setJobOpenSet] = React.useState<Set<string>>(new Set())

    const isDriverBatch = item.jobs[0]?.job_name === 'driver'

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
                        <BatchUrlLink batch_id={item.batch_id} />
                    </CheckboxRow>
                    {isDriverBatch && <DisplayRow label="Driver Batch">True</DisplayRow>}
                    <DisplayRow label="Batch Name">{item.batch_name}</DisplayRow>
                    {item.jobs?.length === 1 && item.jobs[0].job_name && (
                        <DisplayRow label="Job Name">{item.jobs[0].job_name}</DisplayRow>
                    )}

                    <DisplayRow label="Cost">
                        {formatMoney(item.cost, 4)}{' '}
                        {item.jobs?.length > 0 && <em>- across {item.jobs.length} job(s)</em>}
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
                        Cost by Sequencing Groups - {item.seq_groups?.length || 0} sequencing groups
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
                        <CostBySkuRow skus={item.skus} colSpan={1} chartMaxWidth="600" />
                    </DisplayRow>
                    {/* {displayCheckBoxRow(
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
                )} */}

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
    const [openRows, setOpenRows] = React.useState<string[]>([])

    const handleToggle = (position: string) => {
        if (!openRows.includes(position)) {
            setOpenRows([...openRows, position])
        } else {
            setOpenRows(openRows.filter((value) => value !== position))
        }
    }

    const prepareBgColor = (log: any) => {
        if (log.batch_id === undefined) {
            return 'var(--color-border-color)'
        }
        if (log.job_id === undefined) {
            return 'var(--color-border-default)'
        }
        return 'var(--color-bg)'
    }

    const GenericCard: React.FC<{
        item: AnalysisCostRecord
        data: IGenericCardData
        label: string
    }> = ({ item, data, label }) => {
        const [isOpen, setIsOpen] = React.useState(false)
        const [skuIsOpen, setSkuIsOpen] = React.useState(false)

        return (
            <Card fluid style={{ padding: '20px' }}>
                <Table celled compact>
                    <SUITable.Body>
                        <CheckboxRow isChecked={isOpen} setIsChecked={setIsOpen}>
                            {label}
                        </CheckboxRow>

                        <DisplayRow label="Cost">
                            {formatMoney(data.cost, 4)}{' '}
                            {(data?.jobs_cnt || 0) > 0 && <em>- across {data.jobs_cnt} job(s)</em>}
                        </DisplayRow>
                        <DisplayRow label="start">{data.usage_start_time}</DisplayRow>
                        <DisplayRow label="End">{data.usage_end_time}</DisplayRow>

                        <CheckboxRow
                            isChecked={skuIsOpen}
                            setIsChecked={setSkuIsOpen}
                            isVisible={isOpen}
                        >
                            Cost by SKU
                        </CheckboxRow>
                        <DisplayRow isVisible={isOpen && skuIsOpen} label="SKU">
                            <CostBySkuRow skus={data.skus} colSpan={1} chartMaxWidth="600" />
                        </DisplayRow>
                    </SUITable.Body>
                </Table>
            </Card>
        )
    }

    return (
        <>
            <AnalysisRunnerRecordCard data={data} />

            {_.orderBy(data?.batches || [], (b) => b.batch_id).map((batchRecord) => (
                <BatchCard item={batchRecord} key={`batch-card-${batchRecord.batch_id}`} />
            ))}

            {data.dataproc?.map((item, idx) => (
                <GenericCard key={`dataproc-${idx}`} item={data} data={item} label="DATAPROC" />
            ))}

            {data.wdl_tasks?.map((item) => (
                <GenericCard
                    key={`wdl-task-${item.wdl_task_name}`}
                    item={data}
                    data={item}
                    label={`WDL TASK NAME: ${item.wdl_task_name}`}
                />
            ))}

            {data.cromwell_sub_workflows?.map((item) => (
                <GenericCard
                    key={`cromwell-sub-workflow-${item.cromwell_sub_workflow_name}`}
                    item={data}
                    data={item}
                    label={`CROMWELL SUB WORKFLOW NAME: ${item.cromwell_sub_workflow_name}`}
                />
            ))}

            {data.cromwell_workflows?.map((item) => (
                <GenericCard
                    key={`cromwell-workflow-${item.cromwell_workflow_id}`}
                    item={data}
                    data={item}
                    label={`CROMWELL WORKFLOW ID: ${item.cromwell_workflow_id}`}
                />
            ))}
        </>
    )
}

export default BatchGrid
