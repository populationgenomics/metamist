import Paper from '@mui/material/Paper'
import TableBody from '@mui/material/TableBody'
import TableContainer from '@mui/material/TableContainer'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import orderBy from 'lodash/orderBy'
import React from 'react'
import { AnalysisCostRecordBatch, AnalysisCostRecordBatchJob } from '../../../sm-api'

import { TableComponents, TableVirtuoso } from 'react-virtuoso'
import { Checkbox, Table as SUITable, TableBodyProps, TableProps } from 'semantic-ui-react'
import Table, { DisplayRow } from '../../../shared/components/Table'
import formatMoney from '../../../shared/utilities/formatMoney'
import { calcDuration, CostBySkuRow } from './BillingByAnalysisComponents'

const ExpandableRow: React.FC<{ item: AnalysisCostRecordBatchJob }> = ({ item, ...props }) => {
    const [isOpen, setIsOpen] = React.useState(false)

    return (
        <>
            <TableRow
                {...props}
                className={item.job_id === null ? 'bold-text' : ''}
                style={
                    {
                        // backgroundColor: prepareBgColor(item),
                    }
                }
            >
                <SUITable.Cell style={{ width: 50 }}>
                    <Checkbox checked={isOpen} toggle onChange={() => setIsOpen(!isOpen)} />
                </SUITable.Cell>
                <SUITable.Cell style={{ width: 50 }}>{item.job_id}</SUITable.Cell>
                <SUITable.Cell>{item.job_name}</SUITable.Cell>
                <SUITable.Cell>{item.usage_start_time}</SUITable.Cell>
                <SUITable.Cell>
                    {calcDuration(item.usage_start_time, item.usage_end_time)}
                </SUITable.Cell>
                <SUITable.Cell>{formatMoney(item.cost, 4)}</SUITable.Cell>
            </TableRow>
            <DisplayRow isVisible={isOpen} label="" colSpan={4}>
                <CostBySkuRow skus={item.skus} colSpan={1} chartMaxWidth="600" />
            </DisplayRow>
        </>
    )
}

const TblComponents: TableComponents = {
    Scroller: React.forwardRef(function TableContainerWrapper(props, ref) {
        return (
            <TableContainer
                component={Paper}
                // @ts-ignore
                ref={ref}
                {...props}
            />
        )
    }),
    Table: (props: TableProps) => <Table {...props} style={{ borderCollapse: 'separate' }} />,
    TableHead: TableHead,
    // @ts-ignore
    TableRow: ExpandableRow,
    // @ts-ignore
    TableBody: React.forwardRef(function TableBodyWrapper(props: TableBodyProps, ref) {
        return <TableBody ref={ref} {...props} />
    }),
}

export const BatchJobsTable: React.FC<{ batch: AnalysisCostRecordBatch }> = ({ batch }) => {
    const [sortedData, setSortedData] = React.useState(
        orderBy(batch.jobs, (j) => parseInt(j.job_id))
    )

    React.useEffect(() => {
        // sort here to avoid sorting on each render
        setSortedData(orderBy(batch.jobs, (j) => parseInt(j.job_id)))
    }, [batch])

    return (
        <TableVirtuoso
            style={{
                height: batch.jobs.length > 1 ? 800 : 400,
                backgroundColor: 'var(--color-bg)',
            }}
            className="ui celled table compact"
            useWindowScroll={false}
            data={sortedData}
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
            components={TblComponents}
        />
    )
}
