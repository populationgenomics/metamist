import React from 'react'

import Table, { CheckboxRow, DisplayRow } from '../../../shared/components/Table'
import { Table as SUITable, TableProps } from 'semantic-ui-react'
import { DonutChart } from '../../../shared/components/Graphs/DonutChart'
import { AnalysisCostRecordSeqGroup, AnalysisCostRecordSku } from '../../../sm-api'
import formatMoney from '../../../shared/utilities/formatMoney'

export const calcDuration = (start: string | number | Date, end: string | number | Date) => {
    const duration = new Date(end).valueOf() - new Date(start).valueOf()
    const seconds = Math.floor((duration / 1000) % 60)
    const minutes = Math.floor((duration / (1000 * 60)) % 60)
    const hours = Math.floor((duration / (1000 * 60 * 60)) % 24)
    const formattedDuration = `${hours}h ${minutes}m ${seconds}s`
    return <span>{formattedDuration}</span>
}

export const CostBySkuRow: React.FC<{
    chartId?: string
    chartMaxWidth?: string
    colSpan: number
    skus: AnalysisCostRecordSku[]
}> = ({ chartId, chartMaxWidth, colSpan, skus }) => (
    <>
        {chartId && (
            <DonutChart
                id={chartId}
                data={skus.map((srec) => ({
                    label: srec.sku,
                    value: srec.cost,
                }))}
                maxSlices={skus.length}
                showLegend={false}
                isLoading={false}
                maxWidth={chartMaxWidth}
            />
        )}
        <Table celled compact>
            <SUITable.Header>
                <SUITable.Row>
                    <SUITable.HeaderCell>SKU</SUITable.HeaderCell>
                    <SUITable.HeaderCell>COST</SUITable.HeaderCell>
                </SUITable.Row>
            </SUITable.Header>
            <SUITable.Body>
                {skus.map((srec, sidx) => (
                    <SUITable.Row key={`sku-${srec.sku}`} id={`${chartId}-lgd${sidx}`}>
                        <SUITable.Cell>{srec.sku}</SUITable.Cell>
                        <SUITable.Cell>{formatMoney(srec.cost, 4)}</SUITable.Cell>
                    </SUITable.Row>
                ))}
            </SUITable.Body>
        </Table>
    </>
)

export const SeqGrpDisplay: React.FC<{ seq_groups: AnalysisCostRecordSeqGroup[] }> = ({
    seq_groups,
}) => {
    if (!seq_groups) {
        return <em>No sequencing groups</em>
    }

    return (
        <Table celled compact>
            <SUITable.Header>
                <SUITable.Row>
                    <SUITable.HeaderCell>SEQ GROUP</SUITable.HeaderCell>
                    <SUITable.HeaderCell>STAGE</SUITable.HeaderCell>
                    <SUITable.HeaderCell>COST</SUITable.HeaderCell>
                </SUITable.Row>
            </SUITable.Header>
            <SUITable.Body>
                {seq_groups
                    .sort((a, b) => b.cost - a.cost) // Sort by cost in descending order
                    .map((gcat) => (
                        <SUITable.Row key={`seq-grp-${gcat.sequencing_group}-${gcat.stage}`}>
                            <SUITable.Cell>{gcat.sequencing_group}</SUITable.Cell>
                            <SUITable.Cell>{gcat.stage}</SUITable.Cell>
                            <SUITable.Cell>{formatMoney(gcat.cost, 4)}</SUITable.Cell>
                        </SUITable.Row>
                    ))}
            </SUITable.Body>
        </Table>
    )
}
