import React from 'react'
import { Table as SUITable } from 'semantic-ui-react'
import HtmlTooltip from '../../shared/utilities/htmlTooltip'
import { ProjectInsightsSummary } from '../../sm-api'

export interface FooterCellConfig {
    key: string
    label: string
    calculateValue: (data: ProjectInsightsSummary[]) => number | string
    tooltip?: {
        numerator?: (data: ProjectInsightsSummary[]) => number
        denominator?: (data: ProjectInsightsSummary[]) => number
        description: string
    }
    formatValue?: (value: number) => string
}

interface FooterCellProps {
    config: FooterCellConfig
    data: ProjectInsightsSummary[]
}

export const FooterCell: React.FC<FooterCellProps> = ({ config, data }) => {
    const value = config.calculateValue(data)

    const cellContent = config.tooltip ? (
        <div style={{ display: 'flex', justifyContent: 'center' }}>
            <HtmlTooltip
                title={
                    <p>
                        {config.tooltip.numerator && `${config.tooltip.numerator(data)} / `}
                        {config.tooltip.denominator && `${config.tooltip.denominator(data)} `}
                        {config.tooltip.description}
                    </p>
                }
            >
                <div>{config.formatValue ? config.formatValue(value as number) : value}</div>
            </HtmlTooltip>
        </div>
    ) : (
        value
    )

    return <SUITable.Cell className="table-cell">{cellContent}</SUITable.Cell>
}

export const footerCellConfigs: FooterCellConfig[] = [
    {
        key: 'grand-total',
        label: 'Grand Total',
        calculateValue: (data) => 'Grand Total',
    },
    {
        key: 'total-entries',
        label: 'Total Entries',
        calculateValue: (data) => `${data.length} entries`,
    },
    {
        key: 'empty-cell',
        label: '',
        calculateValue: () => '',
    },
    {
        key: 'total-families',
        label: 'Total Families',
        calculateValue: (data) => data.reduce((acc, curr) => acc + curr.total_families, 0),
    },
    {
        key: 'total-participants',
        label: 'Total Participants',
        calculateValue: (data) => data.reduce((acc, curr) => acc + curr.total_participants, 0),
    },
    {
        key: 'total-samples',
        label: 'Total Samples',
        calculateValue: (data) => data.reduce((acc, curr) => acc + curr.total_samples, 0),
    },
    {
        key: 'total-sequencing-groups',
        label: 'Total Sequencing Groups',
        calculateValue: (data) => data.reduce((acc, curr) => acc + curr.total_sequencing_groups, 0),
    },
    {
        key: 'total-crams',
        label: 'Total CRAMs',
        calculateValue: (data) => data.reduce((acc, curr) => acc + curr.total_crams, 0),
    },
    {
        key: 'aligned-percentage',
        label: 'Aligned Percentage',
        calculateValue: (data) => {
            const totalCrams = data.reduce((acc, curr) => acc + curr.total_crams, 0)
            const totalSequencingGroups = data.reduce(
                (acc, curr) => acc + curr.total_sequencing_groups,
                0
            )
            return (totalCrams / totalSequencingGroups) * 100 || 0
        },
        tooltip: {
            numerator: (data) => data.reduce((acc, curr) => acc + curr.total_crams, 0),
            denominator: (data) =>
                data.reduce((acc, curr) => acc + curr.total_sequencing_groups, 0),
            description: 'Total Sequencing Groups with a Completed CRAM Analysis',
        },
        formatValue: (value) => `${value.toFixed(2)}%`,
    },
    {
        key: 'annotate-dataset-percentage',
        label: 'Annotate Dataset Percentage',
        calculateValue: (data) => {
            const totalAnnotated = data.reduce(
                (acc, curr) => acc + (curr.latest_annotate_dataset?.sg_count ?? 0),
                0
            )
            const totalSequencingGroups = data.reduce(
                (acc, curr) => acc + curr.total_sequencing_groups,
                0
            )
            return (totalAnnotated / totalSequencingGroups) * 100 || 0
        },
        tooltip: {
            numerator: (data) =>
                data.reduce((acc, curr) => acc + (curr.latest_annotate_dataset?.sg_count ?? 0), 0),
            denominator: (data) =>
                data.reduce((acc, curr) => acc + curr.total_sequencing_groups, 0),
            description: 'Total Sequencing Groups in the latest AnnotateDataset analysis',
        },
        formatValue: (value) => `${value.toFixed(2)}%`,
    },
    {
        key: 'snv-es-index-percentage',
        label: 'SNV ES Index Percentage',
        calculateValue: (data) => {
            const totalIndexed = data.reduce(
                (acc, curr) => acc + (curr.latest_snv_es_index?.sg_count ?? 0),
                0
            )
            const totalSequencingGroups = data.reduce(
                (acc, curr) => acc + curr.total_sequencing_groups,
                0
            )
            return (totalIndexed / totalSequencingGroups) * 100 || 0
        },
        tooltip: {
            numerator: (data) =>
                data.reduce((acc, curr) => acc + (curr.latest_snv_es_index?.sg_count ?? 0), 0),
            denominator: (data) =>
                data.reduce((acc, curr) => acc + curr.total_sequencing_groups, 0),
            description: 'Total Sequencing Groups in the latest SNV Elasticsearch Index',
        },
        formatValue: (value) => `${value.toFixed(2)}%`,
    },
    {
        key: 'sv-es-index-percentage',
        label: 'SV ES Index Percentage',
        calculateValue: (data) => {
            const totalIndexed = data.reduce(
                (acc, curr) => acc + (curr.latest_sv_es_index?.sg_count ?? 0),
                0
            )
            const totalSequencingGroups = data.reduce(
                (acc, curr) => acc + curr.total_sequencing_groups,
                0
            )
            return (totalIndexed / totalSequencingGroups) * 100 || 0
        },
        tooltip: {
            numerator: (data) =>
                data.reduce((acc, curr) => acc + (curr.latest_sv_es_index?.sg_count ?? 0), 0),
            denominator: (data) =>
                data.reduce((acc, curr) => acc + curr.total_sequencing_groups, 0),
            description: 'Total Sequencing Groups in the latest SV Elasticsearch Index',
        },
        formatValue: (value) => `${value.toFixed(2)}%`,
    },
]
