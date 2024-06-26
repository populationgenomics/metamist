// HeaderCell.tsx
import React from 'react'
import { Table as SUITable } from 'semantic-ui-react'
import HtmlTooltip from '../../shared/utilities/htmlTooltip'
import { ProjectInsightsSummary } from '../../sm-api'
import FilterButton from './FilterButton'

export type ColumnKey = keyof ProjectInsightsSummary | string

export interface HeaderCellConfig {
    key: ColumnKey
    label: string
    sortable?: boolean
    filterable?: boolean
    tooltip?: string
    className?: string
}

interface HeaderCellProps {
    config: HeaderCellConfig
    sortDirection?: 'ascending' | 'descending'
    onSort: (key: ColumnKey, isMultiSort: boolean) => void
    onFilter?: (key: ColumnKey, selectedOptions: string[]) => void
    getUniqueOptionsForColumn: (key: ColumnKey) => string[]
    selectedOptions: string[]
}

export const HeaderCell: React.FC<HeaderCellProps> = ({
    config,
    sortDirection,
    onSort,
    onFilter,
    getUniqueOptionsForColumn,
    selectedOptions,
}) => {
    const { key, label, sortable, filterable, tooltip, className } = config

    return (
        <SUITable.HeaderCell
            className={`header-cell ${className || ''}`}
            sorted={sortable ? sortDirection : undefined}
            onClick={
                sortable
                    ? (event: React.MouseEvent<HTMLElement>) => onSort(key, event.shiftKey)
                    : undefined
            }
            onMouseEnter={
                className
                    ? (event: React.MouseEvent<HTMLElement>) =>
                          event.currentTarget.classList.add('expanded')
                    : undefined
            }
            onMouseLeave={
                className
                    ? (event: React.MouseEvent<HTMLElement>) =>
                          event.currentTarget.classList.remove('expanded')
                    : undefined
            }
        >
            {filterable && onFilter && (
                <div className="filter-button">
                    <FilterButton
                        columnName={label}
                        options={getUniqueOptionsForColumn(key)}
                        selectedOptions={selectedOptions}
                        onSelectionChange={(selectedOptions) => onFilter(key, selectedOptions)}
                    />
                </div>
            )}
            <div className="header-text">
                {tooltip ? (
                    <HtmlTooltip title={<p>{tooltip}</p>}>
                        <div>{label}</div>
                    </HtmlTooltip>
                ) : (
                    label
                )}
            </div>
        </SUITable.HeaderCell>
    )
}

export const summaryTableHeaderCellConfigs: HeaderCellConfig[] = [
    { key: 'dataset', label: 'Dataset', sortable: true },
    { key: 'sequencing_type', label: 'Seq Type', sortable: true },
    { key: 'sequencing_technology', label: 'Technology', sortable: true, filterable: true },
    { key: 'total_families', label: 'Families', sortable: true },
    { key: 'total_participants', label: 'Participants', sortable: true },
    { key: 'total_samples', label: 'Samples', sortable: true },
    { key: 'total_sequencing_groups', label: 'Sequencing Groups', sortable: true },
    { key: 'total_crams', label: 'CRAMs', sortable: true },
    {
        key: 'aligned_percentage',
        label: '% Aligned',
        sortable: false,
        tooltip: 'Percentage of Sequencing Groups with a Completed CRAM Analysis',
        className: 'collapsible-header',
    },
    {
        key: 'annotated_dataset_percentage',
        label: '% in Joint Callset',
        sortable: false,
        tooltip: 'Percentage of Sequencing Groups in the latest AnnotateDataset Analysis',
        className: 'collapsible-header',
    },
    {
        key: 'snv_index_percentage',
        label: '% in SNV ES-Index',
        sortable: false,
        tooltip: 'Percentage of Sequencing Groups in the latest SNV ES-Index Analysis',
        className: 'collapsible-header',
    },
    {
        key: 'sv_index_percentage',
        label: '% in SV ES-Index',
        sortable: false,
        tooltip:
            'Percentage of Sequencing Groups in the latest SV (genome) or gCNV (exome) ES-Index Analysis',
        className: 'collapsible-header',
    },
]

export const detailsTableHeaderCellConfigs: HeaderCellConfig[] = [
    { key: 'dataset', label: 'Dataset', sortable: true },
    { key: 'sequencing_type', label: 'Seq Type', sortable: true },
    {
        key: 'sequencing_technology',
        label: 'Tech',
        sortable: true,
        tooltip: 'Sequencing Technology',
        filterable: true,
    },
    {
        key: 'sequencing_platform',
        label: 'Platform',
        sortable: true,
        tooltip: 'Sequencing Platform',
        filterable: true,
    },
    {
        key: 'sample_type',
        label: 'Type',
        sortable: true,
        tooltip: 'Sample/Assay Type',
        filterable: true,
    },
    {
        key: 'sequencing_group_id',
        label: 'SG ID',
        sortable: true,
        tooltip: 'Internal Sequencing Group ID',
        filterable: true,
    },
    {
        key: 'family_id',
        label: 'Fam. ID',
        sortable: true,
        tooltip: 'Internal Family ID',
        filterable: true,
    },
    {
        key: 'family_ext_id',
        label: 'Family Ext ID',
        sortable: true,
        tooltip: 'External Family ID',
        filterable: true,
    },
    {
        key: 'participant_id',
        label: 'Ppt. ID',
        sortable: true,
        tooltip: 'Internal Participant ID',
        filterable: true,
    },
    {
        key: 'participant_ext_id',
        label: 'Ppt. Ext ID',
        sortable: true,
        tooltip: 'External Participant ID',
        filterable: true,
    },
    {
        key: 'sample_id',
        label: 'Sample ID',
        sortable: true,
        tooltip: 'Internal Sample ID',
        filterable: true,
    },
    {
        key: 'sample_ext_ids',
        label: 'Sample Ext ID(s)',
        sortable: true,
        tooltip: 'External Sample ID(s)',
        filterable: true,
    },
    {
        key: 'completed_cram',
        label: 'CRAM',
        sortable: true,
        tooltip: 'SG has completed CRAM analysis',
        filterable: true,
    },
    {
        key: 'in_latest_annotate_dataset',
        label: 'JC',
        sortable: true,
        tooltip: 'SG in the latest Joint Callset',
        filterable: true,
    },
    {
        key: 'in_latest_snv_es_index',
        label: 'SNV Index',
        sortable: true,
        tooltip: 'SG in the latest SNV Index',
        filterable: true,
    },
    {
        key: 'in_latest_sv_es_index',
        label: 'SV Index',
        sortable: true,
        tooltip: 'SG in the latest SV Index',
        filterable: true,
    },
    {
        key: 'stripy',
        label: 'Stripy',
        sortable: true,
        tooltip: 'SG STRipy report link',
        filterable: true,
    },
    {
        key: 'mito',
        label: 'Mito',
        sortable: true,
        tooltip: 'SG Mito report link',
        filterable: true,
    },
]
