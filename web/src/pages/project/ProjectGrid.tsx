import _, { capitalize } from 'lodash'
import * as React from 'react'

import { TableHeader } from 'semantic-ui-react'
import Table from '../../shared/components/Table'
import {
    MetaSearchEntityPrefix,
    ProjectParticipantGridField,
    ProjectParticipantGridFilter,
    ProjectParticipantGridResponse,
} from '../../sm-api/api'
import { ProjectExportButton } from './ExportProjectButton'
import { ProjectGridParticipantRows } from './ParticipantGridRow'
import {
    headerGroupOrder,
    metaSeachEntityPrefixToFilterKey,
    metaSearchEntityToTitle,
} from './ProjectGridHeaderGroup'
import { ValueFilterPopup } from './ValueFilter'

interface ProjectGridProps {
    participantResponse?: ProjectParticipantGridResponse
    projectName: string

    headerGroups: Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]>
    filterValues: ProjectParticipantGridFilter
    updateFilters: (e: Partial<ProjectParticipantGridFilter>) => void
}

const ProjectGrid: React.FunctionComponent<ProjectGridProps> = ({
    participantResponse: summary,
    projectName,
    headerGroups,
    filterValues,
    updateFilters,
}) => {
    if (!summary) {
        return (
            <p>
                <em>No data</em>
            </p>
        )
    }

    const filteredFamilyFields =
        headerGroups[MetaSearchEntityPrefix.F]?.filter((f) => f.is_visible) ?? []
    const filteredParticipantFields =
        headerGroups[MetaSearchEntityPrefix.P]?.filter((f) => f.is_visible) ?? []
    const filteredSampleFields =
        headerGroups[MetaSearchEntityPrefix.S]?.filter((f) => f.is_visible) ?? []
    const filteredSequencingGroupFields =
        headerGroups[MetaSearchEntityPrefix.Sg]?.filter((f) => f.is_visible) ?? []
    const filteredAssayFields =
        headerGroups[MetaSearchEntityPrefix.A]?.filter((f) => f.is_visible) ?? []

    const visibleHeaderGroups = {
        [MetaSearchEntityPrefix.F]: filteredFamilyFields,
        [MetaSearchEntityPrefix.P]: filteredParticipantFields,
        [MetaSearchEntityPrefix.S]: filteredSampleFields,
        [MetaSearchEntityPrefix.Sg]: filteredSequencingGroupFields,
        [MetaSearchEntityPrefix.A]: filteredAssayFields,
    }

    return (
        <>
            <ProjectExportButton
                participants_in_query={summary.total_results}
                projectName={projectName}
                filterValues={filterValues}
                headerGroups={headerGroups}
            />

            <Table
                className="projectSummaryGrid"
                style={{
                    borderCollapse: 'collapse',
                    borderTop: '2px solid var(--color-border-color)',
                    borderRight: '2px solid var(--color-border-color)',
                    borderBottom: '2px solid var(--color-border-color)',
                }}
            >
                <TableHeader>
                    <ProjectGridCategoryHeader headerGroups={visibleHeaderGroups} />
                    <ProjectGridFilterRow
                        headerGroups={visibleHeaderGroups}
                        filterValues={filterValues}
                        updateFilters={updateFilters}
                    />
                    <ProjectGridFieldHeaderRow headerGroups={visibleHeaderGroups} />
                </TableHeader>
                <tbody>
                    {summary.participants.map((p, pidx) => (
                        <ProjectGridParticipantRows
                            key={`pgrid-row-participant-row-${p.id}`}
                            participant={p}
                            familyFields={filteredFamilyFields}
                            participantFields={filteredParticipantFields}
                            sampleFields={filteredSampleFields}
                            sequencingGroupFields={filteredSequencingGroupFields}
                            assayFields={filteredAssayFields}
                            projectName={projectName}
                            backgroundColor={
                                pidx % 2 === 1 ? 'var(--color-bg)' : 'var(--color-bg-disabled)'
                            }
                        />
                    ))}
                </tbody>
            </Table>
        </>
    )
}

const ProjectGridCategoryHeader: React.FC<{
    headerGroups: Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]>
}> = ({ headerGroups }) => {
    return (
        <tr>
            {headerGroupOrder.map((category) => {
                const fields = headerGroups[category]
                if (fields === undefined) {
                    debugger
                }
                const visible = fields.length // .filter((f) => f.is_visible).length
                if (visible === 0) return <React.Fragment key={`category-header-${category}`} />
                return (
                    <th
                        key={`category-header-${category}`}
                        colSpan={visible}
                        style={{
                            textAlign: 'center',
                            borderLeft: '2px solid var(--color-border-color)',
                            borderBottom: '2px solid var(--color-border-default)',
                            backgroundColor: 'var(--color-table-header)',
                        }}
                    >
                        {_.startCase(metaSearchEntityToTitle(category))}
                    </th>
                )
            })}
        </tr>
    )
}

const ProjectGridFilterRow: React.FC<{
    headerGroups: Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]>
    filterValues: ProjectParticipantGridFilter
    updateFilters: (e: Partial<ProjectParticipantGridFilter>) => void
}> = ({ headerGroups, filterValues, updateFilters }) => {
    return (
        // <SUITable.Header>
        <tr>
            {headerGroupOrder.flatMap((category) =>
                headerGroups[category]
                    .filter((f) => f.is_visible)
                    .map((field, idx) => {
                        if (!field.filter_key) {
                            return (
                                <th
                                    key={`${category}-${field.key}-${idx}`}
                                    style={{
                                        borderBottom: 'none',
                                        borderLeft:
                                            idx === 0
                                                ? '2px solid var(--color-border-color)'
                                                : '1px solid var(--color-border-default)',
                                    }}
                                ></th>
                            )
                        }
                        // debugger
                        return (
                            <th
                                key={`filter-${category}-${field.key}-${idx}`}
                                style={{
                                    borderBottom: 'none',
                                    borderLeft:
                                        idx === 0
                                            ? '2px solid var(--color-border-color)'
                                            : '1px solid var(--color-border-default)',
                                }}
                            >
                                <ValueFilterPopup
                                    filterValues={filterValues}
                                    updateFilterValues={updateFilters}
                                    category={metaSeachEntityPrefixToFilterKey(category)}
                                    field={field}
                                />
                            </th>
                        )
                    })
            )}
        </tr>
        // </SUITable.Header>
    )
}

const ProjectGridFieldHeaderRow: React.FC<{
    headerGroups: Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]>
}> = ({ headerGroups }) => {
    return (
        // <SUITable.Header>
        <tr>
            {headerGroupOrder.flatMap((hg) =>
                headerGroups[hg]
                    ?.filter((f) => f.is_visible)
                    .map((field, idx) => (
                        <th
                            key={`field-header-row-${hg}-${field.key}-${idx}`}
                            style={{
                                borderLeft:
                                    idx === 0
                                        ? '2px solid var(--color-border-color)'
                                        : '1px solid var(--color-border-default)',
                                borderBottom: '2px solid var(--color-border-default)',
                            }}
                        >
                            {field.label.includes(' ') ? field.label : capitalize(field.label)}
                        </th>
                    ))
            )}
        </tr>
    )
}

export default ProjectGrid
