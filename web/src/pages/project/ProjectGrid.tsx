import capitalize  from 'lodash/capitalize'
import startCase  from 'lodash/startCase'
import * as React from 'react'

import { TableBody, TableHeader, TableHeaderCell, TableRow } from 'semantic-ui-react'
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
    firstColBorder,
    headerGroupOrder,
    metaSeachEntityPrefixToFilterKey,
    metaSearchEntityToTitle,
    otherColBorder,
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
    participantResponse,
    projectName,
    headerGroups,
    filterValues,
    updateFilters,
}) => {
    if (!participantResponse) {
        return (
            <p>
                <em>No data</em>
            </p>
        )
    }

    function filteredFields<P extends keyof typeof MetaSearchEntityPrefix>(prefixType: P) {
        return headerGroups[MetaSearchEntityPrefix[prefixType]]?.filter((f) => f.is_visible) ?? []
    }

    const visibleHeaderGroups = {
        [MetaSearchEntityPrefix.F]: filteredFields('F'),
        [MetaSearchEntityPrefix.P]: filteredFields('P'),
        [MetaSearchEntityPrefix.S]: filteredFields('S'),
        [MetaSearchEntityPrefix.Sg]: filteredFields('Sg'),
        [MetaSearchEntityPrefix.A]: filteredFields('A'),
    }

    return (
        <>
            <ProjectExportButton
                participants_in_query={participantResponse.total_results}
                projectName={projectName}
                filterValues={filterValues}
                headerGroups={headerGroups}
            />

            <Table
                className="projectSummaryGrid"
                style={{
                    borderCollapse: 'collapse',
                    borderTop: firstColBorder,
                    borderRight: firstColBorder,
                    borderBottom: firstColBorder,
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
                <TableBody>
                    {participantResponse.participants.map((p, pidx) => (
                        <ProjectGridParticipantRows
                            key={`pgrid-row-participant-row-${p.id}`}
                            participant={p}
                            familyFields={visibleHeaderGroups[MetaSearchEntityPrefix.F]}
                            participantFields={visibleHeaderGroups[MetaSearchEntityPrefix.P]}
                            sampleFields={visibleHeaderGroups[MetaSearchEntityPrefix.S]}
                            sequencingGroupFields={visibleHeaderGroups[MetaSearchEntityPrefix.Sg]}
                            assayFields={visibleHeaderGroups[MetaSearchEntityPrefix.A]}
                            projectName={projectName}
                            backgroundColor={
                                pidx % 2 === 1 ? 'var(--color-bg)' : 'var(--color-bg-disabled)'
                            }
                        />
                    ))}
                </TableBody>
            </Table>
        </>
    )
}

const ProjectGridCategoryHeader: React.FC<{
    headerGroups: Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]>
}> = ({ headerGroups }) => {
    return (
        <TableRow>
            {headerGroupOrder.map((category) => {
                const fields = headerGroups[category]
                const visible = fields?.length || 0
                if (visible === 0) return <React.Fragment key={`category-header-${category}`} />
                return (
                    <TableHeaderCell
                        key={`category-header-${category}`}
                        colSpan={visible}
                        style={{
                            textAlign: 'center',
                            borderLeft: firstColBorder,
                            borderBottom: firstColBorder,
                            backgroundColor: 'var(--color-table-header)',
                        }}
                    >
                        {startCase(metaSearchEntityToTitle(category))}
                    </TableHeaderCell>
                )
            })}
        </TableRow>
    )
}

const ProjectGridFilterRow: React.FC<{
    headerGroups: Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]>
    filterValues: ProjectParticipantGridFilter
    updateFilters: (e: Partial<ProjectParticipantGridFilter>) => void
}> = ({ headerGroups, filterValues, updateFilters }) => {
    return (
        <TableRow>
            {headerGroupOrder.flatMap((category) =>
                headerGroups[category]
                    .filter((f) => f.is_visible)
                    .map((field, idx) => {
                        if (!field.filter_key) {
                            return (
                                <TableHeaderCell
                                    key={`${category}-${field.key}-${idx}`}
                                    style={{
                                        borderBottom: 'none',
                                        borderLeft: idx === 0 ? firstColBorder : otherColBorder,
                                    }}
                                ></TableHeaderCell>
                            )
                        }

                        return (
                            <TableHeaderCell
                                key={`filter-${category}-${field.key}-${idx}`}
                                style={{
                                    borderBottom: 'none',
                                    borderLeft: idx === 0 ? firstColBorder : otherColBorder,
                                }}
                            >
                                <ValueFilterPopup
                                    filterValues={filterValues}
                                    updateFilterValues={updateFilters}
                                    category={metaSeachEntityPrefixToFilterKey(category)}
                                    field={field}
                                />
                            </TableHeaderCell>
                        )
                    })
            )}
        </TableRow>
    )
}

const ProjectGridFieldHeaderRow: React.FC<{
    headerGroups: Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]>
}> = ({ headerGroups }) => {
    return (
        <TableRow>
            {headerGroupOrder.flatMap((hg) =>
                headerGroups[hg]
                    ?.filter((f) => f.is_visible)
                    .map((field, idx) => (
                        <TableHeaderCell
                            key={`field-header-row-${hg}-${field.key}-${idx}`}
                            style={{
                                borderLeft: idx === 0 ? firstColBorder : otherColBorder,
                                borderBottom: firstColBorder,
                            }}
                        >
                            {field.label.includes(' ') ? field.label : capitalize(field.label)}
                        </TableHeaderCell>
                    ))
            )}
        </TableRow>
    )
}

export default ProjectGrid
