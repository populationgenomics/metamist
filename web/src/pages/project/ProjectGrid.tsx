import _, { capitalize } from 'lodash'
import * as React from 'react'

import { TableHeader } from 'semantic-ui-react'
import Table from '../../shared/components/Table'
import { ProjectParticipantGridFilter, ProjectParticipantGridResponse } from '../../sm-api/api'
import { ProjectExportButton } from './ExportProjectButton'
import { ProjectGridParticipantRows } from './ParticipantGridRow'
import { MetaSearchEntityPrefix, ProjectGridHeaderGroup } from './ProjectColumnOptions'
import { ValueFilterPopup } from './ValueFilter'

interface ProjectGridProps {
    participantResponse?: ProjectParticipantGridResponse
    projectName: string

    headerGroups: ProjectGridHeaderGroup[]
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

    const headerGroupByCategory = _.keyBy(headerGroups, 'category')

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
                    <ProjectGridCategoryHeader headerGroups={headerGroups} />
                    <ProjectGridFilterRow
                        headerGroups={headerGroups}
                        filterValues={filterValues}
                        updateFilters={updateFilters}
                    />
                    <ProjectGridFieldHeaderRow headerGroups={headerGroups} />
                </TableHeader>
                <tbody>
                    {summary.participants.map((p, pidx) => (
                        <ProjectGridParticipantRows
                            key={`pgrid-row-participant-row-${p.id}`}
                            participant={p}
                            familyFields={
                                headerGroupByCategory[MetaSearchEntityPrefix.F]?.fields ?? []
                            }
                            participantFields={
                                headerGroupByCategory[MetaSearchEntityPrefix.P]?.fields ?? []
                            }
                            sampleFields={
                                headerGroupByCategory[MetaSearchEntityPrefix.S]?.fields ?? []
                            }
                            sequencingGroupFields={
                                headerGroupByCategory[MetaSearchEntityPrefix.Sg]?.fields ?? []
                            }
                            assayFields={
                                headerGroupByCategory[MetaSearchEntityPrefix.A]?.fields ?? []
                            }
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

const ProjectGridCategoryHeader: React.FC<{ headerGroups: ProjectGridHeaderGroup[] }> = ({
    headerGroups,
}) => {
    return (
        <tr>
            {headerGroups.map(({ category, fields }) => {
                const visible = fields.filter((f) => f.isVisible).length
                if (visible === 0) return <React.Fragment />
                return (
                    <th
                        key={`category-header-${category}`}
                        colSpan={fields.filter((f) => f.isVisible).length}
                        style={{
                            textAlign: 'center',
                            borderLeft: '2px solid var(--color-border-color)',
                            borderBottom: '2px solid var(--color-border-default)',
                            backgroundColor: 'var(--color-table-header)',
                        }}
                    >
                        {_.startCase(category.replaceAll('_', ' '))}
                    </th>
                )
            })}
        </tr>
    )
}

const ProjectGridFilterRow: React.FC<{
    headerGroups: ProjectGridHeaderGroup[]
    filterValues: ProjectParticipantGridFilter
    updateFilters: (e: Partial<ProjectParticipantGridFilter>) => void
}> = ({ headerGroups, filterValues, updateFilters }) => {
    return (
        // <SUITable.Header>
        <tr>
            {headerGroups.flatMap((hg) =>
                hg.fields
                    .filter((f) => f.isVisible)
                    .map((field, idx) => {
                        if (
                            field.title === 'Sample ID' ||
                            field.title === 'Created date' ||
                            field.title === 'Sequencing Group ID'
                        ) {
                            return (
                                <th
                                    key={`${hg.category}-${field.name}-${idx}`}
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
                                key={`filter-${hg.category}-${field.name}-${idx}`}
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
                                    category={hg.category}
                                    filterKey={field.name}
                                />
                            </th>
                        )
                    })
            )}
        </tr>
        // </SUITable.Header>
    )
}

const ProjectGridFieldHeaderRow: React.FC<{ headerGroups: ProjectGridHeaderGroup[] }> = ({
    headerGroups,
}) => {
    return (
        // <SUITable.Header>
        <tr>
            {headerGroups.flatMap((hg) =>
                hg.fields
                    .filter((f) => f.isVisible)
                    .map((field, idx) => (
                        <th
                            key={`field-header-row-${hg.category}-${field.name}-${idx}`}
                            style={{
                                borderLeft:
                                    idx === 0
                                        ? '2px solid var(--color-border-color)'
                                        : '1px solid var(--color-border-default)',
                                borderBottom: '2px solid var(--color-border-default)',
                            }}
                        >
                            {field.title.includes(' ') ? field.title : capitalize(field.title)}
                        </th>
                    ))
            )}
        </tr>
        // </SUITable.Header>
    )
}

export default ProjectGrid
