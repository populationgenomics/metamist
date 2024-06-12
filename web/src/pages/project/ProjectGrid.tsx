import _, { capitalize } from 'lodash'
import * as React from 'react'

import { TableHeader } from 'semantic-ui-react'
import FamilyLink from '../../shared/components/links/FamilyLink'
import SampleLink from '../../shared/components/links/SampleLink'
import SequencingGroupLink from '../../shared/components/links/SequencingGroupLink'
import Table from '../../shared/components/Table'
import sanitiseValue from '../../shared/utilities/sanitiseValue'
import {
    NestedParticipant,
    ProjectParticipantGridFilter,
    ProjectParticipantGridResponse,
} from '../../sm-api/api'
import { ProjectExportButton } from './ExportProjectButton'
import {
    MetaSearchEntityPrefix,
    ProjectGridField,
    ProjectGridHeaderGroup,
} from './ProjectColumnOptions'
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
                            key={`participant-row-${p.id}`}
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

interface IProjectGridParticipantRowProps {
    projectName: string

    participant: NestedParticipant
    familyFields: ProjectGridField[]
    participantFields: ProjectGridField[]
    sampleFields: ProjectGridField[]
    sequencingGroupFields: ProjectGridField[]
    assayFields: ProjectGridField[]

    backgroundColor?: string
}

const ProjectGridParticipantRows: React.FC<IProjectGridParticipantRowProps> = ({
    participant,
    backgroundColor,
    projectName,
    participantFields,
    sampleFields,
    sequencingGroupFields,
    assayFields,
    familyFields,
    ...props
}) => {
    // console.log(
    //     participant.external_id,
    //     sampleFields.map((s) => s.name)
    // )
    const rows = participant.samples.flatMap((s, sidx) => {
        // const border = '1px solid #dee2e6'
        const lengthOfParticipant = participant.samples
            .map((s_) =>
                // do 1 here, because we want to show at least 1 row, even if there are
                // no sequencing groups OR assays
                Math.max(
                    1,
                    (s_.sequencing_groups ?? [])
                        .map((a_) => (a_.assays ?? []).length)
                        .reduce((a, b) => a + b, 0)
                )
            )
            .reduce((a, b) => a + b, 0)

        const lengthOfSamples = (s.sequencing_groups ?? [])
            .map((a_) => (a_.assays ?? []).length)
            .reduce((a, b) => a + b, 0)

        const participantRowSpan = lengthOfParticipant > 0 ? lengthOfParticipant : undefined
        const samplesRowSpan = lengthOfSamples > 0 ? lengthOfSamples : undefined

        let sgs = s.sequencing_groups || []
        if (!sgs || sgs.length === 0) {
            // @ts-ignore
            sgs = [{}]
        }
        return sgs.flatMap((sg, sgidx) =>
            (!!sg?.assays ? sg.assays : [{ id: 0 }]).map((assay, assayidx) => {
                const isFirstOfGroup = sidx === 0 && sgidx === 0 && assayidx === 0
                const border = '1px solid #dee2e6'
                // const border = '1px solid'
                return (
                    <tr key={`${participant.external_id}-${s.id}-${sg.id}-${assay.id}`} {...props}>
                        {isFirstOfGroup && (
                            <td
                                style={{
                                    backgroundColor,
                                    borderRight: border,
                                    borderBottom: border,
                                    borderTop: border,
                                    borderLeft: '2px solid var(--color-border-color)',
                                }}
                                rowSpan={participantRowSpan}
                            >
                                <FamilyLink
                                    id={participant.families.map((f) => f.id).join(', ')}
                                    projectName={projectName}
                                >
                                    {participant.families.map((f) => f.external_id).join(', ')}
                                </FamilyLink>
                            </td>
                        )}
                        {isFirstOfGroup &&
                            participantFields.map((field, i) => (
                                <td
                                    style={{
                                        display: field.isVisible ? 'table-cell' : 'none',
                                        backgroundColor,
                                        borderRight: border,
                                        borderBottom: border,
                                        borderTop: border,
                                        borderLeft:
                                            i === 0
                                                ? '2px solid var(--color-border-color)'
                                                : '1px solid var(--color-border-default)',
                                    }}
                                    key={`${participant.id}participant.${field.name}`}
                                    rowSpan={participantRowSpan}
                                >
                                    {sanitiseValue(_.get(participant, field.name))}
                                </td>
                            ))}
                        {sgidx === 0 &&
                            assayidx === 0 &&
                            sampleFields.map((field, i) => (
                                <td
                                    style={{
                                        display: field.isVisible ? 'table-cell' : 'none',
                                        backgroundColor,
                                        borderRight: border,
                                        borderBottom: border,
                                        borderTop: border,
                                        borderLeft:
                                            i === 0
                                                ? '2px solid var(--color-border-color)'
                                                : '1px solid var(--color-border-default)',
                                        // border,
                                    }}
                                    key={`${s.id}sample.${field.name}`}
                                    rowSpan={samplesRowSpan}
                                >
                                    {field.name === 'external_id' || field.name === 'id' ? (
                                        <SampleLink id={s.id} projectName={projectName}>
                                            {sanitiseValue(_.get(s, field.name))}
                                        </SampleLink>
                                    ) : (
                                        sanitiseValue(_.get(s, field.name))
                                    )}
                                </td>
                            ))}
                        {assayidx === 0 &&
                            sequencingGroupFields.map((field, i) => (
                                <td
                                    style={{
                                        display: field.isVisible ? 'table-cell' : 'none',
                                        borderRight: border,
                                        borderBottom: border,
                                        borderTop: border,
                                        borderLeft:
                                            i === 0
                                                ? '2px solid var(--color-border-color)'
                                                : '1px solid var(--color-border-default)',
                                        backgroundColor,
                                    }}
                                    key={`${s.id}sequencing_group.${field.name}`}
                                    rowSpan={
                                        (sg.assays ?? []).length > 0
                                            ? (sg.assays ?? []).length
                                            : undefined
                                    }
                                >
                                    {field.name === 'id' ? (
                                        <SequencingGroupLink
                                            projectName={projectName}
                                            id={s.id}
                                            sg_id={_.get(sg, 'id')?.toString()}
                                        >
                                            {sanitiseValue(_.get(sg, field.name))}
                                        </SequencingGroupLink>
                                    ) : (
                                        sanitiseValue(_.get(sg, field.name))
                                    )}
                                </td>
                            ))}
                        {assayFields.map((field, i) => (
                            <td
                                style={{
                                    display: field.isVisible ? 'table-cell' : 'none',
                                    backgroundColor,
                                    borderRight: border,
                                    borderBottom: border,
                                    borderTop: border,
                                    borderLeft:
                                        i === 0
                                            ? '2px solid var(--color-border-color)'
                                            : '1px solid var(--color-border-default)',
                                    // border,
                                }}
                                key={`${s.id}assay.${field.name}`}
                            >
                                {sanitiseValue(_.get(assay, field.name))}
                            </td>
                        ))}
                    </tr>
                )
            })
        )
    })
    // return rows
    return <>{rows}</>
}

export default ProjectGrid
