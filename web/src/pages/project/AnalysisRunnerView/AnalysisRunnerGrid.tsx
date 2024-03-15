import * as React from 'react'
import { Table as SUITable, Popup, Checkbox } from 'semantic-ui-react'
import _, { Dictionary } from 'lodash'
import Table from '../../../shared/components/Table'
import sanitiseValue from '../../../shared/utilities/sanitiseValue'

import { Filter } from './Filter'
import './AnalysisGrid.css'

export interface AnalysisRunnerGridItem {
    arGuid: string
    timestamp: Date
    accessLevel: string
    repository: string
    commit: string
    script: string
    description: string
    driverImage: string
    configPath: string
    cwd?: string
    environment: string
    hailVersion?: string
    batchUrl: string
    submittingUser: string
    outputPath: string
    meta: any

    // internal
    position: number
}

type AnalysisRunnerGridItemKeys = keyof AnalysisRunnerGridItem

interface IMainField {
    field: AnalysisRunnerGridItemKeys
    title: string
    width?: number
    filterable?: boolean
    renderer?: (
        log: AnalysisRunnerGridItem,
        filters: Filter[],
        updateFilter: (value: string, field: string) => void
    ) => React.ReactNode
}

const MAIN_FIELDS: IMainField[] = [
    {
        field: 'arGuid',
        title: 'AR GUID',
        filterable: true,
        renderer: (log: AnalysisRunnerGridItem) => (
            <Popup
                trigger={
                    <span
                        style={{
                            cursor: 'pointer',
                        }}
                    >
                        {log.arGuid.substring(0, 7)}
                    </span>
                }
                hoverable
                position="bottom center"
            >
                {log.arGuid}
            </Popup>
        ),
    },
    {
        field: 'batchUrl',
        title: 'Hail Batch',
        filterable: true,
        renderer: (log: AnalysisRunnerGridItem) => (
            <a href={log.batchUrl} rel="noopener noreferrer" target="_blank">
                {log.batchUrl}
            </a>
        ),
    },
    {
        field: 'repository',
        title: 'GitHub',
        width: 200,
        filterable: true,
        renderer: (log: AnalysisRunnerGridItem) => (
            <a
                href={`${`https://www.github.com/populationgenomics/${log.repository}/tree/${log.commit}`}`}
                rel="noopener noreferrer"
                target="_blank"
            >
                {log.repository}@{log.commit?.substring(0, 7)}
            </a>
        ),
    },
    {
        field: 'script',
        title: 'Script',
        width: 200,
        filterable: true,
        renderer: (log: AnalysisRunnerGridItem) => <code>{log.script}</code>,
    },
    {
        field: 'submittingUser',
        title: 'Author',
        filterable: true,
        renderer: (log: AnalysisRunnerGridItem, filters: Filter[], updateFilter) => (
            <Popup
                trigger={
                    <span
                        style={{
                            textDecoration: 'underline',
                            cursor: 'pointer',
                        }}
                        onClick={() => {
                            if (
                                filters.find((f) => f.category === 'submittingUser')?.value ===
                                log.submittingUser
                            ) {
                                updateFilter('', 'submittingUser')
                            } else {
                                updateFilter(log.submittingUser, 'submittingUser')
                            }
                        }}
                    >
                        {log.submittingUser.split('@')[0]}
                    </span>
                }
                hoverable
                position="bottom center"
            >
                {log.submittingUser}
            </Popup>
        ),
    },
    {
        field: 'timestamp',
        title: 'Date',
        renderer: (log: AnalysisRunnerGridItem) => (
            <Popup
                trigger={
                    <span
                        style={{
                            // textDecoration: 'underline',
                            cursor: 'pointer',
                        }}
                    >
                        {log.timestamp.toLocaleString()}
                    </span>
                }
                hoverable
                position="bottom center"
            >
                {log.timestamp.toISOString()}
            </Popup>
        ),
    },
    {
        field: 'accessLevel',
        title: 'Access Level',
        filterable: true,
        renderer: (log: AnalysisRunnerGridItem, filters: Filter[], updateFilter) => (
            <span
                style={{
                    textDecoration: 'underline',
                    cursor: 'pointer',
                    width: '100px',
                }}
                onClick={() => {
                    if (
                        filters.find((f) => f.category === 'accessLevel')?.value === log.accessLevel
                    ) {
                        updateFilter('', 'accessLevel')
                    } else {
                        updateFilter(log.accessLevel, 'accessLevel')
                    }
                }}
            >
                {log.accessLevel}
            </span>
        ),
    },
    {
        field: 'driverImage',
        title: 'Driver Image',
        renderer: (log: AnalysisRunnerGridItem) => {
            // format: australia.southeast1-docker.pkg.dev/populationgenomics-internal/hail-driver/hail-driver:latest
            const parts = log.driverImage.split('/')
            const imageName = parts[parts.length - 1]
            if (imageName === log.driverImage) return <code>{imageName}</code>
            return (
                <Popup
                    trigger={
                        <span
                            style={{
                                // textDecoration: 'underline',
                                cursor: 'pointer',
                            }}
                        >
                            <code>{imageName}</code>
                        </span>
                    }
                    hoverable
                    position="bottom center"
                >
                    {log.driverImage}
                </Popup>
            )
        },
    },
    // { field: 'description', title: 'Description' },
]

const EXTRA_FIELDS: IMainField[] = [
    { field: 'outputPath', title: 'Output Path' },
    { field: 'description', title: 'Description' },
    { field: 'script', title: 'Script' },
    { field: 'cwd', title: 'CWD' },
    { field: 'configPath', title: 'Config Path' },
    // { field: 'environment', title: 'Environment' },
    { field: 'hailVersion', title: 'Hail Version' },
]

interface IAnalysisRunnerGridProps {
    data: AnalysisRunnerGridItem[]
    filters: Filter[]
    updateFilter: (value: string, field: string) => void
    handleSort: (clickedColumn: string) => void
    sort: { column: string | null; direction: string | null }
}

const AnalysisRunnerGrid: React.FC<IAnalysisRunnerGridProps> = ({
    data,
    filters,
    updateFilter,
    handleSort,
    sort,
}) => {
    const [openRows, setOpenRows] = React.useState<Set<string>>(new Set())

    const handleToggle = (arGuid: string) => {
        if (!openRows.has(arGuid)) {
            setOpenRows(new Set([...openRows, arGuid]))
        } else {
            setOpenRows(new Set([...openRows].filter((r) => r !== arGuid)))
        }
    }

    const checkDirection = (field: string) => {
        if (sort.column === field && sort.direction !== null) {
            return sort.direction === 'ascending' ? 'ascending' : 'descending'
        }
        return undefined
    }

    const HeaderTitles = MAIN_FIELDS.map(({ field, title }, i) => (
        <SUITable.HeaderCell
            key={`${field}-${i}`}
            sorted={checkDirection(field)}
            onClick={() => handleSort(field)}
            style={{
                borderBottom: 'none',
                position: 'sticky',
                resize: 'horizontal',
            }}
        >
            {title}
        </SUITable.HeaderCell>
    ))

    const FilterRows = (
        <>
            <SUITable.Cell
                style={{
                    borderBottom: 'none',
                    borderTop: 'none',
                    backgroundColor: 'var(--color-table-header)',
                }}
            />
            {MAIN_FIELDS.map(({ field, filterable }) => (
                <SUITable.Cell
                    key={`${field}-filter`}
                    style={{
                        borderBottom: 'none',
                        borderTop: 'none',
                        backgroundColor: 'var(--color-table-header)',
                    }}
                >
                    {filterable ? (
                        <input
                            type="text"
                            key={field}
                            id={field}
                            onChange={(e) => updateFilter(e.target.value, field)}
                            placeholder="Filter..."
                            value={
                                filters.find(({ category: Filterfield }) => Filterfield === field)
                                    ?.value ?? ''
                            }
                            style={{
                                border: 'none',
                                width: '100%',
                                borderRadius: '5px',
                                padding: '5px',
                            }}
                        />
                    ) : (
                        <></>
                    )}
                </SUITable.Cell>
            ))}
        </>
    )

    const ResizeRow = (
        <SUITable.Row>
            <SUITable.Cell
                style={{
                    borderTop: 'none',
                    backgroundColor: 'var(--color-table-header)',
                }}
            />
            {MAIN_FIELDS.map(({ field }) => (
                <SUITable.Cell
                    className="sizeRow"
                    key={`${field}-resize`}
                    style={{
                        borderTop: 'none',
                        backgroundColor: 'var(--color-table-header)',
                    }}
                ></SUITable.Cell>
            ))}
        </SUITable.Row>
    )

    return (
        <Table celled compact sortable>
            <SUITable.Header>
                <SUITable.Row>
                    <SUITable.Cell
                        style={{
                            borderBottom: 'none',
                            borderTop: 'none',
                            backgroundColor: 'var(--color-table-header)',
                        }}
                    />
                    {HeaderTitles}
                </SUITable.Row>
                <SUITable.Row>{FilterRows}</SUITable.Row>
                {ResizeRow}
            </SUITable.Header>
            <SUITable.Body>
                {data.map((log) => {
                    const isExpanded = openRows.has(log.arGuid)
                    return (
                        <React.Fragment key={`ar-big-row-${log.arGuid}`}>
                            <SUITable.Row>
                                <SUITable.Cell collapsing>
                                    <Checkbox
                                        checked={isExpanded}
                                        slider
                                        onChange={() => handleToggle(log.arGuid)}
                                    />
                                </SUITable.Cell>
                                {MAIN_FIELDS.map(({ field, renderer, width }) => (
                                    <SUITable.Cell
                                        key={field}
                                        style={{ width: `${width || 100}px` }}
                                    >
                                        {renderer?.(log, filters, updateFilter) ||
                                            sanitiseValue(_.get(log, field))}
                                    </SUITable.Cell>
                                ))}
                            </SUITable.Row>
                            {isExpanded &&
                                EXTRA_FIELDS.filter(
                                    ({ field, renderer }) => !renderer || _.get(log, field)
                                ).map(({ title, field, renderer }, i) => (
                                    <SUITable.Row key={`extra-field-${log.arGuid}-${i}`}>
                                        <SUITable.Cell style={{ border: 'none' }} />
                                        <SUITable.Cell>
                                            <b>{title}</b>
                                        </SUITable.Cell>
                                        <SUITable.Cell colSpan={MAIN_FIELDS.length - 1}>
                                            {renderer?.(log, filters, updateFilter) || (
                                                <code>{sanitiseValue(_.get(log, field))}</code>
                                            )}
                                        </SUITable.Cell>
                                    </SUITable.Row>
                                ))}
                            {isExpanded &&
                                log.meta &&
                                Object.keys(log.meta).map((key) => (
                                    <SUITable.Row key={`extra-field-${log.arGuid}-${key}`}>
                                        <SUITable.Cell style={{ border: 'none' }} />
                                        <SUITable.Cell>
                                            <b>{key}</b>
                                        </SUITable.Cell>
                                        <SUITable.Cell colSpan={MAIN_FIELDS.length - 1}>
                                            <code>{sanitiseValue(log.meta[key])}</code>
                                        </SUITable.Cell>
                                    </SUITable.Row>
                                ))}
                        </React.Fragment>
                    )
                })}
            </SUITable.Body>
        </Table>
    )
}

export default AnalysisRunnerGrid
