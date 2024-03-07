import * as React from 'react'
import { Table as SUITable, Popup, Checkbox } from 'semantic-ui-react'
import _ from 'lodash'
import Table from '../../../shared/components/Table'
import sanitiseValue from '../../../shared/utilities/sanitiseValue'

import { Filter } from './Filter'
import './AnalysisGrid.css'
import { access } from 'fs'


export interface AnalysisRunnerGridItem {
    arGuid: string
    timestamp: any
    accessLevel: string
    repository: string
    commit: string
    script: string
    description: string
    driverImage: string
    configPath: string
    cwd: string
    environment: string
    hailVersion: string
    batchUrl: string
    submittingUser: string
    meta: any
}

type AnalysisRunnerGridItemKeys = keyof AnalysisRunnerGridItem;


interface IMainField {
    field: AnalysisRunnerGridItemKeys
    title: string
}

const MAIN_FIELDS: IMainField[] = [
    { field: "arGuid", title: "AR GUID" },
    { field: 'batchUrl', title: 'Hail Batch' },
    { field: 'repository', title: 'GitHub' },
    { field: 'submittingUser', title: 'Author' },
    { field: 'timestamp', title: 'Date' },
    // { field: 'script', title: 'Script' },
    { field: 'accessLevel', title: 'Access Level' },
    { field: 'driverImage', title: 'Driver Image' },
    // { field: 'description', title: 'Description' },
]

const EXCLUDED_FIELDS: AnalysisRunnerGridItemKeys[] = [
    'arGuid',
    'commit',
    'batchUrl',
    'repository',
    'submittingUser',
    'timestamp',
    'environment',

]

interface IAnalysisRunnerGridProps {
    data: AnalysisRunnerGridItem[]
    filters: Filter[]
    updateFilter: (value: string, field: string) => void
    handleSort: (clickedColumn: string) => void
    sort: { column: string | null; direction: string | null }

}

interface IFieldCellProps {
    log: AnalysisRunnerGridItem
}
const FieldToCell: { [value: AnalysisRunnerGridItemKeys]: React.FunctionComponent<IFieldCellProps> } = {
    hailBatch: ({ log, ...props }: IFieldCellProps) => (
        <SUITable.Cell
            {...props}
            style={{ width: '100px' }}
        >
            <a
                href={`${log.batchUrl}`}
                rel="noopener noreferrer"
                target="_blank"
            >
                {log.batchUrl}
            </a>
        </SUITable.Cell>
    ),
    arGuid: ({ log, ...props }: IFieldCellProps) => (
        <SUITable.Cell
            {...props}
            style={{ width: '100px' }}
        >
            {log.arGuid}
        </SUITable.Cell>
    ),
    repository: ({ log, ...props }: IFieldCellProps) => (

        <SUITable.Cell
            {...props}
            style={{ width: '200px' }}
        >
            <a
                href={`${`https://www.github.com/populationgenomics/${log.repository}/tree/${log.commit}`}`}
                rel="noopener noreferrer"
                target="_blank"
            >
                {log.repository}
            </a>
        </SUITable.Cell>
    ),
    author: ({ log, ...props }: IFieldCellProps) => (
        <SUITable.Cell
            {...props}
            style={{ width: '100px' }}
        >
            <Popup
                trigger={
                    <span
                        style={{
                            textDecoration: 'underline',
                            cursor: 'pointer',
                        }}
                    // onClick={() => {
                    //     const author = _.get(log, field)
                    //     if (
                    //         filters.find(
                    //             (f) =>
                    //                 f.field === field
                    //         )?.value === author
                    //     ) {
                    //         updateFilter('', 'Author')
                    //     } else {
                    //         updateFilter(
                    //             _.get(log, field),
                    //             'Author'
                    //         )
                    //     }
                    // }}
                    >
                        {log.submittingUser}
                    </span>
                }
                hoverable
                position="bottom center"
            >
                {log.submittingUser}
            </Popup>
        </SUITable.Cell>
    ),
    date: ({ log, ...props }: IFieldCellProps) => (
        <SUITable.Cell
            {...props}
            style={{ width: '100px' }}
        >
            <Popup
                trigger={<span>{_.get(log, 'Date')}</span>}
                hoverable
                position="bottom center"
            >
                log.timestamp
            </Popup>
        </SUITable.Cell>
    ),
    // image: ({log, ...props}: IFieldCellProps) => (
    // case 'script':
    //     return (
    //         <SUITable.Cell key={field} className="scriptField">
    //             <code
    //                 onClick={() => handleToggle(log.arGuid)}
    //                 style={{
    //                     cursor: 'pointer',
    //                 }}
    //             >
    //                 {sanitiseValue(_.get(log, field))}
    //             </code>
    //         </SUITable.Cell>
    //     )
    accessLevel: ({ log, ...props }: IFieldCellProps) => (
        <SUITable.Cell
            {...props}
            style={{ width: '100px' }}
        >
            <span
                style={{
                    textDecoration: 'underline',
                    cursor: 'pointer',
                    width: '100px',
                }}
            // onClick={() => {
            //     if (
            //         filters.filter(
            //             (f) => f.field === field
            //         ).length > 0
            //     ) {
            //         updateFilter('', 'accessLevel')
            //     } else {
            //         updateFilter(
            //             _.get(log, field),
            //             'accessLevel'
            //         )
            //     }
            // }}
            >
                {log.accessLevel}
            </span>
        </SUITable.Cell>
    )
}

const AnalysisRunnerGrid: React.FC<IAnalysisRunnerGridProps> = ({ data, filters, updateFilter, handleSort, sort }) => {
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

    return (
        <Table celled compact sortable>
            <SUITable.Header>
                <SUITable.Row>
                    <SUITable.HeaderCell style={{ borderBottom: 'none' }} />
                    {MAIN_FIELDS.map(({ field, title }, i) => (
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
                    ))}
                </SUITable.Row>
                <SUITable.Row>
                    <SUITable.Cell
                        style={{
                            borderBottom: 'none',
                            borderTop: 'none',
                            backgroundColor: 'var(--color-table-header)',
                        }}
                    />
                    {MAIN_FIELDS.map(({ field }) => (
                        <SUITable.Cell
                            key={`${field}-filter`}
                            style={{
                                borderBottom: 'none',
                                borderTop: 'none',
                                backgroundColor: 'var(--color-table-header)',
                            }}
                        >
                            <input
                                type="text"
                                key={field}
                                id={field}
                                onChange={(e) => updateFilter(e.target.value, field)}
                                placeholder="Filter..."
                                value={
                                    filters.find(
                                        ({ category: Filterfield }) =>
                                            Filterfield === field
                                    )?.value ?? ''
                                }
                                style={{ border: 'none', width: '100%', borderRadius: '25px' }}
                            />
                        </SUITable.Cell>
                    ))}
                </SUITable.Row>
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
            </SUITable.Header>
            <SUITable.Body>
                {data.map((log) => (
                    <React.Fragment key={`asdasdad-{log.arGuid}`}>
                        <SUITable.Row>
                            <SUITable.Cell collapsing>
                                <Checkbox
                                    checked={openRows.has(log.arGuid)}
                                    slider
                                    onChange={() => handleToggle(log.arGuid)}
                                />
                            </SUITable.Cell>
                            {MAIN_FIELDS.map(({ field }) => {
                                const component = FieldToCell[field]
                                if (component) {
                                    const FieldComponent = FieldToCell[field]
                                    return (
                                        <FieldComponent
                                            key={`cell-{field}`}
                                            log={log}
                                        />
                                    )
                                }
                                switch (field) {

                                    default:
                                        return (
                                            <SUITable.Cell
                                                key={field}
                                                style={{ width: '300px' }}
                                            >
                                                {sanitiseValue(_.get(log, field))}
                                            </SUITable.Cell>
                                        )
                                }
                            })}
                        </SUITable.Row>
                        {Object.entries(log)
                            .filter(
                                ([c]: AnalysisRunnerGridItemKeys[]) =>
                                    (!MAIN_FIELDS.map(({ field }) => field).includes(c) ||
                                        c === 'script') &&
                                    !EXCLUDED_FIELDS.includes(c)
                            )
                            .map(([field, value], i) => (
                                <SUITable.Row
                                    style={{
                                        display: openRows.has(log.arGuid)
                                            ? 'table-row'
                                            : 'none',
                                    }}
                                    key={i}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell>
                                        <b>{_.capitalize(field)}</b>
                                    </SUITable.Cell>
                                    <SUITable.Cell colSpan={MAIN_FIELDS.length - 1}>
                                        <code>{value}</code>
                                    </SUITable.Cell>
                                </SUITable.Row>
                            ))}
                    </React.Fragment>
                ))}
            </SUITable.Body>
        </Table>
    )
}

export default AnalysisRunnerGrid
