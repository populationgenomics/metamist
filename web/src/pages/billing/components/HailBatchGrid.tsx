import * as React from 'react'
import { Table as SUITable, Popup, Checkbox } from 'semantic-ui-react'
import _ from 'lodash'
import Table from '../../../shared/components/Table'
import sanitiseValue from '../../../shared/utilities/sanitiseValue'

import { Filter } from '../../project/AnalysisRunnerView/Filter'
import '../../project/AnalysisRunnerView/AnalysisGrid.css'

const EXCLUDED_FIELDS = [
    'id',
    'commit',
    'source',
    'position',
    'batch_url',
    'repo',
    'email',
    'timestamp',
]

interface Field {
    category: string
    title: string
    width?: string
    className?: string
    dataMap?: (data: any, value: string) => any
}

const HailBatchGrid: React.FunctionComponent<{
    data: any[]
    filters: Filter[]
    updateFilter: (value: string, category: string) => void
    handleSort: (clickedColumn: string) => void
    sort: { column: string | null; direction: string | null }
    idColumn?: string
}> = ({ data, filters, updateFilter, handleSort, sort, idColumn }) => {
    console.log(data)

    const [openRows, setOpenRows] = React.useState<number[]>([])

    const handleToggle = (position: number) => {
        if (!openRows.includes(position)) {
            setOpenRows([...openRows, position])
        } else {
            setOpenRows(openRows.filter((i) => i !== position))
        }
    }

    const checkDirection = (category: string) => {
        if (sort.column === category && sort.direction !== null) {
            return sort.direction === 'ascending' ? 'ascending' : 'descending'
        }
        return undefined
    }

    const MAIN_FIELDS: Field[] = [
        {
            category: 'Hail Batch',
            title: 'Hail Batch',
            dataMap: (data: any, value: string) => (
                <a href={`${data.batch_url}`} rel="noopener noreferrer" target="_blank">
                    {value}
                </a>
            ),
        },
        {
            category: 'GitHub',
            title: 'GitHub',
            width: '200px',
            dataMap: (data: any, value: string) => (
                <a
                    href={`${`https://www.github.com/populationgenomics/${data.repo}/tree/${data.commit}`}`}
                    rel="noopener noreferrer"
                    target="_blank"
                >
                    {value}
                </a>
            ),
        },
        {
            category: 'Author',
            title: 'Author',
            dataMap: (data: any, value: string) => (
                <Popup
                    trigger={
                        <span
                            style={{
                                textDecoration: 'underline',
                                cursor: 'pointer',
                            }}
                            onClick={() => {
                                const author = value
                                if (
                                    filters.find((f) => f.category === 'Author')?.value === author
                                ) {
                                    updateFilter('', 'Author')
                                } else {
                                    updateFilter(value, 'Author')
                                }
                            }}
                        >
                            {value}
                        </span>
                    }
                    hoverable
                    position="bottom center"
                >
                    {data['email']}
                </Popup>
            ),
        },
        {
            category: 'Date',
            title: 'Date',
            dataMap: (data: any, value: string) => (
                <Popup trigger={<span>{value}</span>} hoverable position="bottom center">
                    {data['timestamp']}
                </Popup>
            ),
        },
        {
            category: 'script',
            title: 'Script',
            className: 'scriptField',
            dataMap: (data: any, value: string) => (
                <code
                    onClick={() => handleToggle(data.position)}
                    style={{
                        cursor: 'pointer',
                    }}
                >
                    {sanitiseValue(value)}
                </code>
            ),
        },
        {
            category: 'accessLevel',
            title: 'Access Level',
            dataMap: (data: any, value: string) => (
                <span
                    style={{
                        textDecoration: 'underline',
                        cursor: 'pointer',
                        width: '100px',
                    }}
                    onClick={() => {
                        if (filters.filter((f) => f.category === 'accessLevel').length > 0) {
                            updateFilter('', 'accessLevel')
                        } else {
                            updateFilter(value, 'accessLevel')
                        }
                    }}
                >
                    {value}
                </span>
            ),
        },
        { category: 'Image', title: 'Driver Image' },
        { category: 'description', title: 'Description' },
        { category: 'mode', title: 'Mode' },
    ]

    const expandedRow = (log: any) =>
        MAIN_FIELDS.map(({ category, title, width, dataMap, className }) => {
            ;<SUITable.Cell
                key={category}
                style={{ width: width ?? '100px' }}
                className={className}
            >
                {dataMap ? dataMap(log, log[category]) : sanitiseValue(log[category])}
            </SUITable.Cell>
        })

    return (
        <Table celled compact sortable>
            <SUITable.Header>
                <SUITable.Row>
                    <SUITable.HeaderCell style={{ borderBottom: 'none' }} />
                    {MAIN_FIELDS.map(({ category, title }, i) => (
                        <SUITable.HeaderCell
                            key={`${category}-${i}`}
                            sorted={checkDirection(category)}
                            onClick={() => handleSort(category)}
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
                    {MAIN_FIELDS.map(({ category }) => (
                        <SUITable.Cell
                            key={`${category}-filter`}
                            style={{
                                borderBottom: 'none',
                                borderTop: 'none',
                                backgroundColor: 'var(--color-table-header)',
                            }}
                        >
                            <input
                                type="text"
                                key={category}
                                id={category}
                                onChange={(e) => updateFilter(e.target.value, category)}
                                placeholder="Filter..."
                                value={
                                    filters.find(
                                        ({ category: FilterCategory }) =>
                                            FilterCategory === category
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
                    {MAIN_FIELDS.map(({ category }) => (
                        <SUITable.Cell
                            className="sizeRow"
                            key={`${category}-resize`}
                            style={{
                                borderTop: 'none',
                                backgroundColor: 'var(--color-table-header)',
                            }}
                        ></SUITable.Cell>
                    ))}
                </SUITable.Row>
            </SUITable.Header>
            <SUITable.Body>
                {data.map((log, idx) => (
                    <React.Fragment key={idColumn ? log[idColumn] : idx}>
                        <SUITable.Row>
                            <SUITable.Cell collapsing>
                                <Checkbox
                                    checked={openRows.includes(log.position)}
                                    slider
                                    onChange={() => handleToggle(log.position)}
                                />
                            </SUITable.Cell>
                            {expandedRow(log)}
                        </SUITable.Row>
                        {Object.entries(log)
                            .filter(
                                ([c]) =>
                                    (!MAIN_FIELDS.map(({ category }) => category).includes(c) ||
                                        c === 'script') &&
                                    !EXCLUDED_FIELDS.includes(c)
                            )
                            .map(([category, value], i) => (
                                <SUITable.Row
                                    style={{
                                        display: openRows.includes(log.position)
                                            ? 'table-row'
                                            : 'none',
                                    }}
                                    key={i}
                                >
                                    <SUITable.Cell style={{ border: 'none' }} />
                                    <SUITable.Cell>
                                        <b>{_.capitalize(category)}</b>
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

export default HailBatchGrid
