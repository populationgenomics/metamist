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

const MAIN_FIELDS = [
    {
        category: 'Hail Batch',
        title: 'Hail Batch',
    },
    { category: 'GitHub', title: 'GitHub' },
    { category: 'Author', title: 'Author' },
    { category: 'Date', title: 'Date' },
    { category: 'script', title: 'Script' },
    { category: 'accessLevel', title: 'Access Level' },
    { category: 'Image', title: 'Driver Image' },
    { category: 'description', title: 'Description' },
    { category: 'mode', title: 'Mode' },
]

const HailBatchGrid: React.FunctionComponent<{
    data: any[]
    filters: Filter[]
    updateFilter: (value: string, category: string) => void
    handleSort: (clickedColumn: string) => void
    sort: { column: string | null; direction: string | null }
}> = ({ data, filters, updateFilter, handleSort, sort }) => {
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
                {data.map((log) => (
                    <React.Fragment key={log.id}>
                        <SUITable.Row>
                            <SUITable.Cell collapsing>
                                <Checkbox
                                    checked={openRows.includes(log.position)}
                                    slider
                                    onChange={() => handleToggle(log.position)}
                                />
                            </SUITable.Cell>
                            {MAIN_FIELDS.map(({ category }) => {
                                switch (category) {
                                    case 'Hail Batch':
                                        return (
                                            <SUITable.Cell
                                                key={category}
                                                style={{ width: '100px' }}
                                            >
                                                <a
                                                    href={`${log.batch_url}`}
                                                    rel="noopener noreferrer"
                                                    target="_blank"
                                                >
                                                    {_.get(log, category)}
                                                </a>
                                            </SUITable.Cell>
                                        )
                                    case 'GitHub':
                                        return (
                                            <SUITable.Cell
                                                key={category}
                                                style={{ width: '200px' }}
                                            >
                                                <a
                                                    href={`${`https://www.github.com/populationgenomics/${log.repo}/tree/${log.commit}`}`}
                                                    rel="noopener noreferrer"
                                                    target="_blank"
                                                >
                                                    {_.get(log, category)}
                                                </a>
                                            </SUITable.Cell>
                                        )
                                    case 'Author':
                                        return (
                                            <SUITable.Cell
                                                key={category}
                                                style={{ width: '100px' }}
                                            >
                                                <Popup
                                                    trigger={
                                                        <span
                                                            style={{
                                                                textDecoration: 'underline',
                                                                cursor: 'pointer',
                                                            }}
                                                            onClick={() => {
                                                                const author = _.get(log, category)
                                                                if (
                                                                    filters.find(
                                                                        (f) =>
                                                                            f.category === category
                                                                    )?.value === author
                                                                ) {
                                                                    updateFilter('', 'Author')
                                                                } else {
                                                                    updateFilter(
                                                                        _.get(log, category),
                                                                        'Author'
                                                                    )
                                                                }
                                                            }}
                                                        >
                                                            {_.get(log, category)}
                                                        </span>
                                                    }
                                                    hoverable
                                                    position="bottom center"
                                                >
                                                    {_.get(log, 'email')}
                                                </Popup>
                                            </SUITable.Cell>
                                        )
                                    case 'Date':
                                        return (
                                            <SUITable.Cell
                                                key={category}
                                                style={{ width: '100px' }}
                                            >
                                                <Popup
                                                    trigger={<span>{_.get(log, 'Date')}</span>}
                                                    hoverable
                                                    position="bottom center"
                                                >
                                                    {_.get(log, 'timestamp')}
                                                </Popup>
                                            </SUITable.Cell>
                                        )
                                    case 'Image':
                                    case 'mode':
                                        return (
                                            <SUITable.Cell
                                                key={category}
                                                style={{ width: '100px' }}
                                            >
                                                {_.get(log, category)}
                                            </SUITable.Cell>
                                        )

                                    case 'script':
                                        return (
                                            <SUITable.Cell key={category} className="scriptField">
                                                <code
                                                    onClick={() => handleToggle(log.position)}
                                                    style={{
                                                        cursor: 'pointer',
                                                    }}
                                                >
                                                    {sanitiseValue(_.get(log, category))}
                                                </code>
                                            </SUITable.Cell>
                                        )
                                    case 'accessLevel':
                                        return (
                                            <SUITable.Cell
                                                key={category}
                                                style={{ width: '100px' }}
                                            >
                                                <span
                                                    style={{
                                                        textDecoration: 'underline',
                                                        cursor: 'pointer',
                                                        width: '100px',
                                                    }}
                                                    onClick={() => {
                                                        if (
                                                            filters.filter(
                                                                (f) => f.category === category
                                                            ).length > 0
                                                        ) {
                                                            updateFilter('', 'accessLevel')
                                                        } else {
                                                            updateFilter(
                                                                _.get(log, category),
                                                                'accessLevel'
                                                            )
                                                        }
                                                    }}
                                                >
                                                    {_.get(log, category)}
                                                </span>
                                            </SUITable.Cell>
                                        )
                                    default:
                                        return (
                                            <SUITable.Cell
                                                key={category}
                                                style={{ width: '300px' }}
                                            >
                                                {sanitiseValue(_.get(log, category))}
                                            </SUITable.Cell>
                                        )
                                }
                            })}
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
