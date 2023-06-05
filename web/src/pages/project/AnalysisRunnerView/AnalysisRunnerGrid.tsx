import * as React from 'react'
import { Table as SUITable, Popup, Checkbox } from 'semantic-ui-react'
import _ from 'lodash'
import Table from '../../../shared/components/Table'
import sanitiseValue from '../../../shared/utilities/sanitiseValue'
import parseEmail from '../../../shared/utilities/parseEmail'
import parseDate from '../../../shared/utilities/parseDate'
import parseDriverImage from '../../../shared/utilities/parseDriverImage'
import parseScript from '../../../shared/utilities/parseScript'
import { Filter } from './Filter'

const EXCLUDED_FIELDS = ['id', 'commit', 'source', 'position', 'batch_url', 'repo']

const MAIN_FIELDS = [
    {
        category: 'Hail Batch',
        title: 'Hail Batch',
    },
    { category: 'GitHub', title: 'GitHub' },
    { category: 'author', title: 'Author' },
    { category: 'timestamp', title: 'Date' },
    { category: 'script', title: 'Script' },
    { category: 'accessLevel', title: 'Access Level' },
    { category: 'driverImage', title: 'Driver Image' },
    { category: 'description', title: 'Description' },
]

const AnalysisRunnerGrid: React.FunctionComponent<{
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
        <>
            <Table celled compact sortable>
                <SUITable.Header>
                    <SUITable.Row>
                        <SUITable.HeaderCell style={{ borderBottom: 'none' }} />
                        {MAIN_FIELDS.map(({ category, title }, i) => (
                            <SUITable.HeaderCell
                                key={`${category}-${i}`}
                                sorted={checkDirection(category)}
                                onClick={() => handleSort(category)}
                                style={{ borderBottom: 'none' }}
                            >
                                {title}
                            </SUITable.HeaderCell>
                        ))}
                    </SUITable.Row>
                    <SUITable.Row>
                        <SUITable.HeaderCell />
                        {MAIN_FIELDS.map(({ category }) => (
                            <SUITable.HeaderCell key={`${category}-filter`}>
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
                            </SUITable.HeaderCell>
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
                                                <SUITable.Cell key={category}>
                                                    <a
                                                        href={`${log.batch_url}`}
                                                        rel="noopener noreferrer"
                                                        target="_blank"
                                                    >
                                                        {log.batch_url.replace(
                                                            'https://batch.hail.populationgenomics.org.au/',
                                                            ''
                                                        )}
                                                    </a>
                                                </SUITable.Cell>
                                            )
                                        case 'GitHub':
                                            return (
                                                <SUITable.Cell key={category}>
                                                    <a
                                                        href={`${`https://www.github.com/populationgenomics/${log.repo}/tree/${log.commit}`}`}
                                                        rel="noopener noreferrer"
                                                        target="_blank"
                                                    >
                                                        {log.repo}@{log.commit.substring(0, 7)}
                                                    </a>
                                                </SUITable.Cell>
                                            )
                                        case 'author':
                                            return (
                                                <SUITable.Cell key={category}>
                                                    <Popup
                                                        trigger={
                                                            <span
                                                                style={{
                                                                    textDecoration: 'underline',
                                                                    cursor: 'pointer',
                                                                }}
                                                                onClick={() => {
                                                                    if (
                                                                        filters.filter(
                                                                            (f) =>
                                                                                f.category ===
                                                                                category
                                                                        ).length > 0
                                                                    ) {
                                                                        updateFilter('', 'author')
                                                                    } else {
                                                                        updateFilter(
                                                                            _.get(log, category),
                                                                            'author'
                                                                        )
                                                                    }
                                                                }}
                                                            >
                                                                {parseEmail(
                                                                    sanitiseValue(
                                                                        _.get(log, category)
                                                                    )
                                                                )}
                                                            </span>
                                                        }
                                                        hoverable
                                                        position="bottom center"
                                                    >
                                                        {sanitiseValue(_.get(log, category))}
                                                    </Popup>
                                                </SUITable.Cell>
                                            )
                                        case 'timestamp':
                                            return (
                                                <SUITable.Cell key={category}>
                                                    <Popup
                                                        trigger={
                                                            <span>
                                                                {parseDate(
                                                                    sanitiseValue(
                                                                        _.get(log, category)
                                                                    )
                                                                )}
                                                            </span>
                                                        }
                                                        hoverable
                                                        position="bottom center"
                                                    >
                                                        {sanitiseValue(_.get(log, category))}
                                                    </Popup>
                                                </SUITable.Cell>
                                            )
                                        case 'driverImage':
                                            return (
                                                <SUITable.Cell key={category}>
                                                    {parseDriverImage(
                                                        sanitiseValue(_.get(log, category))
                                                    )}
                                                </SUITable.Cell>
                                            )
                                        case 'script':
                                            return (
                                                <SUITable.Cell key={category}>
                                                    <code
                                                        onClick={() => handleToggle(log.position)}
                                                        style={{ cursor: 'pointer' }}
                                                    >
                                                        {parseScript(
                                                            sanitiseValue(_.get(log, category))
                                                        )}
                                                    </code>
                                                </SUITable.Cell>
                                            )
                                        case 'accessLevel':
                                            return (
                                                <SUITable.Cell key={category}>
                                                    <span
                                                        style={{
                                                            textDecoration: 'underline',
                                                            cursor: 'pointer',
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
                                                        {parseEmail(
                                                            sanitiseValue(_.get(log, category))
                                                        )}
                                                    </span>
                                                </SUITable.Cell>
                                            )
                                        default:
                                            return (
                                                <SUITable.Cell key={category}>
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
        </>
    )
}

export default AnalysisRunnerGrid
