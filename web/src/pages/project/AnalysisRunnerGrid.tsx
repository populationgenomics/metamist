import * as React from 'react'
import { useQuery } from '@apollo/client'
import { Table as SUITable, Popup, Checkbox, Dropdown } from 'semantic-ui-react'
import _ from 'lodash'
import { gql } from '../../__generated__/gql'
import MuckError from '../../shared/components/MuckError'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import Table from '../../shared/components/Table'
import sanitiseValue from '../../shared/utilities/sanitiseValue'
import parseEmail from '../../shared/utilities/parseEmail'
import parseDate from '../../shared/utilities/parseDate'
import parseDriverImage from '../../shared/utilities/parseDriverImage'
import parseScript from '../../shared/utilities/parseScript'
import PageOptions from './PageOptions'

const PAGE_SIZES = [20, 40, 100, 1000]

const GET_ANALYSIS_RUNNER_LOGS = gql(`
query AnalysisRunnerLogs($project_name: String!) {
    project(name: $project_name) {
        analyses(type: ANALYSIS_RUNNER) {
          author
          id
          meta
          output
        }
      }
  }
`)

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

type Filter = {
    category: string
    value: string
}

const AnalysisRunnerGrid: React.FunctionComponent<{ projectName: string }> = ({ projectName }) => {
    const { loading, error, data } = useQuery(GET_ANALYSIS_RUNNER_LOGS, {
        variables: { project_name: projectName },
    })
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: 'timestamp',
        direction: 'descending',
    })
    const [filters, setFilters] = React.useState<Filter[]>([])
    const [openRows, setOpenRows] = React.useState<number[]>([])
    const [pageNumber, setPageNumber] = React.useState<number>(1)
    const [pageLimit, _setPageLimit] = React.useState<number>(PAGE_SIZES[2])

    const handleOnClick = React.useCallback((p) => {
        setPageNumber(p)
    }, [])

    const setPageLimit = React.useCallback((e: React.SyntheticEvent<HTMLElement>, { value }) => {
        _setPageLimit(parseInt(value, 10))
        setPageNumber(1)
    }, [])

    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>
    if (!data?.project.analyses.length)
        return (
            <MuckError
                message={`Ah Muck, there are no analysis-runner logs for this project here`}
            />
        )

    const handleToggle = (position: number) => {
        if (!openRows.includes(position)) {
            setOpenRows([...openRows, position])
        } else {
            setOpenRows(openRows.filter((i) => i !== position))
        }
    }

    const handleSort = (clickedColumn: string) => {
        if (sort.column !== clickedColumn) {
            setSort({ column: clickedColumn, direction: 'ascending' })
            return
        }
        if (sort.direction === 'ascending') {
            setSort({ column: clickedColumn, direction: 'descending' })
            return
        }
        setSort({ column: null, direction: null })
    }

    const checkDirection = (category: string) => {
        if (sort.column === category && sort.direction !== null) {
            return sort.direction === 'ascending' ? 'ascending' : 'descending'
        }
        return undefined
    }

    const flatData = data.project.analyses.map(({ author, id, output, meta }, i) => ({
        author,
        id,
        output,
        ...meta,
        position: i,
    }))

    const updateFilter = (v: string, c: string) => {
        setFilters([
            ...filters.filter(({ category }) => c !== category),
            ...(v ? [{ value: v, category: c }] : []),
        ])
    }

    return (
        <>
            <div
                style={{
                    marginBottom: '10px',
                    justifyContent: 'flex-end',
                    display: 'flex',
                    flexDirection: 'row',
                }}
            >
                <Dropdown
                    selection
                    onChange={setPageLimit}
                    value={pageLimit}
                    options={PAGE_SIZES.map((s) => ({
                        key: s,
                        text: `${s} samples`,
                        value: s,
                    }))}
                />
                <PageOptions
                    isLoading={loading}
                    totalPageNumbers={Math.ceil(
                        (flatData.filter((log) =>
                            filters.every(({ category, value }) =>
                                _.get(log, category, '').includes(value)
                            )
                        ).length || 0) / pageLimit
                    )}
                    totalSamples={Math.ceil(
                        flatData.filter((log) =>
                            filters.every(({ category, value }) =>
                                _.get(log, category, '').includes(value)
                            )
                        ).length
                    )}
                    pageNumber={pageNumber}
                    handleOnClick={handleOnClick}
                    title={'logs'}
                />
            </div>
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
                                    id={`${category}`}
                                    onChange={(e) => updateFilter(e.target.value, category)}
                                    placeholder="Filter..."
                                    style={{ border: 'none', width: '100%', borderRadius: '25px' }}
                                />
                            </SUITable.HeaderCell>
                        ))}
                    </SUITable.Row>
                </SUITable.Header>
                <SUITable.Body>
                    {(!sort.column
                        ? flatData
                        : _.orderBy(
                              flatData,
                              [sort.column],
                              sort.direction === 'ascending' ? ['asc'] : ['desc']
                          )
                    )
                        .filter((log) =>
                            filters.every(({ category, value }) =>
                                _.get(log, category, '').includes(value)
                            )
                        )
                        .slice((pageNumber - 1) * pageLimit, pageNumber * pageLimit)
                        .map((log) => (
                            <>
                                <SUITable.Row key={`${log.id}`}>
                                    <SUITable.Cell collapsing>
                                        <Checkbox
                                            defaultChecked={openRows.includes(log.position)}
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
                                                    <SUITable.Cell key={`${category}`}>
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
                                                                            setFilters(
                                                                                filters.filter(
                                                                                    (f) =>
                                                                                        f.category !==
                                                                                        category
                                                                                )
                                                                            )
                                                                        } else {
                                                                            setFilters([
                                                                                ...filters,
                                                                                {
                                                                                    category,
                                                                                    value: _.get(
                                                                                        log,
                                                                                        category
                                                                                    ),
                                                                                },
                                                                            ])
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
                                                    <SUITable.Cell key={`${category}`}>
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
                                                    <SUITable.Cell key={`${category}`}>
                                                        {parseDriverImage(
                                                            sanitiseValue(_.get(log, category))
                                                        )}
                                                    </SUITable.Cell>
                                                )
                                            case 'script':
                                                return (
                                                    <SUITable.Cell key={`${category}`}>
                                                        <code>
                                                            {parseScript(
                                                                sanitiseValue(_.get(log, category))
                                                            )}
                                                        </code>
                                                    </SUITable.Cell>
                                                )
                                            default:
                                                return (
                                                    <SUITable.Cell key={`${category}`}>
                                                        {sanitiseValue(_.get(log, category))}
                                                    </SUITable.Cell>
                                                )
                                        }
                                    })}
                                </SUITable.Row>
                                {Object.entries(log)
                                    .filter(
                                        ([c]) =>
                                            (!MAIN_FIELDS.map(({ category }) => category).includes(
                                                c
                                            ) ||
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
                            </>
                        ))}
                </SUITable.Body>
            </Table>
            <PageOptions
                isLoading={loading}
                totalPageNumbers={Math.ceil(
                    (flatData.filter((log) =>
                        filters.every(({ category, value }) => _.get(log, category) === value)
                    ).length || 0) / pageLimit
                )}
                totalSamples={Math.ceil(
                    flatData.filter((log) =>
                        filters.every(({ category, value }) => _.get(log, category) === value)
                    ).length
                )}
                pageNumber={pageNumber}
                handleOnClick={handleOnClick}
            />
        </>
    )
}

export default AnalysisRunnerGrid
