import * as React from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@apollo/client'
import { Dropdown } from 'semantic-ui-react'
import _ from 'lodash'
import ProjectSelector from '../ProjectSelector'
import AnalysisRunnerGrid from './AnalysisRunnerGrid'
import { gql } from '../../../__generated__/gql'
import MuckError from '../../../shared/components/MuckError'
import LoadingDucks from '../../../shared/components/LoadingDucks/LoadingDucks'
import PageOptions from '../PageOptions'
import { Filter } from './Filter'
import sanitiseValue from '../../../shared/utilities/sanitiseValue'
import parseDate from '../../../shared/utilities/parseDate'
import parseEmail from '../../../shared/utilities/parseEmail'
import parseDriverImage from '../../../shared/utilities/parseDriverImage'

const PAGE_SIZES = [20, 40, 100, 1000]

const GET_ANALYSIS_RUNNER_LOGS = gql(`
query AnalysisRunnerLogs($project_name: String!) {
    project(name: $project_name) {
        analyses(type: { eq: "analysis-runner" }, status: {eq: UNKNOWN}) {
          id
          meta
          outputs
          auditLogs {
            author
            timestamp
          }
        }
      }
  }
`)

const AnalysisRunnerSummary: React.FunctionComponent = () => {
    const navigate = useNavigate()
    const { projectName } = useParams()
    const project_name = projectName || ''
    const { loading, error, data } = useQuery(GET_ANALYSIS_RUNNER_LOGS, {
        variables: { project_name },
    })
    const [pageNumber, setPageNumber] = React.useState<number>(1)
    const [pageLimit, _setPageLimit] = React.useState<number>(PAGE_SIZES[2])
    const [filters, setFilters] = React.useState<Filter[]>([])
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: 'timestamp',
        direction: 'descending',
    })

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

    const projectSelectorOnClick = React.useCallback(
        (__, { value }) => {
            navigate(`/analysis-runner/${value}`)
        },
        [navigate]
    )

    const handleOnClick = React.useCallback((p) => {
        setPageNumber(p)
    }, [])

    const setPageLimit = React.useCallback((e: React.SyntheticEvent<HTMLElement>, { value }) => {
        _setPageLimit(parseInt(value, 10))
        setPageNumber(1)
    }, [])

    const updateFilter = (v: string, c: string) => {
        setFilters([
            ...filters.filter(({ category }) => c !== category),
            ...(v ? [{ value: v, category: c }] : []),
        ])
    }

    const flatData = data?.project.analyses.map(({ auditLogs, id, output, meta }, i) => {
        const author = _.orderBy(auditLogs, ['timestamp'])[0]?.author

        return {
            email: author,
            id,
            output,
            ...meta,
            position: i,
            'Hail Batch': meta?.batch_url
                ? meta?.batch_url.replace('https://batch.hail.populationgenomics.org.au/', '')
                : '',
            GitHub: `${meta?.repo}@${meta?.commit.substring(0, 7)}`,
            Date: `${parseDate(sanitiseValue(meta?.timestamp))}`,
            Author: `${parseEmail(sanitiseValue(author))}`,
            Image: meta?.driverImage ? `${parseDriverImage(sanitiseValue(meta?.driverImage))}` : '',
        }
    })

    return (
        <>
            <ProjectSelector onClickFunction={projectSelectorOnClick} />
            {projectName && loading && <LoadingDucks />}
            {projectName && error && <>Error! {error.message}</>}
            {projectName && !loading && !flatData?.length && (
                <MuckError
                    message={`Ah Muck, there are no analysis-runner logs for this project here`}
                />
            )}
            {!!flatData?.length && !loading && (
                <>
                    <div
                        style={{
                            marginTop: '10px',
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
                    <AnalysisRunnerGrid
                        data={(!sort.column
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
                            .slice((pageNumber - 1) * pageLimit, pageNumber * pageLimit)}
                        filters={filters}
                        updateFilter={updateFilter}
                        handleSort={handleSort}
                        sort={sort}
                    />
                    <PageOptions
                        isLoading={loading}
                        totalPageNumbers={Math.ceil(
                            (flatData.filter((log) =>
                                filters.every(
                                    ({ category, value }) => _.get(log, category) === value
                                )
                            ).length || 0) / pageLimit
                        )}
                        totalSamples={Math.ceil(
                            flatData.filter((log) =>
                                filters.every(
                                    ({ category, value }) => _.get(log, category) === value
                                )
                            ).length
                        )}
                        pageNumber={pageNumber}
                        handleOnClick={handleOnClick}
                    />
                </>
            )}
        </>
    )
}

export default AnalysisRunnerSummary
