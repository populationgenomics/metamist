import * as React from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@apollo/client'
import { Dropdown } from 'semantic-ui-react'
import _ from 'lodash'
import ProjectSelector from '../ProjectSelector'
import AnalysisRunnerGrid, { AnalysisRunnerGridItem } from './AnalysisRunnerGrid'
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
        analysisRunner {
            arGuid
            timestamp
            accessLevel
            repository
            commit
            script
            description
            driverImage
            configPath
            cwd
            environment
            hailVersion
            batchUrl
            submittingUser
            meta
            outputPath
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

    const flatData: AnalysisRunnerGridItem[] | undefined = data?.project.analysisRunner.map(
        (arLog, i) => {
            // author is technically the "sample-metadata-pubsub" service, so we'll
            // which is the user who triggered the analysis

            return {
                email: arLog.submittingUser,
                arGuid: arLog.arGuid,
                outputPath: arLog.outputPath,
                position: i,
                batchUrl: arLog.batchUrl,
                repository: arLog.repository,
                commit: arLog.commit,
                timestamp: new Date(sanitiseValue(arLog?.timestamp)),
                submittingUser: arLog.submittingUser,
                driverImage: arLog.driverImage,
                description: arLog.description,
                script: arLog.script,
                configPath: arLog.configPath,
                cwd: arLog.cwd,
                environment: arLog.environment,
                hailVersion: arLog.hailVersion,
                meta: arLog.meta,
                accessLevel: arLog.accessLevel,
            } as AnalysisRunnerGridItem
        }
    )

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
                                text: `${s} records`,
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
                            total={Math.ceil(
                                flatData.filter((log) =>
                                    filters.every(({ category, value }) =>
                                        _.get(log, category, '').includes(value)
                                    )
                                ).length
                            )}
                            pageNumber={pageNumber}
                            handleOnClick={handleOnClick}
                            title="records"
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
                        total={Math.ceil(
                            flatData.filter((log) =>
                                filters.every(
                                    ({ category, value }) => _.get(log, category) === value
                                )
                            ).length
                        )}
                        title="records"
                        pageNumber={pageNumber}
                        handleOnClick={handleOnClick}
                    />
                </>
            )}
        </>
    )
}

export default AnalysisRunnerSummary
