import * as React from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'

import { Dropdown } from 'semantic-ui-react'
import ProjectSelector from './ProjectSelector'
import {
    WebApi,
    ProjectSummaryResponse,
    SearchItem,
    MetaSearchEntityPrefix,
} from '../../sm-api/api'

import PageOptions from './PageOptions'
import SeqrLinks from './SeqrLinks'
import MultiQCReports from './MultiQCReports'
import SummaryStatistics from './SummaryStatistics'
import BatchStatistics from './BatchStatistics'
import ProjectGrid from './ProjectGrid'
import TotalsStats from './TotalsStats'
import MuckError from '../../shared/components/MuckError'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'

const PAGE_SIZES = [20, 40, 100, 1000]

/* eslint-disable consistent-return */
const getPrefix = (model_type: string) => {
    switch (model_type) {
        case 'participant':
            return MetaSearchEntityPrefix.P
        case 'sequence':
            return MetaSearchEntityPrefix.Sq
        case 'family':
            return MetaSearchEntityPrefix.F
        case 'sample':
            return MetaSearchEntityPrefix.S
        // no default
    }
}

const ProjectSummary: React.FunctionComponent = () => {
    const navigate = useNavigate()

    const { projectName, page } = useParams()

    const [searchParams] = useSearchParams()
    const pageSize = searchParams.get('size') || 20
    let directToken = 0
    if (page && pageSize) {
        directToken = +page * +pageSize - +pageSize
    }

    const validPages = !!(page && +page && pageSize && +pageSize && PAGE_SIZES.includes(+pageSize))

    const [summary, setSummary] = React.useState<ProjectSummaryResponse | undefined>()
    const [pageNumber, setPageNumber] = React.useState<number>(validPages ? +page : 1)
    const [isLoading, setIsLoading] = React.useState<boolean>(false)
    const [error, setError] = React.useState<string | undefined>()
    const [pageLimit, _setPageLimit] = React.useState<number>(
        validPages ? +pageSize : PAGE_SIZES[0]
    )
    const [filterValues, setFilterValues] = React.useState<SearchItem[]>([])
    const [gridFilterValues, setGridFilterValues] = React.useState<
        Record<string, { value: string; category: string; title: string }>
    >({})

    const handleOnClick = React.useCallback(
        (p) => {
            navigate(`/project/${projectName}/${p}?size=${pageLimit}`)
            setPageNumber(p)
        },
        [navigate, pageLimit, projectName]
    )

    const getProjectSummary = React.useCallback(
        async (token: number) => {
            if (!projectName) {
                setSummary(undefined)
                return
            }
            const sanitisedToken = token || undefined
            setError(undefined)
            setIsLoading(true)
            try {
                const response = await new WebApi().getProjectSummary(
                    projectName,
                    filterValues,
                    pageLimit,
                    sanitisedToken
                )
                setSummary(response.data)
            } catch (er: any) {
                setError(er.message)
            } finally {
                setIsLoading(false)
            }
        },
        [projectName, pageLimit, filterValues]
    )

    const setPageLimit = React.useCallback(
        (e: React.SyntheticEvent<HTMLElement>, { value }) => {
            navigate(`/project/${projectName}/1?size=${parseInt(value, 10)}`)
            _setPageLimit(parseInt(value, 10))
            setPageNumber(1)
        },
        [projectName, navigate]
    )

    const updateFilters = React.useCallback(
        (e: Record<string, { value: string; category: string; title: string }>) => {
            if (!summary) return
            /* eslint-disable no-param-reassign */
            const processedFilter = Object.entries(e).reduce(
                (filter, [column, { value, category }]) => {
                    if (!value) {
                        return filter
                    }
                    const is_meta = column.startsWith('meta.')
                    const field = is_meta ? column.slice(5) : column
                    const model_type = getPrefix(category)
                    if (!model_type) {
                        throw new TypeError('Invalid Model Type')
                    }
                    filter.push({ query: value, is_meta, model_type, field })
                    return filter
                },
                [] as SearchItem[]
            )
            /* eslint-enable no-param-reassign */
            setFilterValues(processedFilter)
            setGridFilterValues(Object.entries(processedFilter).length ? e : {})
        },
        [summary]
    )

    const _updateProjectSummary = React.useCallback(() => {
        getProjectSummary(directToken)
    }, [getProjectSummary, directToken])

    // retrigger if project changes, or pageLimit changes
    React.useEffect(_updateProjectSummary, [
        projectName,
        pageLimit,
        pageNumber,
        _updateProjectSummary,
    ])

    const totalPageNumbers = Math.ceil((summary?.total_samples_in_query || 0) / pageLimit)

    return (
        <>
            <ProjectSelector
                setPageLimit={_setPageLimit}
                setPageNumber={setPageNumber}
                setFilterValues={setFilterValues}
                setGridFilterValues={setGridFilterValues}
                pageLimit={PAGE_SIZES[0]}
            />
            <hr />
            {error && (
                <MuckError message={`Ah Muck, An error occurred when fetching samples: ${error}`} />
            )}
            {isLoading && <LoadingDucks />}
            {!projectName && (
                <p>
                    <em>Please select a project</em>
                </p>
            )}
            {!isLoading &&
                summary &&
                summary.participants.length === 0 &&
                !Object.keys(filterValues).length && (
                    <MuckError message={`Ah Muck, there aren't any samples in this project`} />
                )}
            {projectName && !error && !isLoading && summary && (
                <>
                    <TotalsStats summary={summary ?? {}} />
                    <SummaryStatistics
                        projectName={projectName}
                        cramSeqrStats={summary?.cram_seqr_stats ?? {}}
                    />
                    <BatchStatistics
                        projectName={projectName}
                        cramSeqrStats={summary?.cram_seqr_stats ?? {}}
                        batchSequenceStats={summary?.batch_sequence_stats ?? {}}
                    />
                    <hr />
                    <MultiQCReports projectName={projectName} />
                    <SeqrLinks seqrLinks={summary?.seqr_links ?? {}} />
                    <hr />
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
                            isLoading={isLoading}
                            totalPageNumbers={totalPageNumbers}
                            totalSamples={summary?.total_samples_in_query}
                            pageNumber={pageNumber}
                            handleOnClick={handleOnClick}
                        />
                    </div>
                    <ProjectGrid
                        summary={summary}
                        projectName={projectName}
                        updateFilters={updateFilters}
                        filterValues={gridFilterValues}
                    />
                    <PageOptions
                        isLoading={isLoading}
                        totalPageNumbers={totalPageNumbers}
                        totalSamples={summary?.total_samples_in_query}
                        pageNumber={pageNumber}
                        handleOnClick={handleOnClick}
                    />
                </>
            )}
        </>
    )
}

export default ProjectSummary
