import * as React from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'

import { Dropdown } from 'semantic-ui-react'
import ProjectSelector from './ProjectSelector'
import { WebApi, ProjectSummaryResponse } from '../../sm-api/api'

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
    const [filterValues, setFilterValues] = React.useState<
        Record<string, Record<string, Record<string, string>[]>>
    >({})
    const [gridFilterValues, setGridFilterValues] = React.useState<Record<string, string>>({})

    const handleOnClick = React.useCallback(
        (p) => {
            navigate(`/project/${projectName}/${p}?size=${pageLimit}`)
            setPageNumber(p)
        },
        [navigate, pageLimit, projectName]
    )

    const getProjectSummary = React.useCallback(
        async (token: any) => {
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
                    // filterValues,
                    { test: 'value' },
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
        [projectName, pageLimit]
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
        (e) => {
            if (!summary) return
            const processedFilter = Object.entries(e).reduce((filter, [column, v]) => {
                const value = JSON.stringify(v)
                const participantIndex = summary.participant_keys.findIndex((i) => i[1] === column)
                const sampleIndex = summary.sample_keys.findIndex((i) => i[1] === column)
                const sequenceIndex = summary.sequence_keys.findIndex(
                    (i) => `sequence.${i[1]}` === column
                )
                const { category, field } = (participantIndex > 0 && {
                    category: 'participant',
                    field: summary.participant_keys[participantIndex][0],
                }) ||
                    (sampleIndex > 0 && {
                        category: 'sample',
                        field: summary.sample_keys[sampleIndex][0],
                    }) ||
                    (sequenceIndex > 0 && {
                        category: 'sequence',
                        field: summary.sequence_keys[sequenceIndex][0],
                    }) || { category: 'family', field: 'family_ID' }
                const metaKey = field.startsWith('meta.') ? 'meta' : 'non-meta'
                const categoryName = filter[category] || {}
                const metaGroup = categoryName[metaKey] || []
                metaGroup.push({ field, value })
                categoryName[metaKey] = metaGroup
                filter[category] = categoryName
                return filter
            }, {} as Record<string, Record<string, Record<string, string>[]>>)
            setFilterValues(processedFilter)
            setGridFilterValues(e)
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

    const totalPageNumbers = Math.ceil((summary?.total_samples || 0) / pageLimit)

    return (
        <>
            <ProjectSelector
                setPageLimit={_setPageLimit}
                setPageNumber={setPageNumber}
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
            {!isLoading && summary && summary.participants.length === 0 && (
                <MuckError message={`Ah Muck, there aren't any samples in this project`} />
            )}
            {projectName &&
                !error &&
                !isLoading &&
                summary &&
                summary?.participants.length !== 0 && (
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
                                totalSamples={summary?.total_samples}
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
                            totalSamples={summary?.total_samples}
                            pageNumber={pageNumber}
                            handleOnClick={handleOnClick}
                        />
                    </>
                )}
        </>
    )
}

export default ProjectSummary
