import * as React from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'

import { Dropdown } from 'semantic-ui-react'
import ProjectSelector from './ProjectSelector'
import { WebApi, ProjectSummary, ProjectParticipantGridResponse, ProjectParticipantGridFilter } from '../../sm-api/api'

import PageOptions from './PageOptions'
import SeqrLinks from './SeqrLinks'
import MultiQCReports from './MultiQCReports'
import SummaryStatistics from './SummaryStatistics'
import BatchStatistics from './BatchStatistics'
import ProjectGrid from './ProjectGrid'
import TotalsStats from './TotalsStats'
import MuckError from '../../shared/components/MuckError'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import SeqrSync from './SeqrSync'

const PAGE_SIZES = [20, 40, 100, 1000]

const ProjectOverview: React.FunctionComponent = () => {
    const navigate = useNavigate()

    const { projectName, page } = useParams()

    const [searchParams] = useSearchParams()
    const pageSize = searchParams.get('size') || 20
    let directToken = 0
    if (page && pageSize) {
        directToken = +page * +pageSize - +pageSize
    }

    const validPages = !!(page && +page && pageSize && +pageSize && PAGE_SIZES.includes(+pageSize))

    const [summary, setSummary] = React.useState<ProjectSummary | undefined>()
    const [participants, setParticipants] = React.useState<ProjectParticipantGridResponse | undefined>()
    const [pageNumber, setPageNumber] = React.useState<number>(validPages ? +page : 1)
    const [isLoading, setIsLoading] = React.useState<boolean>(false)
    const [error, setError] = React.useState<string | undefined>()
    const [pageLimit, _setPageLimit] = React.useState<number>(
        validPages ? +pageSize : PAGE_SIZES[0]
    )
    const [filterValues, setFilterValues] = React.useState<ProjectParticipantGridFilter>({})

    const handleOnClick = React.useCallback(
        (p) => {
            navigate(`/project/${projectName}/${p}?size=${pageLimit}`)
            setPageNumber(p)
        },
        [navigate, pageLimit, projectName]
    )

    const getProjectSummary = React.useCallback(
        async () => {
            if (!projectName) {
                setSummary(undefined)
                return
            }
            setError(undefined)
            setIsLoading(true)
            try {
                const response = await new WebApi().getProjectSummary(projectName)
                setSummary(response.data)
            } catch (er: any) {
                setError(er.message)
            } finally {
                setIsLoading(false)
            }
        }, [projectName]
    )

    const getParticipants = React.useCallback(
        async (token: number) => {
            if (!projectName) {
                setSummary(undefined)
                return
            }
            const sanitisedToken = token || undefined
            setError(undefined)
            setIsLoading(true)
            try {
                const response = await new WebApi().getProjectParticipantsGridWithLimit(
                    projectName,
                    pageLimit,
                    filterValues,
                    sanitisedToken,
                )
                setParticipants(response.data)
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

    // const updateFilters = React.useCallback(
    //     (
    //         e: Record<
    //             string,
    //             { value: string; category: MetaSearchEntityPrefix; title: string; field: string }
    //         >
    //     ) => {
    //         if (!summary) return
    //         /* eslint-disable no-param-reassign */
    //         const processedFilter = Object.entries(e).reduce(
    //             (filter, [, { value, category, field }]) => {
    //                 if (!value) {
    //                     return filter
    //                 }
    //                 const is_meta = field.startsWith('meta.')
    //                 const fieldName = is_meta ? field.slice(5) : field
    //                 filter.push({ query: value, is_meta, type: category, field: fieldName })
    //                 return filter
    //             },
    //             [] as SearchItem[]
    //         )
    //         /* eslint-enable no-param-reassign */
    //         setFilterValues(processedFilter)
    //         setGridFilterValues(Object.entries(processedFilter).length ? e : {})
    //     },
    //     [summary]
    // )

    const _updateProjectSummary = React.useCallback(() => {
        getParticipants(directToken)
    }, [getParticipants, directToken])

    React.useEffect(() => {
        getProjectSummary()
    }, [projectName])

    // retrigger if project changes, or pageLimit changes

    React.useEffect(_updateProjectSummary, [
        projectName,
        pageLimit,
        pageNumber,
        _updateProjectSummary,
    ])

    const totalPageNumbers = Math.ceil((participants?.total_results || 0) / pageLimit)

    const projectSelectorOnClick = React.useCallback(
        (_, { value }) => {
            navigate(`/project/${value}`)
            setPageNumber(1)
            _setPageLimit(pageLimit)
            setFilterValues({})
            // setGridFilterValues({})
        },
        [navigate, _setPageLimit, setPageNumber, pageLimit, setFilterValues]
    )

    return (
        <>
            <ProjectSelector onClickFunction={projectSelectorOnClick} />
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
            {projectName &&
                !error &&
                !isLoading &&
                participants?.participants?.length === 0 &&
                !Object.keys(filterValues).length && (
                    <MuckError message={`Ah Muck, there aren't any participants in this project`} />
                )}
            {projectName &&
                !error &&
                !isLoading &&
                summary &&
                !!(summary || Object.keys(filterValues).length) && (
                    <>
                        <TotalsStats summary={summary ?? {}} />
                        <SummaryStatistics
                            projectName={projectName}
                            cramSeqrStats={summary?.cram_seqr_stats ?? {}}
                        />
                        <BatchStatistics
                            projectName={projectName}
                            cramSeqrStats={summary?.cram_seqr_stats ?? {}}
                            batchSequenceStats={summary?.batch_sequencing_group_stats ?? {}}
                        />
                        <hr />
                        <MultiQCReports projectName={projectName} />
                        <SeqrLinks seqrLinks={summary?.seqr_links ?? {}} />
                        <SeqrSync syncTypes={summary?.seqr_sync_types} project={projectName} />
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
                                    text: `${s} participants`,
                                    value: s,
                                }))}
                            />
                            <PageOptions
                                isLoading={isLoading}
                                totalPageNumbers={totalPageNumbers}
                                total={participants?.total_results}
                                pageNumber={pageNumber}
                                handleOnClick={handleOnClick}
                                title="participants"
                            />
                        </div>
                        <div style={{ position: 'absolute', paddingBottom: '80px' }}>
                            <ProjectGrid
                                participantResponse={participants}
                                projectName={projectName}
                                updateFilters={console.log}
                                filterValues={{}}
                            />
                            <PageOptions
                                isLoading={isLoading}
                                totalPageNumbers={totalPageNumbers}
                                total={participants?.total_results}
                                pageNumber={pageNumber}
                                handleOnClick={handleOnClick}
                                title="participants"
                            />
                        </div>
                    </>
                )}
        </>
    )
}

export default ProjectOverview
