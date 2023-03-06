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
            {projectName && summary?.participants.length !== 0 && (
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
                </>
            )}
            <ProjectGrid summary={summary} projectName={projectName} error={error} />
            <PageOptions
                isLoading={isLoading}
                totalPageNumbers={totalPageNumbers}
                totalSamples={summary?.total_samples}
                pageNumber={pageNumber}
                handleOnClick={handleOnClick}
            />
        </>
    )
}

export default ProjectSummary
