import * as _ from 'lodash'
import * as React from 'react'

import { useNavigate, useParams, useSearchParams } from 'react-router-dom'
import { Dropdown } from 'semantic-ui-react'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import MuckError from '../../shared/components/MuckError'
import { ProjectParticipantGridFilter, ProjectParticipantGridResponse, WebApi } from '../../sm-api'
import PageOptions from './PageOptions'
import ProjectGrid from './ProjectGrid'

interface IProjectGridContainerProps {
    projectName: string
}
const PAGE_SIZES = [20, 40, 100, 1000]


export const ProjectGridContainer: React.FunctionComponent<IProjectGridContainerProps> = ({ projectName }) => {
    const navigate = useNavigate()
    const [searchParams] = useSearchParams()
    const { page } = useParams()

    const pageSize = searchParams.get('size') || 20

    // pageSize
    const validPages = !!(page && +page && pageSize && +pageSize && PAGE_SIZES.includes(+pageSize))
    const [pageLimit, _setPageLimit] = React.useState<number>(
        validPages ? +pageSize : PAGE_SIZES[0]
    )
    const [pageNumber, _setPageNumber] = React.useState<number>(1)
    const [tokens, setTokens] = React.useState<(number | undefined)[]>([undefined])


    // fetched data
    const [numberOfEntries, setNumberOfEntries] = React.useState<number>(0)
    const [participants, setParticipants] = React.useState<ProjectParticipantGridResponse | undefined>()
    const [filterValues, _setFilterValues] = React.useState<ProjectParticipantGridFilter>({})

    const [isLoading, setIsLoading] = React.useState<boolean>(false)
    const [error, setError] = React.useState<string | undefined>()

    const setFilterValues = (values: Partial<ProjectParticipantGridFilter>) => {
        // nested merge
        _setFilterValues(_.merge({}, filterValues, values))
    }

    const getParticipantsFor = React.useCallback(
        (token: number | undefined, _pageLimit: number) => {
            if (!projectName) {
                setParticipants(undefined)
                return
            }
            const sanitisedToken = token || undefined
            setError(undefined)
            setIsLoading(true)
            return new WebApi().getProjectParticipantsGridWithLimit(
                projectName,
                _pageLimit,
                filterValues,
                sanitisedToken,
            ).then(resp => {
                setParticipants(resp.data)
                setIsLoading(false)
                const nextToken = resp.data.links?.token
                if (nextToken) {
                    setTokens([...(tokens || []), nextToken])
                }
            }).catch((er: Error) => {
                setError(er.message)
                setIsLoading(false)
            })
        },
        [projectName, pageLimit, filterValues]
    )

    const setPage = (p: number, _pageLimit: number) => {
        debugger
        const q: any = { size: _pageLimit }
        const _token = p > 1 ? tokens[p - 1] : undefined
        if (_token) {
            q.token = _token
        }
        const qq = new URLSearchParams(q).toString()
        navigate(`/project/${projectName}/?${qq}`)
        getParticipantsFor(_token, _pageLimit)

    }

    const setPageLimit = (pageLimit: number) => {
        _setPageLimit(pageLimit)
        setPage(1, pageLimit)
    }

    // load every time the project name changes
    React.useEffect(() => setPage(1, pageLimit), [projectName])

    const totalPageNumbers = Math.ceil((participants?.total_results || 0) / pageLimit)

    if (isLoading) {
        return <LoadingDucks />
    }
    if (error) {
        return <MuckError message={`Ah Muck, An error occurred when fetching project participants: ${error}`} />
    }

    const handleOnClick = console.log

    return (
        <div>
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
                    onChange={(_, data) => setPageLimit(data.value as number)}
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
                    handleOnClick={(_page) => setPage(_page, pageLimit)}
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
        </div>
    )
}
