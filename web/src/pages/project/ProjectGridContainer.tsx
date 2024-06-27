import * as React from 'react'

import { AxiosError } from 'axios'
import { useNavigate, useParams, useSearchParams } from 'react-router-dom'
import { Button, Dropdown, Message } from 'semantic-ui-react'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import MuckError from '../../shared/components/MuckError'
import ErrorBoundary from '../../shared/utilities/errorBoundary'
import {
    MetaSearchEntityPrefix,
    ProjectParticipantGridField,
    ProjectParticipantGridFilter,
    ProjectParticipantGridResponse,
    WebApi,
} from '../../sm-api'
import { DictEditor } from './DictEditor'
import PageOptions from './PageOptions'
import { defaultHeaderGroupsFromResponse, ProjectColumnOptions } from './ProjectColumnOptions'
import ProjectGrid from './ProjectGrid'

interface IProjectGridContainerProps {
    projectName: string
}
const PAGE_SIZES = [20, 40, 100, 1000]

interface IPageChangeOptions {
    pageNumber?: number
    pageSize?: number
    filter?: ProjectParticipantGridFilter
}

export const ProjectGridContainer: React.FunctionComponent<IProjectGridContainerProps> = ({
    projectName,
}) => {
    const navigate = useNavigate()
    const [searchParams] = useSearchParams()

    const [isLoading, setIsLoading] = React.useState<boolean>(false)
    let [error, setError] = React.useState<React.ReactElement | undefined>()
    const [projectColOptionsAreOpen, setProjectColOptionsAreOpen] = React.useState<boolean>(false)
    const [headerGroups, setHeaderGroups] = React.useState<
        Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]>
    >({} as Record<MetaSearchEntityPrefix, ProjectParticipantGridField[]>)

    // fetched data
    const [participants, setParticipants] = React.useState<
        ProjectParticipantGridResponse | undefined
    >()

    const hasOptionErrors: Partial<
        Record<keyof IPageChangeOptions, { error: string; default: any; value: any }>
    > = {}

    const { page } = useParams()

    // parse pageSize
    const _pageSizeFromSearch = searchParams.get('size')
    let pageSize = parseInt(_pageSizeFromSearch || '20')
    if (isNaN(pageSize)) {
        hasOptionErrors.pageSize = {
            error: `Invalid page size ${_pageSizeFromSearch}`,
            default: 20,
            value: pageSize,
        }
    }
    // parse pageNumber
    let pageNumber = parseInt(page || '1')
    if (isNaN(pageNumber)) {
        hasOptionErrors.pageNumber = {
            error: `Invalid page number ${pageNumber}`,
            default: 1,
            value: pageNumber,
        }
    }

    const setPageOptions: (options: IPageChangeOptions) => void = (options) => {
        const qParams: Record<string, string> = {
            size: `${options.pageSize || pageSize}`,
        }
        if (options.filter || filterOptions) {
            qParams.filter = JSON.stringify(options.filter || filterOptions)
        }
        const q = new URLSearchParams(qParams).toString()

        const newPageNumber = options.pageNumber || pageNumber
        const pageSuffix = newPageNumber ? `/${newPageNumber}` : ''
        navigate(`/project/${projectName}${pageSuffix}?${q}`)
    }

    let filterOptions: ProjectParticipantGridFilter = {}
    const _filterOptions = searchParams.get('filter') || '{}'
    try {
        filterOptions = JSON.parse(_filterOptions)
    } catch (e) {
        hasOptionErrors.filter = {
            error: `Error parsing filter options: ${_filterOptions}`,
            default: {},
            value: _filterOptions,
        }
    }

    const setFilterValues = (values: ProjectParticipantGridFilter) => {
        setPageOptions({ filter: values, pageNumber: 1 })
    }

    const getParticipantsFor = () => {
        if (!projectName) {
            return Promise.reject()
        }
        if (Object.keys(hasOptionErrors).length) {
            return Promise.reject()
        }
        setError(undefined)
        setIsLoading(true)
        const skip = pageNumber > 1 ? (pageNumber - 1) * pageSize : undefined
        // return
        return new WebApi()
            .getProjectParticipantsGridWithLimit(projectName, pageSize, filterOptions, skip)
            .then((resp) => {
                setParticipants(resp.data)
                setIsLoading(false)
                setHeaderGroups(defaultHeaderGroupsFromResponse(resp.data))
            })
            .catch((er: AxiosError) => {
                setError(
                    <>
                        An error occurred when fetching participants: {er.message}
                        <br />
                        <pre>{JSON.stringify(er.response?.data, null, 2)}</pre>
                        <Button
                            color="red"
                            onClick={() => {
                                getParticipantsFor()
                            }}
                        >
                            Retry
                        </Button>
                    </>
                )
                setIsLoading(false)
            })
    }

    // load every time the project name changes
    React.useEffect(() => {
        getParticipantsFor()
        // use the raw _filterOptions, otherwise it breaks
    }, [projectName, pageSize, pageNumber, _filterOptions])

    const totalPageNumbers = Math.ceil((participants?.total_results || 0) / pageSize)

    const projectColOptions = (
        <ProjectColumnOptions
            headerGroups={headerGroups}
            setHeaderGroups={setHeaderGroups}
            filterValues={filterOptions}
            updateFilters={(values) => setFilterValues(values)}
            participantCount={participants?.participants?.length ?? 0}
            isOpen={projectColOptionsAreOpen}
            setIsOpen={setProjectColOptionsAreOpen}
        />
    )

    if (isLoading) {
        return (
            <>
                {projectColOptions}
                <LoadingDucks />
            </>
        )
    }
    if (error) {
        return (
            <>
                {projectColOptions}
                <MuckError>{error}</MuckError>
            </>
        )
    }

    if (Object.keys(hasOptionErrors).length) {
        const defaultOptions = Object.entries(hasOptionErrors).reduce((acc, [k, v]) => {
            // @ts-ignore
            acc[k] = v.default
            return acc
        }, {} as IPageChangeOptions)

        const displayDefaults = Object.entries(hasOptionErrors).reduce((acc, [k, v]) => {
            // @ts-ignore
            acc.push(`${k}=${JSON.stringify(v.default)}`)
            return acc
        }, [] as string[])

        return (
            <Message error>
                <Message.Header>Invalid page options</Message.Header>
                <Message.List>
                    {Object.entries(hasOptionErrors).map(([k, v]) => (
                        <Message.Item key={k}>{v.error}</Message.Item>
                    ))}
                </Message.List>
                {hasOptionErrors.filter && (
                    <DictEditor
                        input={hasOptionErrors.filter.value}
                        onChange={(v) => setFilterValues(v)}
                    />
                )}

                <Button onClick={() => setPageOptions(defaultOptions)}>
                    Reset to default ({displayDefaults.join(', ')})
                </Button>
            </Message>
        )
    }

    return (
        <ErrorBoundary title="Error rendering project grid">
            {projectColOptions}

            <div
                style={{
                    marginBottom: '10px',
                    justifyContent: 'flex-end',
                    display: 'flex',
                    flexDirection: 'row',
                    marginTop: '40px',
                }}
            >
                <Dropdown
                    selection
                    onChange={(_, data) =>
                        setPageOptions({ pageSize: data.value as number, pageNumber: 1 })
                    }
                    value={pageSize}
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
                    handleOnClick={(_page) => setPageOptions({ pageNumber: _page })}
                    title="participants"
                />
            </div>
            <ProjectGrid
                participantResponse={participants}
                projectName={projectName}
                headerGroups={headerGroups}
                updateFilters={setFilterValues}
                filterValues={filterOptions}
            />
            <PageOptions
                isLoading={isLoading}
                totalPageNumbers={totalPageNumbers}
                total={participants?.total_results}
                pageNumber={pageNumber}
                handleOnClick={(_page) => setPageOptions({ pageNumber: _page })}
                title="participants"
            />
        </ErrorBoundary>
    )
}
