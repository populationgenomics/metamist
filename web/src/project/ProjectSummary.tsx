/* eslint react-hooks/exhaustive-deps: 0 */

import * as React from 'react'
import { useSearchParams } from 'react-router-dom'
import _ from 'lodash'

import { AppContext } from '../GlobalState'
import { ProjectSelector } from './ProjectSelector'
import { WebApi, ProjectSummaryResponse } from '../sm-api/api'

import { Input, Button } from 'reactstrap'

const PAGE_SIZES = [20, 40, 100, 1000]

const sanitiseValue = (value: any) => {
    const tvalue = typeof value
    if (tvalue === 'string' || tvalue === 'number') return value
    if (tvalue === 'boolean') return value ? 'true' : 'false'
    if (value === undefined || value === null) return ''
    return JSON.stringify(value)
}


export const ProjectSummary = () => {

    // store project in global settings
    const globalContext = React.useContext(AppContext)

    let [searchParams, setSearchParams] = useSearchParams()

    const directToken = searchParams.get('token')

    const [summary, setSummary] = React.useState<ProjectSummaryResponse | undefined>()
    const [suppliedDirectToken, setSuppliedDirectToken] = React.useState<boolean | undefined>(undefined)
    const [pageTokens, setPageTokens] = React.useState<number[]>([])
    const [isLoading, setIsLoading] = React.useState<boolean>(false)
    const [error, setError] = React.useState<string | undefined>()
    const [pageLimit, _setPageLimit] = React.useState<number>(PAGE_SIZES[0])

    const getProjectSummary = React.useCallback(async (token: any, addToPageToken: boolean) => {
        if (!globalContext.project) return
        let sanitisedToken = !!token ? token : undefined
        setIsLoading(true)
        new WebApi()
            .getProjectSummary(globalContext.project, pageLimit, sanitisedToken)
            .then(resp => {
                if (token && addToPageToken) {
                    setPageTokens([...pageTokens, token])
                }
                setIsLoading(false)
                setSummary(resp.data)
            }).catch(er => {
                setIsLoading(false)
                setError(er.message)
            })
    }, [globalContext.project, pageLimit, pageTokens])


    const setPageLimit = React.useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
        const value = e.currentTarget.value

        if (!suppliedDirectToken) {
            // if we supplied a token directly to the page,
            // we want to START from that point, otherwise,
            // let's go back to the start.
            setPageTokens([])
            searchParams.delete('token')
            setSearchParams(searchParams)
        }
        _setPageLimit(parseInt(value))
    }, [suppliedDirectToken])

    const _updateProjectSummary = React.useCallback(() => {
        if (_.isUndefined(suppliedDirectToken)) {
            setSuppliedDirectToken(!!directToken)
        }
        getProjectSummary(directToken, false)
    }, [getProjectSummary, suppliedDirectToken, directToken])

    // retrigger if project changes, or pageLimit changes
    React.useEffect(
        _updateProjectSummary,
        [globalContext.project, pageLimit]
    )

    let table: React.ReactElement = <></>


    const pageNumber = pageTokens.length + 1
    const totalPageNumbers = Math.ceil((summary?.total_samples || 0) / pageLimit)

    let pageOptions: React.ReactElement = <></>
    if (suppliedDirectToken) {
        let supplied_samples = summary?.participants.reduce((p, s) => s.samples.length + p, 0)
        pageOptions = <>
            {<Button disabled={isLoading} onClick={(async () => {
                await getProjectSummary(undefined, false)
                setSuppliedDirectToken(false)
                searchParams.delete('token')
                setSearchParams(searchParams)
            })}>Go to start</Button>}
            <span style={{ padding: "0 10px" }}>({supplied_samples} / {summary?.total_samples} samples)</span>
        </>
    } else {

        pageOptions = <>
            {pageTokens.length >= 1 && <Button disabled={isLoading} onClick={(async () => {
                setPageTokens(pageTokens.slice(undefined, pageTokens.length - 1))
                await getProjectSummary(pageTokens[pageTokens.length - 2], false)
            })}>Previous</Button>}
            {!!summary?.total_samples && <span style={{ padding: "0 10px" }}>Page {pageNumber} / {totalPageNumbers} ({summary?.total_samples} samples)</span>}
            {summary?._links?.token && <Button disabled={isLoading} onClick={async () => {
                searchParams.set('token', summary?._links?.token || '')
                setSearchParams(searchParams)
                await getProjectSummary(summary?._links?.token, true)
            }}>Next</Button>}
        </>
    }


    if (error) {
        table = <p><em>An error occurred when fetching samples: {error}</em></p>
    }
    else if (!summary) {
        if (globalContext.project) { table = <p>Loading...</p> }
        else { table = <p><em>Please select a project</em></p> }
    }

    else {
        if (summary.participants.length === 0) {
            table = <p><em>No samples</em></p>
        } else {

            const headers = [
                'Family ID',
                'Participant',
                ...(summary.sample_keys.map(sk => sk.replace(".meta", ""))),
                ...(summary.sequence_keys.map(sk => "sequence." + sk.replace(".meta", ""))),
            ]

            table = <table className='table table-bordered'>
                <thead>
                    <tr>
                        {headers.map(k => <th key={k}>{k}</th>)}
                    </tr>
                </thead>
                <tbody>
                    {summary.participants.map((p, pidx) => p.samples.map((s, sidx) => {
                        // @ts-ignore
                        const backgroundColor = pidx % 2 === 0 ? 'white' : 'var(--bs-table-striped-bg)'
                        const lengthOfParticipant = p.samples.map(s => s.sequences.length).reduce((a, b) => a + b, 0)
                        return s.sequences.map((seq, seqidx) => {
                            return <tr key={`${p.external_id}-${s.id}-${seq.id}`}>
                                {sidx === 0 && seqidx === 0 && <td style={{ backgroundColor }} rowSpan={lengthOfParticipant}>{p.families.map(f => f.external_id).join(', ')}</td>}
                                {sidx === 0 && seqidx === 0 && <td style={{ backgroundColor }} rowSpan={lengthOfParticipant}>{p.external_id}</td>}
                                {seqidx === 0 && summary.sample_keys.map(k => <td style={{ backgroundColor }} key={s.id + 'sample.' + k} rowSpan={s.sequences.length}>{sanitiseValue(_.get(s, k))}</td>)}
                                {seq && summary.sequence_keys.map(k => <td style={{ backgroundColor }} key={s.id + 'sequence.' + k}>{sanitiseValue(_.get(seq, k))}</td>)}
                            </tr>
                        })

                    }))}
                </tbody>
            </table>
        }
    }


    return <>
        <br />
        <ProjectSelector />
        <hr />
        <div style={{ marginBottom: "10px", justifyContent: "flex-end", display: "flex", flexDirection: "row" }}>
            <Input type="select" onChange={setPageLimit} value={pageLimit} style={{ display: "inline", width: "150px", marginRight: "10px" }}>
                {PAGE_SIZES.map(s => <option key={`page-size-${s}`} value={s}>{s} samples</option>)}
            </Input>
            {pageOptions}
        </div>
        {table}
        {pageOptions}
    </>

}
