/* eslint react-hooks/exhaustive-deps: 0 */

import * as React from 'react'
import * as _ from 'lodash'

import { AppContext } from '../GlobalState'
import { ProjectSelector } from './ProjectSelector'
import { WebApi, ProjectSummaryResponse } from '../sm-api/api'

import { Input, Button } from 'reactstrap'

const PAGE_SIZES = [20, 40, 100, 1000]


export const ProjectSummary = () => {

    // store project in global settings
    const globalContext = React.useContext(AppContext)


    const [summary, setSummary] = React.useState<ProjectSummaryResponse | undefined>()
    const [pageTokens, setPageTokens] = React.useState<number[]>([])
    const [isLoading, setIsLoading] = React.useState<boolean>(false)
    const [error, setError] = React.useState<string | undefined>()
    const [pageLimit, _setPageLimit] = React.useState<number>(PAGE_SIZES[0])

    async function getProjectSummary(after: any, addToPageToken: boolean) {
        if (!globalContext.project) return
        setIsLoading(true)
        new WebApi()
            .getProjectSummary(globalContext.project, pageLimit, after)
            .then(resp => {
                if (after && addToPageToken) {
                    setPageTokens([...pageTokens, after])
                }
                setIsLoading(false)
                setSummary(resp.data)
            }).catch(er => {
                setIsLoading(false)
                setError(er.message)
            })
    }

    function setPageLimit(e: React.ChangeEvent<HTMLInputElement>) {
        const value = e.currentTarget.value
        setPageTokens([])
        _setPageLimit(parseInt(value))
    }

    // eslint:
    React.useEffect(() => {
        getProjectSummary(undefined, false)

    }, [globalContext.project, pageLimit])

    let table: React.ReactElement = <></>


    const pageNumber = pageTokens.length + 1
    const totalPageNumbers = Math.ceil((summary?.total_samples || 0) / pageLimit)

    let pageOptions: React.ReactElement = <>
        {pageTokens.length >= 1 && <Button disabled={isLoading} onClick={(async () => {
            setPageTokens(pageTokens.slice(undefined, pageTokens.length - 1))
            await getProjectSummary(pageTokens[pageTokens.length - 2], false)
        })}>Previous</Button>}
        {(summary?.total_samples || 0) > 0 && <span style={{ padding: "0 10px" }}>Page {pageNumber} / {totalPageNumbers}</span>}
        {summary?._links?.after && <Button disabled={isLoading} onClick={async () => await getProjectSummary(summary?._links?.after, true)}>Next</Button>}
    </>


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
            // table = <DataGrid
            //     columns={headers}
            //     rows={samples}
            //     style={{ resize: 'vertical' }}
            // // sortColumns={sortColumns}
            // // onSortColumnsChange={onSortColumnsChange}
            // />

            const headers = [
                'Family ID',
                'Participant',
                ...(summary.sample_keys.map(sk => sk.replace(".meta", ""))),
                ...(summary.sequence_keys.map(sk => "sequence." + sk.replace(".meta", ""))),
            ]

            table = <table className='table table-striped table-bordered'>
                <thead>
                    <tr>
                        {headers.map(k => <th key={k}>{k}</th>)}
                    </tr>
                </thead>
                <tbody>
                    {summary.participants.map(p => p.samples.map((s, sidx) =>
                        <tr>
                            {sidx === 0 && <td rowSpan={p.samples.length}>{p.families.map(f => f.external_id).join(', ')}</td>}
                            {sidx === 0 && <td rowSpan={p.samples.length}>{p.external_id}</td>}
                            {summary.sample_keys.map(k => <td key={s.id + 'sample.' + k}>{_.get(s, k)}</td>)}
                            {s.sequences[0] && summary.sequence_keys.map(k => <td key={s.id + 'sequence.' + k}>{_.get(s.sequences[0], k)}</td>)}
                        </tr>
                    ))}
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
