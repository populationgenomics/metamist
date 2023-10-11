/* eslint-disable */
// Project ETL specific page

import * as React from 'react'
import { Button } from 'semantic-ui-react'
import { EtlApi, EtlRecord } from '../../sm-api/api'

const EtlAdmin = () => {
    // requests by sample type?
    // requests by loading status?
    const [records, setRecords] = React.useState<EtlRecord[] | null>(null)
    const [text, setText] = React.useState<string | null>(null)

    const getEtlSummary = React.useCallback(async () => {
        try {
            const response = await new EtlApi().getEtlSummary(undefined, undefined, undefined)
            setRecords(response.data)
        } catch (er: any) {
            setText(`Failed with error: ${er.message}`)
        }
    }, [])

    const resubmitRequest = (request_id: any) => {
        console.log('resubmitRequest: ', request_id)
        new EtlApi()
            .etlResubmit(request_id)
            .then(() => getEtlSummary())
            .catch((er) => setText(er.message))
    }

    React.useEffect(() => {
        getEtlSummary()
    }, [])

    const getTableBodyAsReactElement = () => {
        console.log('records: ', records)

        return !records ? null : (
            <tbody>
                {records.map((item) => {
                    // changed here
                    console.log('item: ', item)
                    return (
                        <tr>
                            <td style={{ padding: '15px' }}>{item.last_run_at}</td>
                            <td style={{ padding: '15px' }}>{item.request_id}</td>
                            <td style={{ padding: '15px' }}>{item.source_type}</td>
                            <td style={{ padding: '15px' }}>{item.submitting_user}</td>
                            <td style={{ padding: '15px' }}>{item.status}</td>
                            <td style={{ padding: '15px' }}>{item.parser_result}</td>
                            <td style={{ padding: '15px' }}>
                                <Button
                                    color="red"
                                    onClick={() => resubmitRequest(item.request_id)}
                                >
                                    Resubmit
                                </Button>
                            </td>
                        </tr>
                    )
                })}
            </tbody>
        )
    }

    if (!records) {
        return (
            <p style={{ textAlign: 'center' }}>
                <em>Loading...</em>
            </p>
        )
    }

    return (
        <>
            <h1>ETL Admin</h1>
            <div className="article">
                <ul>
                    <table border="1">
                        <thead>
                            <tr>
                                <th style={{ padding: '15px' }}>Last Run At</th>
                                <th style={{ padding: '15px' }}>Request ID</th>
                                <th style={{ padding: '15px' }}>Source Type</th>
                                <th style={{ padding: '15px' }}>User</th>
                                <th style={{ padding: '15px' }}>Status</th>
                                <th style={{ padding: '15px' }}>Parser Msg</th>
                                <th style={{ padding: '15px' }}>Reload</th>
                            </tr>
                        </thead>
                        {getTableBodyAsReactElement()}
                    </table>
                </ul>
            </div>
        </>
    )
}

export default EtlAdmin
