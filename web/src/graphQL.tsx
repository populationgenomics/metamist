import * as React from 'react'
import { useQuery } from '@apollo/client'

import { gql } from './__generated__/gql'

const GET_PROJECT_SUMMARY = gql(`
    query projectSummary {
        project(name: "dr-dev-bioheart") {
            id
            participants {
                id
            }
        }
    }
`)
const GraphQL: React.FunctionComponent<Record<string, never>> = () => {
    const { loading, error, data } = useQuery(GET_PROJECT_SUMMARY)
    if (loading) return <>`Loading...`</>
    if (error) return <>`Error! ${error.message}`</>
    return (
        <>
            {data &&
                data.project.participants.map((item) => <option key={item.id}>{item.id}</option>)}
        </>
    )
}
export default GraphQL
