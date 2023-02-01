import * as React from 'react'
import { useQuery } from '@apollo/client'

import { gql } from '../src/__generated__/gql'

const GET_PROJECTS = gql(`
    query getProjects {
        myProjects {
            id
        }
    }
`)
const GraphQL: React.FunctionComponent<{}> = () => {
    const { loading, error, data } = useQuery(GET_PROJECTS)
    if (loading) return <>"Loading..."</>
    if (error) return <>`Error! ${error.message}`</>
    // console.log(data.myProjects);
    return <>{data && data.myProjects.map((item) => <option key={item.id}>{item.id}</option>)}</>
}
export default GraphQL
