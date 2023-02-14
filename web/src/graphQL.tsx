import * as React from 'react'
import { useQuery } from '@apollo/client'

import { gql } from './__generated__/gql'

const GET_SAMPLE_INFO = gql(`
query SampleInfo1 {
    sample(id: "CPGLCL978") {
      id
      participantId
      externalId
      participant {
        externalId
      }
      sequences {
        id
        externalIds
        type
        meta
      }
      type
      active
    }
  }
`)
const GraphQL: React.FunctionComponent<Record<string, never>> = () => {
    const { loading, error, data } = useQuery(GET_SAMPLE_INFO)
    if (loading) return <>`Loading...`</>
    if (error) return <>`Error! ${error.message}`</>
    return <>{data && JSON.stringify(data)}</>
}
export default GraphQL
