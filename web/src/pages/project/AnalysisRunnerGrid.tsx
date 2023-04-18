import * as React from 'react'
import { useQuery } from '@apollo/client'
import { Table as SUITable } from 'semantic-ui-react'
import _ from 'lodash'
import { gql } from '../../__generated__/gql'
import MuckError from '../../shared/components/MuckError'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import Table from '../../shared/components/Table'
import sanitiseValue from '../../shared/utilities/sanitiseValue'

const GET_ANALYSIS_RUNNER_LOGS = gql(`
query AnalysisRunnerLogs($project_name: String!) {
    project(name: $project_name) {
        id
        analyses(type: "analysis-runner") {
          active
          type
          meta
          id
          output
          status
        }
      }
  }
`)

const AnalysisRunnerGrid: React.FunctionComponent<{ projectName: string }> = ({ projectName }) => {
    const { loading, error, data } = useQuery(GET_ANALYSIS_RUNNER_LOGS, {
        variables: { project_name: projectName },
    })

    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>
    console.log(data)
    if (!data?.project.analyses.length)
        return (
            <MuckError
                message={`Ah Muck, there are no analysis-runner logs for this project here`}
            />
        )

    const mainHeaders = [
        'id',
        ...new Set(
            data.project.analyses
                .flatMap(Object.keys)
                .filter((header) => header !== '__typename' && header !== 'meta' && header !== 'id')
        ),
    ]
    const metaHeaders = [
        ...new Set(data.project.analyses.map((item) => Object.keys(item.meta)).flat()),
    ]

    const allHeaders = mainHeaders.concat(metaHeaders)

    return (
        <Table celled>
            <SUITable.Header>
                <SUITable.Row>
                    {allHeaders.map((name, i) => (
                        <SUITable.HeaderCell key={`${name}-${i}`}>{name}</SUITable.HeaderCell>
                    ))}
                </SUITable.Row>
            </SUITable.Header>
            <SUITable.Body>
                {data.project.analyses.map((log) => (
                    <SUITable.Row key={`${log.id}`}>
                        {mainHeaders.map((category) => (
                            <SUITable.Cell key={`${category}`}>
                                {sanitiseValue(_.get(log, category))}
                            </SUITable.Cell>
                        ))}
                        {metaHeaders.map((category) => (
                            <SUITable.Cell key={`${category}`}>
                                {sanitiseValue(_.get(log.meta, category))}
                            </SUITable.Cell>
                        ))}
                    </SUITable.Row>
                ))}
            </SUITable.Body>
        </Table>
    )
}

export default AnalysisRunnerGrid
