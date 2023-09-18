import * as React from 'react'

import { useParams } from 'react-router-dom'
import { Table as SUITable } from 'semantic-ui-react'
import { useQuery } from '@apollo/client'
import Table from '../../shared/components/Table'

import Pedigree from '../../shared/components/pedigree/Pedigree'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'

import { gql } from '../../__generated__/gql'
import FamilyViewTitle from '../family/FamilyViewTitle'

const GET_PROJECT_INFO = gql(`
query getMyProjects {
    myProjects {
      id
      name
    }
  }`)

const HEADINGS = [
    'Dataset',
    'Sequencing Type',
    'Families',
    'Participants',
    'Samples',
    'Sequencing Groups',
    'Completed CRAMs',
    'Sequencing Groups in latest ES-index',
    'Sequencing Groups in latest Joint Call',
    'CRAM Completeness (%)',
    'ES-index Completeness (%)',
    'Joint Call Completeness (%)',
]

const DatasetView: React.FunctionComponent<Record<string, unknown>> = () => {
    const { loading, error, data } = useQuery(GET_PROJECT_INFO)

    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>

    const tableData = data?.myProjects.map((project) => ({ projectName: project.name }))

    return data ? (
        <div className="dataStyle" style={{ width: '100%' }}>
            <>
                <Table celled compact sortable>
                    <SUITable.Header>
                        <SUITable.Row>
                            {HEADINGS.map((title) => (
                                <SUITable.HeaderCell
                                    key={title}
                                    style={{
                                        borderBottom: 'none',
                                        position: 'sticky',
                                        resize: 'horizontal',
                                    }}
                                >
                                    {title}
                                </SUITable.HeaderCell>
                            ))}
                        </SUITable.Row>
                    </SUITable.Header>
                    <SUITable.Body>
                        {tableData?.map((row, i) => (
                            <SUITable.Row key={i}>
                                {Object.values(row).map((cell, j) => (
                                    <SUITable.Cell key={`${i}-${j}`}>{cell}</SUITable.Cell>
                                ))}
                            </SUITable.Row>
                        ))}
                    </SUITable.Body>
                </Table>
            </>
        </div>
    ) : (
        <></>
    )
}

export default DatasetView
