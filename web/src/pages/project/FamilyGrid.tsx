import * as React from 'react'
import { useQuery } from '@apollo/client'
import { Table as SUITable } from 'semantic-ui-react'
import FamilyLink from '../../shared/components/links/FamilyLink'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { gql } from '../../__generated__/gql'
import Table from '../../shared/components/Table'

interface FamilyGridProps {
    projectName: string
}

const GET_FAMILY_GRID_INFO = gql(`
query GetFamilyData($project: String!) {
  project(name: $project ) {
    id
    families {
      externalId
      id
      participants {
        id
        samples {
          id
          sequencingGroups {
            type
          }
        }
      }
    }
    pedigree
  }
}
`)

const HEADINGS = [
    'Family Name',
    'Number of Individuals',
    'Number of Affected Individuals',
    'Individuals with SG type Genome',
    'Individuals with SG type Exome',
    'Individuals with SG type Transcriptome',
    'Individuals with SG type Mtseq',
]

const FamilyGrid: React.FunctionComponent<FamilyGridProps> = ({ projectName }) => {
    const { loading, error, data } = useQuery(GET_FAMILY_GRID_INFO, {
        variables: { project: projectName },
    })

    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>

    const tableData = data?.project.families.map((family) => {
        const types = family.participants
            .map((p) => [
                ...new Set(p.samples.map((s) => s.sequencingGroups.map((sg) => sg.type)).flat()),
            ])
            .flat()
            .reduce((acc, color) => ((acc[color] = (acc[color] || 0) + 1), acc), {})
        return {
            family: { ID: family.id, externalID: family.externalId },
            individualsCount: family.participants.length,
            affectedIndividuals: data.project.pedigree.filter(
                (item) => item.family_id === family.externalId && item.affected === 2
            ).length,
            individuals_SG_genome: types.genome ?? 0,
            individuals_SG_exome: types.exome ?? 0,
            individuals_SG_transcriptome: types.transcriptome ?? 0,
            individuals_SG_mtseq: types.mtseq ?? 0,
        }
    })

    return (
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
                        {Object.values(row).map((cell, j) =>
                            j === 0 ? (
                                <SUITable.Cell key={`${i}-${j}`}>
                                    <FamilyLink id={cell.ID.toString()} projectName={projectName}>
                                        {cell.externalID}
                                    </FamilyLink>
                                </SUITable.Cell>
                            ) : (
                                <SUITable.Cell key={`${i}-${j}`}>{cell}</SUITable.Cell>
                            )
                        )}
                    </SUITable.Row>
                ))}
            </SUITable.Body>
        </Table>
    )
}

export default FamilyGrid
