import * as React from 'react'

import { useParams } from 'react-router-dom'
import { Accordion, AccordionTitleProps, Card, Grid, GridColumn, GridRow, Table as SUITable } from 'semantic-ui-react'



import PersonRoundedIcon from '@mui/icons-material/PersonRounded'
import BloodtypeRoundedIcon from '@mui/icons-material/BloodtypeRounded'

import { useQuery } from '@apollo/client'
import Pedigree from '../../shared/components/pedigree/Pedigree'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'

import { gql } from '../../__generated__/gql'
import FamilyViewTitle from './FamilyViewTitle'

import iconStyle from '../../shared/iconStyle'
import SeqPanel from '../../shared/components/SeqPanel'
import SampleInfo from '../../shared/components/SampleInfo'
import TangledTree, { PersonNode } from '../../shared/components/pedigree/TangledTree'
import Table from '../../shared/components/Table'
import _ from 'lodash'

const sampleFieldsToDisplay = ['active', 'type']

const GET_FAMILY_INFO = gql(`
query FamilyInfo($family_id: Int!) {
    family(familyId: $family_id) {
      id
      externalId
      project {
        name
        pedigree(
          internalFamilyIds: [$family_id]
          replaceWithFamilyExternalIds: true
          replaceWithParticipantExternalIds: true
        )
      }
      familyParticipants {
        affected
        participant {
          id
          externalId
          phenotypes
          meta
          samples {
            id
            externalId
            meta
            type
            sequencingGroups {
              id
              platform
              technology
              type
              assays {
                id
                meta
                type
              }
              analyses(status: {eq: COMPLETED}) {
                id
                timestampCompleted
                type
                meta
                output
              }
            }
          }
        }
      }
    }
  }`)

const FamilyView: React.FunctionComponent<Record<string, unknown>> = () => {
  const { familyID } = useParams()

  if (!familyID) return <em>No family ID</em>
  const familyId = parseInt(familyID)
  if (!familyId || isNaN(familyId)) return <em>Invalid family ID</em>


  const { loading, error, data } = useQuery(GET_FAMILY_INFO, {
    variables: { family_id: parseInt(familyID) },
  })

  if (loading) return <LoadingDucks />
  if (error) return <>Error! {error.message}</>
  if (!data) return <>No data!</>

  const sgs = data?.family?.familyParticipants.flatMap(fp => fp.participant.samples.flatMap(s => s.sequencingGroups))
  const sgsById = _.keyBy(sgs, s => s.id)
  const participantBySgId = data?.family?.familyParticipants.reduce((acc, fp) => {
    for (const s of fp.participant.samples) {
      for (const sg of s.sequencingGroups) {
        acc[sg.id] = fp.participant
      }
    }
    return acc
  }, {})

  const aById: any = {}
  for (const fp of data?.family?.familyParticipants) {
    for (const s of fp.participant.samples) {
      for (const sg of s.sequencingGroups) {
        for (const a of sg.analyses) {
          if (a.id in aById) {
            aById[a.id].sgs.push(sg.id)
          } else {
            aById[a.id] = {
              ...a,
              sgs: [sg.id]
            }
          }
        }
      }
    }
  }
  const analyses = _.orderBy(Object.values(aById), a => a.completed_timestamp)

  return <div className="dataStyle" style={{ width: '100%' }}>
    <h2>{data?.family?.externalId} ({data?.family?.project?.name})</h2>
    <Card>
      <TangledTree data={data?.family?.project.pedigree} />
    </Card>
    <PedigreeTable pedigree={data?.family?.project.pedigree} />

    <Table>
      <thead>
        <SUITable.Row>
          <SUITable.HeaderCell>Sequencing group</SUITable.HeaderCell>
          <SUITable.HeaderCell>Created</SUITable.HeaderCell>
          <SUITable.HeaderCell>Type</SUITable.HeaderCell>
          <SUITable.HeaderCell>Sequencing type</SUITable.HeaderCell>
          <SUITable.HeaderCell>Output</SUITable.HeaderCell>
        </SUITable.Row>
      </thead>
      {analyses?.map((a: any) => <SUITable.Row key={a.id}>
        <SUITable.Cell>
          {a.sgs.map(sg => `${sg} (${participantBySgId[sg].externalId})`)}
        </SUITable.Cell>
        <SUITable.Cell>{a.timestampCompleted}</SUITable.Cell>
        <SUITable.Cell>{a.meta?.sequencing_type}</SUITable.Cell>
        <SUITable.Cell>{a.type}</SUITable.Cell>
        <SUITable.Cell>{a.output}</SUITable.Cell>
      </SUITable.Row>
      )}
    </Table>

  </div>
}

const PedigreeTable: React.FC<{ pedigree: any, participant }> = ({ pedigree, participant }) => {

  return <Table>
    <thead>
      <SUITable.Row>
        <SUITable.HeaderCell></SUITable.HeaderCell>
        <SUITable.HeaderCell>Participant</SUITable.HeaderCell>
        <SUITable.HeaderCell>Paternal ID</SUITable.HeaderCell>
        <SUITable.HeaderCell>Maternal ID</SUITable.HeaderCell>
        <SUITable.HeaderCell>Affected</SUITable.HeaderCell>
        <SUITable.HeaderCell>Notes</SUITable.HeaderCell>
      </SUITable.Row>
    </thead>
    <tbody>
      {pedigree?.map((pr: any) => <SUITable.Row key={pr.individual_id}>
        <td><svg width={30} height={30}><PersonNode showIndividualId={false} nodeSize={30} node={{
          x: 15,
          y: 15
        }} entry={pr} />
        </svg></td>
        <td>{pr.individual_id}</td>
        <td>{pr.paternal_id}</td>
        <td>{pr.maternal_id}</td>
        <td>{pr.affected}</td>
        <td>{pr.notes}</td>
      </SUITable.Row>)}
    </tbody>
  </Table>
}

export default FamilyView
