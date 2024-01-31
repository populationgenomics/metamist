import React from 'react'
import { useQuery } from '@apollo/client'
import { useParams } from 'react-router-dom'
import { Container, Divider } from 'semantic-ui-react'

import { gql } from '../../__generated__'
import MuckError from '../../shared/components/MuckError'
import SequencingGroupTable from './SequencingGroupTable'

const COHORT_DETAIL_VIEW_FRAGMENT = gql(`
query CohortDetailView($id: Int!) {
  cohort(id: {eq: $id}) {
    id
    name
    description
    project {
      id
      name
    }
    sequencingGroups {
      id
      type
      technology
      platform
      assays {
        meta
      }
      sample {
        project {
            id
            name
          }
      }
    }
  }
}
`)

const CohortDetailView: React.FC = () => {
    const { id } = useParams()

    const { loading, error, data } = useQuery(COHORT_DETAIL_VIEW_FRAGMENT, {
        variables: { id: id ? parseInt(id, 10) : 0 },
    })

    if (loading) {
        return (
            <Container>
                <h1>Cohort Information</h1>
                <Divider />
                <div>Loading Cohort...</div>
            </Container>
        )
    }

    if (error) {
        return (
            <Container>
                <h1>Cohort Information</h1>
                <Divider />
                <MuckError message={error.message} />
            </Container>
        )
    }

    const cohort = data?.cohort[0] || null
    if (!cohort) {
        return (
            <Container>
                <h1>Cohort Information</h1>
                <Divider />
                <MuckError message="Cohort not found" />
            </Container>
        )
    }

    return (
        <Container>
            <h1>Cohort Information</h1>
            <Divider />
            <div>
                <b>Name: </b> {cohort.name}
                <br />
                <b>Description: </b> {cohort.description}
                <br />
                <b>Project: </b> {cohort.project.name}
            </div>
            <Divider />
            <SequencingGroupTable
                height={800}
                emptyMessage={'This cohort does not have any sequencing groups'}
                sequencingGroups={cohort.sequencingGroups.map((sg) => ({
                    id: sg.id,
                    type: sg.type,
                    technology: sg.technology,
                    platform: sg.platform,
                    project: sg.sample.project,
                    assayMeta: (sg.assays ?? []).map((a) => a.meta),
                }))}
                editable={false}
            />
        </Container>
    )
}

export default CohortDetailView
