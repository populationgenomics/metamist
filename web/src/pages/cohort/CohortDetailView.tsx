import React from 'react'
import { useQuery } from '@apollo/client'
import { Container, Divider } from 'semantic-ui-react'

import { gql } from '../../__generated__'
import MuckError from '../../shared/components/MuckError'
import { Project } from './types'
import SequencingGroupTable from './SequencingGroupTable'

const COHORT_DETAIL_VIEW_FRAGMENT = gql(`
query CohortDetailView($id: Int!) {
  cohort(id: {eq: $id}) {
    id
    name
    description
    sequencingGroups {
      id
      type
      technology
      platform
    }
  }
}
`)

interface ICohortDetailViewProps {
    id: number
    project: Project
}

const CohortDetailView: React.FC<ICohortDetailViewProps> = ({ id, project }) => {
    const { loading, error, data } = useQuery(COHORT_DETAIL_VIEW_FRAGMENT, {
        variables: { id },
    })

    if (loading) {
        return (
            <Container>
                <div>Loading Cohort...</div>
            </Container>
        )
    }

    if (error) {
        return (
            <Container>
                <MuckError message={error.message} />
            </Container>
        )
    }

    const cohort = data?.cohort[0] || null
    if (!cohort) {
        return (
            <Container>
                <MuckError message="No data found" />
            </Container>
        )
    }

    return (
        <Container>
            <div>
                <b>Name: </b> {cohort.name}
                <b>Description: </b> {cohort.description}
            </div>
            <Divider />
            <SequencingGroupTable
                sequencingGroups={cohort.sequencingGroups.map((sg) => ({
                    id: sg.id,
                    type: sg.type,
                    technology: sg.technology,
                    platform: sg.platform,
                    project,
                }))}
                editable={false}
            />
        </Container>
    )
}

export default CohortDetailView
