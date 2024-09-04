import { useQuery } from '@apollo/client'
import * as React from 'react'
import { gql } from '../../__generated__'

import { useParams } from 'react-router-dom'
import { Message } from 'semantic-ui-react'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { IAnalysisGridAnalysis } from '../analysis/AnalysisGrid'
import { ParticipantView } from './ParticipantView'

const GET_PARTICIPANT_VIEW_INFO = gql(`
query ParticipantViewInfo($participantId: Int!) {
  participant(id: $participantId) {
    id
    externalId
    karyotype
    reportedGender
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
          sequencingGroups {
            id
          }
        }
      }
    }
  }
}`)

interface IParticipantPageProps {
    participantId?: number
}

export const ParticipantPage: React.FC<IParticipantPageProps> = (props) => {
    const { participantId } = useParams()
    const pid = props.participantId || parseInt(participantId || '')

    const { loading, error, data } = useQuery(GET_PARTICIPANT_VIEW_INFO, {
        variables: { participantId: pid },
    })

    if (!pid || isNaN(pid)) return <em>No participant ID</em>

    if (loading) {
        return <LoadingDucks />
    }
    if (error || !data) {
        return <Message error>{error?.message || 'Participant was not found'}</Message>
    }

    const participant = data?.participant
    const analyses = participant.samples
        .flatMap((s) => s.sequencingGroups)
        .flatMap((sg) => sg.analyses)
        .flatMap(
            (an) =>
                ({
                    ...an,
                    sgs: an.sequencingGroups.map((sg) => sg.id),
                }) as IAnalysisGridAnalysis
        )
    return <ParticipantView participant={participant} analyses={analyses} showNonSingleSgAnalyses />
}
