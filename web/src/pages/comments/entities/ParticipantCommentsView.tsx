import { useQuery } from '@apollo/client'
import { gql } from '../../../__generated__'
import { DiscussionView } from '../DiscussionView'
import { useNewComment } from '../data/commentMutations'

export const PARTICIPANT_COMMENTS = gql(`
    query ParticipantComments($participantId: Int!) {
        participant(id: $participantId) {
            id
            discussion {
                ... DiscussionFragment
            }
        }
    }
`)

export const PARTICIPANT_ADD_COMMENT = gql(`
    mutation AddCommentToParticipant($id: Int!, $content: String!) {
        participant {
            addComment(id: $id, content: $content) {
                ...CommentFragment

                thread {
                    ...CommentFragment
                }
            }
        }
    }
`)

export function useNewCommentOnParticipant(id: number | null) {
    return useNewComment(
        PARTICIPANT_ADD_COMMENT,
        id ? `GraphQLParticipant:${id}` : null,
        (data) => data.participant.addComment
    )
}

type ParticipantCommentsViewProps = {
    participantId: number
    projectName: string
    onToggleCollapsed: (collapsed: boolean) => void
    collapsed: boolean
}

export function ParticipantCommentsView(props: ParticipantCommentsViewProps) {
    const { loading, error, data, refetch } = useQuery(PARTICIPANT_COMMENTS, {
        variables: { participantId: props.participantId },
        notifyOnNetworkStatusChange: true,
    })

    const participant = data?.participant

    const [addCommentToParticipantMutation, addCommentToParticipantResult] =
        useNewCommentOnParticipant(participant?.id ?? null)

    return (
        <DiscussionView
            discussionEntityType={participant?.__typename}
            discussionLoading={loading}
            discussionError={error}
            discussion={participant?.discussion}
            collapsed={props.collapsed}
            onToggleCollapsed={props.onToggleCollapsed}
            addingCommentLoading={addCommentToParticipantResult.loading}
            addingCommentError={addCommentToParticipantResult.error}
            projectName={props.projectName}
            onReload={() => {
                refetch()
            }}
            onAddComment={async (content: string) => {
                if (participant?.id) {
                    await addCommentToParticipantMutation({
                        variables: {
                            content,
                            id: participant.id,
                        },
                    })
                }
            }}
        />
    )
}
