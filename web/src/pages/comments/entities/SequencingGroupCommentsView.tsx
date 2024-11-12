import { useQuery } from '@apollo/client'
import { gql } from '../../../__generated__'
import { useNewComment } from '../data/commentMutations'
import { DiscussionView } from '../DiscussionView'

export const SEQUENCING_GROUP_COMMENTS = gql(`
    query SequencingGroupComments($sequencingGroupId: String!) {
        sequencingGroups(id:{eq: $sequencingGroupId}) {
            id
            discussion {
                ... DiscussionFragment
            }
        }
    }
`)

export const SEQUENCING_GROUP_ADD_COMMENT = gql(`
    mutation AddCommentToSequencingGroup($id: String!, $content: String!, $project: String!) {
        sequencingGroup(projectName: $project) {
            addComment(id: $id, content: $content) {
                ...CommentFragment

                thread {
                    ...CommentFragment
                }
            }
        }
    }
`)

export function useNewCommentOnSequencingGroup(id: string | null) {
    return useNewComment(
        SEQUENCING_GROUP_ADD_COMMENT,
        id ? `GraphQLSequencingGroup:${id}` : null,
        (data) => data.sequencingGroup.addComment
    )
}

type SequencingGroupCommentsViewProps = {
    sequencingGroupId: string
    projectName: string
    onToggleCollapsed: (collapsed: boolean) => void
    collapsed: boolean
}

export function SequencingGroupCommentsView(props: SequencingGroupCommentsViewProps) {
    const { loading, error, data, refetch } = useQuery(SEQUENCING_GROUP_COMMENTS, {
        variables: { sequencingGroupId: props.sequencingGroupId },
        notifyOnNetworkStatusChange: true,
    })

    const sequencingGroup = data?.sequencingGroups[0]

    const [addCommentToSequencingGroupMutation, addCommentToSequencingGroupResult] =
        useNewCommentOnSequencingGroup(sequencingGroup?.id ?? null)

    return (
        <DiscussionView
            discussionEntityType={'GraphQLSequencingGroup'}
            discussionLoading={loading}
            discussionError={error}
            discussion={sequencingGroup?.discussion}
            collapsed={props.collapsed}
            onToggleCollapsed={props.onToggleCollapsed}
            addingCommentLoading={addCommentToSequencingGroupResult.loading}
            addingCommentError={addCommentToSequencingGroupResult.error}
            projectName={props.projectName}
            onReload={() => {
                refetch()
            }}
            onAddComment={async (content: string) => {
                if (sequencingGroup?.id) {
                    await addCommentToSequencingGroupMutation({
                        variables: {
                            content,
                            id: sequencingGroup.id,
                        },
                    })
                }
            }}
        />
    )
}
