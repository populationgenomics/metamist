import { useQuery } from '@apollo/client'
import { gql } from '../../../__generated__'
import { useNewComment } from '../data/commentMutations'
import { DiscussionView } from '../DiscussionView'

export const FAMILY_COMMENTS = gql(`
    query FamilyComments($familyId: Int!) {
        family(familyId: $familyId) {
            id
            discussion {
                ... DiscussionFragment
            }
        }
    }
`)

export const FAMILY_ADD_COMMENT = gql(`
    mutation AddCommentToFamily($id: Int!, $content: String!) {
        family {
            addComment(id: $id, content: $content) {
                ...CommentFragment

                thread {
                    ...CommentFragment
                }
            }
        }
    }
`)

export function useNewCommentOnFamily(id: number | null) {
    return useNewComment(
        FAMILY_ADD_COMMENT,
        id ? `GraphQLFamily:${id}` : null,
        (data) => data.family.addComment
    )
}

type FamilyCommentsViewProps = {
    familyId: number
    projectName: string
    onToggleCollapsed: (collapsed: boolean) => void
    collapsed: boolean
}

export function FamilyCommentsView(props: FamilyCommentsViewProps) {
    const { loading, error, data, refetch } = useQuery(FAMILY_COMMENTS, {
        variables: { familyId: props.familyId },
        notifyOnNetworkStatusChange: true,
    })

    const family = data?.family

    const [addCommentToFamilyMutation, addCommentToFamilyResult] = useNewCommentOnFamily(
        family?.id ?? null
    )

    return (
        <DiscussionView
            discussionEntityType={'GraphQLFamily'}
            discussionLoading={loading}
            discussionError={error}
            discussion={family?.discussion}
            collapsed={props.collapsed}
            onToggleCollapsed={props.onToggleCollapsed}
            addingCommentLoading={addCommentToFamilyResult.loading}
            addingCommentError={addCommentToFamilyResult.error}
            projectName={props.projectName}
            onReload={() => {
                refetch()
            }}
            onAddComment={async (content: string) => {
                if (family?.id) {
                    await addCommentToFamilyMutation({
                        variables: {
                            content,
                            id: family.id,
                        },
                    })
                }
            }}
        />
    )
}
