import { useQuery } from '@apollo/client'
import { gql } from '../../../__generated__'
import { useNewComment } from '../data/commentMutations'
import { DiscussionView } from '../DiscussionView'

export const SAMPLE_COMMENTS = gql(`
    query SampleComments($sampleId: String!) {
        sample(id:{eq: $sampleId}) {
            id
            discussion {
                ... DiscussionFragment
            }
        }
    }
`)

export const SAMPLE_ADD_COMMENT = gql(`
    mutation AddCommentToSample($id: String!, $content: String!, $project: String!) {
        sample(projectName: $project) {
            addComment(id: $id, content: $content) {
                ...CommentFragment

                thread {
                    ...CommentFragment
                }
            }
        }
    }
`)

export function useNewCommentOnSample(id: string | null) {
    return useNewComment(
        SAMPLE_ADD_COMMENT,
        id ? `GraphQLSample:${id}` : null,
        (data) => data.sample.addComment
    )
}

type SampleCommentsViewProps = {
    sampleId: string
    projectName: string
    onToggleCollapsed: (collapsed: boolean) => void
    collapsed: boolean
}

export function SampleCommentsView(props: SampleCommentsViewProps) {
    const { loading, error, data, refetch } = useQuery(SAMPLE_COMMENTS, {
        variables: { sampleId: props.sampleId },
        notifyOnNetworkStatusChange: true,
    })

    const sample = data?.sample[0]

    const [addCommentToSampleMutation, addCommentToSampleResult] = useNewCommentOnSample(
        sample?.id ?? null
    )

    return (
        <DiscussionView
            discussionEntityType={'GraphQLSample'}
            discussionLoading={loading}
            discussionError={error}
            discussion={sample?.discussion}
            collapsed={props.collapsed}
            onToggleCollapsed={props.onToggleCollapsed}
            addingCommentLoading={addCommentToSampleResult.loading}
            addingCommentError={addCommentToSampleResult.error}
            projectName={props.projectName}
            onReload={() => {
                refetch()
            }}
            onAddComment={async (content: string) => {
                if (sample?.id) {
                    await addCommentToSampleMutation({
                        variables: {
                            project: props.projectName,
                            content,
                            id: sample.id,
                        },
                    })
                }
            }}
        />
    )
}
