import { useQuery } from '@apollo/client'
import { gql } from '../../__generated__'
import { DiscussionView } from './DiscussionView'
import { useNewComment } from './commentMutations'

export const PROJECT_COMMENTS = gql(`
    query ProjectComments($projectName: String!) {
        project(name: $projectName) {
            id
            name
            discussion {
                ... DiscussionFragment
            }
        }
    }
`)

export const PROJECT_ADD_COMMENT = gql(`
    mutation AddCommentToProject($id: Int!, $content: String!) {
        project {
            addComment(id: $id, content: $content) {
                ...CommentFragment

                thread {
                    ...CommentFragment
                }
            }
        }
    }
`)

export function newCommentOnProject(id: number | null) {
    return useNewComment(
        PROJECT_ADD_COMMENT,
        id ? `GraphQLProject:${id}` : null,
        (data) => data.project.addComment
    )
}

type ProjectCommentsViewProps = {
    projectName: string
}

export function ProjectCommentsView(props: ProjectCommentsViewProps) {
    const { loading, error, data, refetch } = useQuery(PROJECT_COMMENTS, {
        variables: { projectName: props.projectName },
        notifyOnNetworkStatusChange: true,
    })

    const [addCommentToProjectMutation, addCommentToProjectResult] = newCommentOnProject(
        data?.project.id ?? null
    )

    return (
        <div>
            <DiscussionView
                discussionLoading={loading}
                discussionError={error}
                discussion={data?.project.discussion}
                addingCommentLoading={addCommentToProjectResult.loading}
                addingCommentError={addCommentToProjectResult.error}
                projectName={props.projectName}
                onReload={() => {
                    refetch()
                }}
                onAddComment={async (content: string) => {
                    if (data?.project.id) {
                        await addCommentToProjectMutation({
                            variables: {
                                content,
                                id: data.project.id,
                            },
                        })
                    }
                }}
            />
        </div>
    )
}
