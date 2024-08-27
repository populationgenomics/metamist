import { useQuery } from '@apollo/client'
import { gql } from '../../__generated__/gql'

const PROJECT_COMMENTS = gql(`
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

type ProjectCommentsViewProps = {
    projectName: string
}

export function ProjectCommentsView(props: ProjectCommentsViewProps) {
    const { loading, error, data } = useQuery(PROJECT_COMMENTS, {
        variables: { projectName: props.projectName },
    })

    if (!data || !data.project) return null

    return (
        <>
            {data.project.discussion.directComments.map((cc) => (
                <div key={cc.id}>{cc.content}</div>
            ))}
        </>
    )
}
