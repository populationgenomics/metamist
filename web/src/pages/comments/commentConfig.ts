import { DiscussionFragmentFragment } from '../../__generated__/graphql'

export type CommentEntityType = CommentThreadData['entity']['__typename']
export type CommentThreadData = DiscussionFragmentFragment['directComments'][number]
export type CommentData = Omit<CommentThreadData, 'thread'>

export const commentEntityTypeMap: Record<CommentEntityType, string> = {
    GraphQLAssay: 'Assay',
    GraphQLFamily: 'Family',
    GraphQLParticipant: 'Participant',
    GraphQLProject: 'Project',
    GraphQLSample: 'Sample',
    GraphQLSequencingGroup: 'Sequencing Group',
}

export function getCommentEntityId(comment: CommentThreadData): string {
    switch (comment.entity.__typename) {
        case 'GraphQLAssay':
            return `assay:${comment.entity.assayId}`

        case 'GraphQLFamily':
            return `family:${comment.entity.familyId}`

        case 'GraphQLParticipant':
            return `participant:${comment.entity.participantId}`

        case 'GraphQLProject':
            return `project:${comment.entity.projectId}`

        case 'GraphQLSample':
            return `sample:${comment.entity.sampleId}`
        case 'GraphQLSequencingGroup':
            return `sg:${comment.entity.sequencingGroupId}`
    }
}
