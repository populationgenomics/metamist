import { DiscussionFragmentFragment } from '../../__generated__/graphql'
import AssayIcon from '../../shared/components/icons/AssayIcon'
import FamilyIcon from '../../shared/components/icons/FamilyIcon'
import ParticipantIcon from '../../shared/components/icons/ParticipantIcon'
import ProjectIcon from '../../shared/components/icons/ProjectIcon'
import SampleIcon from '../../shared/components/icons/SampleIcon'
import SequencingGroupIcon from '../../shared/components/icons/SequencingGroupIcon'

export type CommentEntityType = CommentThreadData['entity']['__typename']
export type CommentThreadData = DiscussionFragmentFragment['directComments'][number]
export type CommentData = Omit<CommentThreadData, 'thread'>

export const commentEntityTypeMap = {
    GraphQLProject: {
        name: 'Project',
        namePlural: 'Projects',
        Icon: ProjectIcon,
    },
    GraphQLFamily: {
        name: 'Family',
        namePlural: 'Families',
        Icon: FamilyIcon,
    },
    GraphQLParticipant: {
        name: 'Participant',
        namePlural: 'Participants',
        Icon: ParticipantIcon,
    },
    GraphQLSample: {
        name: 'Sample',
        namePlural: 'Samples',
        Icon: SampleIcon,
    },
    GraphQLAssay: {
        name: 'Assay',
        namePlural: 'Assays',
        Icon: AssayIcon,
    },
    GraphQLSequencingGroup: {
        name: 'Sequencing Group',
        namePlural: 'Sequencing Groups',
        Icon: SequencingGroupIcon,
    },
} as const

export const commentEntityOrder = Object.keys(commentEntityTypeMap) as CommentEntityType[]

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
