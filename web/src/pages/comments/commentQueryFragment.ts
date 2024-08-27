import { gql } from '../../__generated__/gql'

export const VERSION_FRAGMENT = gql(`
    fragment CommentVersionFragment on GraphQLCommentVersion {
        content
        author
        status
        timestamp
    }
`)

export const COMMENT_FRAGMENT = gql(`
    fragment CommentFragment on GraphQLComment {
        id
        parentId
        author
        content
        createdAt
        updatedAt
        status

        versions {
            ... CommentVersionFragment
        }
    }    
`)

export const COMMENT_ENTITY_FRAGMENT = gql(`
    fragment CommentEntityFragment on GraphQLCommentEntity {
        __typename
        ... on GraphQLProject {
            projectId: id
        }
        ... on GraphQLSample {
            sampleId: id
        }
        ... on GraphQLSequencingGroup {
            sequencingGroupId: id
        }
        ... on GraphQLAssay {
            assayId: id
        }
        ... on GraphQLParticipant {
            participantId:id
        }
        ... on GraphQLFamily {
            participantId: id
        }
    }    
`)

export const DISCUSSION_FRAGMENT = gql(`
    fragment DiscussionFragment on GraphQLDiscussion {
        directComments {
            ... CommentFragment
            thread {
                ... CommentFragment
            }
            entity {
                ... CommentEntityFragment
            }
        }

        relatedComments {
            ... CommentFragment
            thread {
                ... CommentFragment
            }
            entity {
                ... CommentEntityFragment
            }
        }
    }
`)
