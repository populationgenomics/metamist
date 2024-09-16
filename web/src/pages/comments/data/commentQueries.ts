import { gql } from '../../../__generated__/gql'

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

        entity {
            ... CommentEntityFragment
        }

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
            projectName: name
        }
        ... on GraphQLSample {
            sampleId: id
            sampleExternalId: externalId
        }
        ... on GraphQLSequencingGroup {
            sequencingGroupId: id
            sample {
                id
            }
        }
        ... on GraphQLAssay {
            assayId: id
            sample {
                id
                project {
                    name
                }
            }
        }
        ... on GraphQLParticipant {
            participantId:id
            participantExternalId: externalId
        }
        ... on GraphQLFamily {
            familyId: id
            familyExternalId: externalId
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
        }

        relatedComments {
            ... CommentFragment

            thread {
                ... CommentFragment
            }
        }
    }
`)
