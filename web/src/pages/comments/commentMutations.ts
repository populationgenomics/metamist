import {
    DocumentNode,
    FetchResult,
    OperationVariables,
    TypedDocumentNode,
    useMutation,
} from '@apollo/client'
import { gql } from '../../__generated__'
import { CommentFragmentFragment, GraphQlComment } from '../../__generated__/graphql'
import { COMMENT_FRAGMENT } from './commentQueries'

const ADD_COMMENT_TO_THREAD = gql(`
    mutation AddCommentToThread($parentId: Int!, $content: String!) {
        comment {
            addCommentToThread(parentId: $parentId, content: $content) {
                ...CommentFragment
            }
        }
    }
`)

export function useAddCommentToThread(id: number) {
    return useMutation(ADD_COMMENT_TO_THREAD, {
        update(cache, result) {
            const newComment = result.data?.comment.addCommentToThread
            if (!newComment) return

            cache.modify<GraphQlComment>({
                id: `GraphQLComment:${id}`,
                fields: {
                    thread(existingCommentRefs = []) {
                        const newCommentRef = cache.writeFragment({
                            data: newComment,
                            fragment: COMMENT_FRAGMENT,
                            fragmentName: 'CommentFragment',
                        })

                        return [...existingCommentRefs, newCommentRef]
                    },
                },
            })
        },
    })
}

const UPDATE_COMMENT = gql(`
    mutation UpdateComment($id: Int!, $content: String!) {
        comment {
            updateComment(id: $id, content: $content) {
                ...CommentFragment

                thread {
                    ...CommentFragment
                }
            }
        }
    }
`)

export function useUpdateComment() {
    return useMutation(UPDATE_COMMENT)
}

export function useNewComment<TData, TVariables = OperationVariables>(
    query: DocumentNode | TypedDocumentNode<TData, TVariables>,
    cacheId: string | null,
    commentGetter: (
        data: NonNullable<Omit<FetchResult<TData>, 'context'>['data']>
    ) => CommentFragmentFragment
) {
    return useMutation(query, {
        update(cache, result) {
            if (!cacheId || !result || !result.data) return

            const newComment = commentGetter(result.data)

            if (!newComment) return

            cache.modify({
                id: cacheId,
                fields: {
                    discussion(existingDiscussion) {
                        const newCommentRef = cache.writeFragment({
                            data: newComment,
                            fragment: COMMENT_FRAGMENT,
                            fragmentName: 'CommentFragment',
                        })

                        return {
                            ...existingDiscussion,
                            directComments: (existingDiscussion.directComments || []).concat(
                                newCommentRef
                            ),
                        }
                    },
                },
            })
        },
    })
}
