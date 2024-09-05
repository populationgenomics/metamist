import { AvatarGroup, Box, Button, Divider, Typography } from '@mui/material'

import { useEffect, useRef, useState } from 'react'
import { Comment, parseAuthor } from './Comment'
import { CommentAvatar } from './CommentAvatar'
import { CommentThreadData, getCommentEntityId } from './commentConfig'
import { CommentEditor } from './CommentEditor'
import { CommentEntityLink } from './CommentEntityLink'
import { useAddCommentToThread } from './data/commentMutations'

export function CommentThread(props: {
    comment: CommentThreadData
    prevComment?: CommentThreadData
    canComment: boolean
    viewerUser: string | null
    showEntityInfo: boolean
    projectName: string
}) {
    console.log('comment thread')
    const { comment, prevComment, canComment, showEntityInfo, viewerUser } = props

    const [showThread, setShowThread] = useState(false)
    const [isReplying, setIsReplying] = useState(false)
    const [replyContent, setReplyContent] = useState('')
    const replyFormRef = useRef<HTMLTextAreaElement>(null)

    const [addCommentToThreadMutation, addCommentToThreadResult] = useAddCommentToThread(comment.id)

    const replyCount = comment.thread.length
    const hasReplies = replyCount > 0
    const replyAuthors = [...new Set(comment.thread.flatMap((comment) => comment.author))]

    // Only show the entity info if it wasn't already displayed on the previous comment
    const sameEntityAsPreviousComment =
        prevComment && getCommentEntityId(prevComment) === getCommentEntityId(comment)
    const shouldShowEntityInfo = showEntityInfo && !sameEntityAsPreviousComment

    const onReply = () => {
        setShowThread(true)
        setIsReplying(true)

        if (replyFormRef.current) {
            replyFormRef.current.scrollIntoView({ block: 'center' })
            replyFormRef.current.focus()
        }
    }

    const onToggleReplies = () => {
        // If this is closing the thread, then set replying to false
        if (showThread) setIsReplying(false)
        setShowThread(!showThread)
    }

    useEffect(() => {
        if (replyFormRef.current && showThread && isReplying) {
            replyFormRef.current.scrollIntoView({ block: 'center' })
            replyFormRef.current.focus()
        }
    }, [showThread, isReplying])

    const addCommentToThread = () => {
        // Don't do anything if already loading
        if (addCommentToThreadResult.loading || !replyContent) return

        addCommentToThreadMutation({
            variables: {
                parentId: comment.id,
                content: replyContent,
            },
        })
            .then(() => {
                setReplyContent('')
            })
            .catch((err) => console.error(err))
    }

    return (
        <Box mb={2}>
            {/* Show the entity that the comment is on for context's sake */}
            {shouldShowEntityInfo && (
                <>
                    {prevComment && <Divider />}
                    <Typography fontStyle={'italic'} mt={1}>
                        <CommentEntityLink comment={comment} />
                    </Typography>
                </>
            )}

            {/* The comment itself */}
            <Comment
                comment={comment}
                canComment={canComment}
                viewerUser={viewerUser}
                isTopLevel={true}
                onReply={onReply}
            />

            {/* Comment thread */}
            {hasReplies && (
                <>
                    <Box display={'flex'} onClick={onToggleReplies}>
                        <AvatarGroup>
                            {replyAuthors.map((authorStr) => (
                                <CommentAvatar
                                    key={authorStr}
                                    {...parseAuthor(authorStr)}
                                    size={'small'}
                                />
                            ))}
                        </AvatarGroup>

                        <Button variant="text">
                            {showThread ? 'Hide' : 'Show'} {replyCount}{' '}
                            {replyCount > 1 ? 'replies' : 'reply'}
                        </Button>
                    </Box>
                    {showThread && (
                        <Box ml={2} pl={2} borderLeft={'2px solid var(--color-border-color)'}>
                            {comment.thread.map((cc) => (
                                <Comment
                                    key={cc.id}
                                    comment={cc}
                                    canComment={canComment}
                                    viewerUser={viewerUser}
                                    isTopLevel={false}
                                    onReply={onReply}
                                />
                            ))}
                        </Box>
                    )}
                </>
            )}
            {showThread && (
                <Box ml={4}>
                    <CommentEditor
                        ref={replyFormRef}
                        label="Reply"
                        content={replyContent}
                        onChange={(content) => setReplyContent(content)}
                    />

                    <Box mt={2} display={'flex'} gap={2}>
                        <Button
                            variant="contained"
                            sx={{ fontSize: 12 }}
                            color={'info'}
                            disabled={addCommentToThreadResult.loading}
                            onClick={addCommentToThread}
                        >
                            {addCommentToThreadResult.loading ? 'Saving' : 'Save'}
                        </Button>
                        {/* Only show cancel button if there's no replies */}
                        {comment.thread.length === 0 && (
                            <Button
                                variant="contained"
                                sx={{ fontSize: 12 }}
                                color={'secondary'}
                                onClick={onToggleReplies}
                            >
                                Cancel
                            </Button>
                        )}
                    </Box>
                </Box>
            )}
        </Box>
    )
}
