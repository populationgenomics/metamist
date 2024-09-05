import { AvatarGroup, Box, Button, Divider, Typography } from '@mui/material'

import { useEffect, useRef, useState } from 'react'
import { Comment } from './Comment'
import { CommentAvatar } from './CommentAvatar'
import { CommentData, CommentThreadData, getCommentEntityId } from './commentConfig'
import { CommentEditor } from './CommentEditor'
import { CommentEntityLink } from './CommentEntityLink'
import { parseAuthor } from './commentUtils'
import { useAddCommentToThread } from './data/commentMutations'

function commentIdInComments(id: number, comments: CommentData[]) {
    return comments.some((cc) => cc.id === id)
}

export function CommentThread(props: {
    comment: CommentThreadData
    commentToShow: CommentThreadData | CommentData | null
    prevComment?: CommentThreadData
    canComment: boolean
    viewerUser: string | null
    showEntityInfo: boolean
    projectName: string
}) {
    const { comment, prevComment, canComment, showEntityInfo, commentToShow, viewerUser } = props

    const threadContainsCommentToShow =
        commentToShow && commentIdInComments(commentToShow.id, comment.thread)

    const [showThread, setShowThread] = useState(threadContainsCommentToShow)
    const [isReplying, setIsReplying] = useState(false)
    const [replyContent, setReplyContent] = useState('')
    const replyFormRef = useRef<HTMLTextAreaElement>(null)
    const commentToShowRef = useRef<HTMLDivElement>(null)

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

    // Scroll down to the reply form after reply is clicked
    useEffect(() => {
        if (replyFormRef.current && showThread && isReplying) {
            replyFormRef.current.scrollIntoView({ block: 'center' })
            replyFormRef.current.focus()
        }
    }, [showThread, isReplying])

    // Scroll to the comment that is specified in the URL
    useEffect(() => {
        console.log(commentToShowRef.current)
        if (commentToShowRef.current) {
            commentToShowRef.current.scrollIntoView({ block: 'center' })
        }
    }, [commentToShow?.id, commentToShowRef.current])

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
                // Reset the content of the textarea so that another comment can be written easily
                setReplyContent('')
            })
            // This error is shown as an alert below the button, this console is just for debugging
            .catch((err) => console.error(err))
    }

    return (
        <Box>
            {/* Show the entity that the comment is on for context's sake */}
            {shouldShowEntityInfo && (
                <>
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
                highlighted={comment.id === commentToShow?.id}
                ref={comment.id === commentToShow?.id ? commentToShowRef : undefined}
                isTopLevel={true}
                onReply={onReply}
            />

            {/* Comment thread */}
            {hasReplies && (
                <>
                    <Box display={'flex'} onClick={onToggleReplies}>
                        <AvatarGroup max={4}>
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
                                    highlighted={cc.id === commentToShow?.id}
                                    ref={cc.id === commentToShow?.id ? commentToShowRef : undefined}
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
            <Box my={3}>
                <Divider />
            </Box>
        </Box>
    )
}
