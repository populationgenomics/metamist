import { Alert, AvatarGroup, Box, Button, Divider, Typography } from '@mui/material'

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
    // commentToShow is set when a deep link to a comment in the url, it is used
    // to scroll to and highlight a specific comment in the list
    commentToShow: CommentThreadData | CommentData | null
    prevComment?: CommentThreadData
    canComment: boolean
    viewerUser: string | null
    showEntityInfo: boolean
    projectName: string
}) {
    const { comment, prevComment, canComment, showEntityInfo, commentToShow, viewerUser } = props

    const thread = comment.thread.filter(
        (cc) => cc.author === viewerUser || comment.status === 'active'
    )

    // This is used to determine whether the thread needs to be expanded to show
    // the linked comment
    const threadContainsCommentToShow =
        commentToShow && commentIdInComments(commentToShow.id, thread)

    // Control state of whether thread is open or closed
    const [showThread, setShowThread] = useState(threadContainsCommentToShow)
    // This is set after a user clicks the reply button and is used in an effect
    // below to scroll to and focus the reply box
    const [isReplying, setIsReplying] = useState(false)
    // Manage the state of the reply content before it is submitted
    const [replyContent, setReplyContent] = useState('')
    // A reference to the reply form
    const replyFormRef = useRef<HTMLTextAreaElement>(null)
    // A reference to the comment that should be highlighted as it is deep linked, this is used
    // to scroll the pane down to the comment
    const commentToShowRef = useRef<HTMLDivElement>(null)

    const [addCommentToThreadMutation, addCommentToThreadResult] = useAddCommentToThread(comment.id)

    const replyCount = thread.length
    const hasReplies = replyCount > 0
    const replyAuthors = [...new Set(thread.flatMap((comment) => comment.author))]

    // Only show the entity info if it wasn't already displayed on the previous comment
    const sameEntityAsPreviousComment =
        prevComment && getCommentEntityId(prevComment) === getCommentEntityId(comment)
    const shouldShowEntityInfo = showEntityInfo && !sameEntityAsPreviousComment

    // Hide the thread and reset the replying state
    const onToggleReplies = () => {
        // If this is closing the thread, then set replying to false
        if (showThread) setIsReplying(false)
        setShowThread(!showThread)
    }

    const scrollAndFocusReplyBox = () => {
        if (replyFormRef.current) {
            const rect = replyFormRef.current.getBoundingClientRect()
            // only scroll into view if reply isn't already in view
            if (rect.top <= 0 || rect.bottom >= window.innerHeight) {
                replyFormRef.current.scrollIntoView({ block: 'center' })
            }
            replyFormRef.current.focus()
        }
    }

    const onReply = () => {
        setShowThread(true)
        setIsReplying(true)

        // Scroll to and focus the reply box, this works if the thread is already open
        // the useEffect below is used to do the scroll in the case where the thread was
        // closed as the ref won't exist when the thread is closed.
        scrollAndFocusReplyBox()
    }

    // Scroll down to the reply form after reply is clicked
    useEffect(() => {
        if (showThread && isReplying) {
            scrollAndFocusReplyBox()
        }
    }, [showThread, isReplying])

    // Scroll to the comment that is linked in the URL
    useEffect(() => {
        if (commentToShowRef.current) {
            commentToShowRef.current.scrollIntoView({ block: 'center' })
        }
    }, [commentToShow?.id])

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

    // Don't show deleted comments to anyone other than the author of the comment
    if (viewerUser !== comment.author && comment.status === 'deleted') return null

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
                replyCount={replyCount}
                canComment={canComment}
                viewerUser={viewerUser}
                highlighted={comment.id === commentToShow?.id}
                ref={comment.id === commentToShow?.id ? commentToShowRef : undefined}
                isTopLevel={true}
                onReply={onReply}
            />

            {/* Comment thread */}
            {hasReplies && comment.status === 'active' && (
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
                            {thread.map((cc) => (
                                <Comment
                                    key={cc.id}
                                    comment={cc}
                                    replyCount={0}
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
            {/* Form to add a new comment */}
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
                        {thread.length === 0 && (
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

                    {addCommentToThreadResult.error && (
                        <Box my={1}>
                            <Alert severity="error">
                                Error saving comment: {addCommentToThreadResult.error.message}
                            </Alert>
                        </Box>
                    )}
                </Box>
            )}
            <Box my={3}>
                <Divider />
            </Box>
        </Box>
    )
}
