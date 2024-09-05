import {
    Alert,
    Box,
    Button,
    Modal,
    Link as MuiLink,
    LinkProps as MuiLinkProps,
    Typography,
} from '@mui/material'
import { DateTime } from 'luxon'
import { forwardRef, useContext, useEffect, useState } from 'react'
import { useLocation } from 'react-router-dom'
import useCopyToClipboard from 'react-use/esm/useCopyToClipboard'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import { CommentAvatar } from './CommentAvatar'
import { CommentData } from './commentConfig'
import { CommentContent } from './CommentContent'
import { CommentEditor } from './CommentEditor'
import { CommentHistory } from './CommentHistory'
import { getCommentLink, parseAuthor } from './commentUtils'
import { useUpdateComment } from './data/commentMutations'

function CommentAction(props: MuiLinkProps & { preventDefault?: boolean }) {
    const { preventDefault, ...otherProps } = props
    return (
        <MuiLink
            href="#"
            sx={{ fontSize: 12, mr: 2 }}
            {...otherProps}
            onClick={(e) => {
                if (preventDefault) {
                    e.preventDefault()
                    e.stopPropagation()
                }
                if (props.onClick) props.onClick(e)
            }}
        ></MuiLink>
    )
}

export const Comment = forwardRef<
    HTMLDivElement,
    {
        comment: CommentData
        highlighted: boolean
        canComment: boolean
        viewerUser: string | null
        isTopLevel: boolean
        onReply: () => void
    }
>(function Comment(props, ref) {
    const { comment, canComment, viewerUser, isTopLevel, onReply } = props
    const author = parseAuthor(comment.author)

    const canEdit = viewerUser === comment.author
    const [isEditing, setIsEditing] = useState(false)
    const [content, setContent] = useState(comment.content)
    const [copyLinkState, copyLinkToClipboard] = useCopyToClipboard()
    const [showCopiedState, setShowCopiedState] = useState(true)
    const [isShowingHistory, setIsShowingHistory] = useState(false)
    const location = useLocation()

    // Hide the "Copied!" notification after a couple of seconds
    useEffect(() => {
        let timeout = null
        if (copyLinkState.value) {
            timeout = setTimeout(() => {
                setShowCopiedState(false)
            }, 1500)
        }

        return () => {
            if (timeout) clearTimeout(timeout)
        }
    }, [copyLinkState])

    const createdAt = DateTime.fromISO(comment.createdAt)
    const updatedAt = DateTime.fromISO(comment.updatedAt)
    const hasBeenEdited = comment.versions.length > 0

    const theme = useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'

    const [updateCommentMutation, updateCommentResult] = useUpdateComment()

    const saveComment = () => {
        // Don't do anything if already loading
        if (updateCommentResult.loading) return

        updateCommentMutation({
            variables: {
                id: comment.id,
                content: content,
            },
        }).then(() => {
            setIsEditing(false)
        })
    }

    const commentLink = getCommentLink(comment.id, location)

    return (
        <Box
            component={'article'}
            my={2}
            ref={ref}
            position={'relative'}
            sx={{
                // Highlight the comment by placing a coloured square behind it
                '&:before': {
                    content: props.highlighted ? `" "` : 'initial',
                    position: 'absolute',
                    borderRadius: 1,
                    overflow: 'hidden',
                    top: -10,
                    right: -10,
                    left: -10,
                    bottom: -10,
                    bgcolor: 'var(--color-bg-highlight)',
                    zIndex: -1,
                },
            }}
        >
            <Box component="header">
                <Box display={'flex'}>
                    <CommentAvatar {...author} size="full" />
                    <Box pl={1} overflow={'hidden'}>
                        <Typography
                            fontSize={14}
                            noWrap={true}
                            overflow={'hidden'}
                            textOverflow={'ellipsis'}
                            fontWeight={'bold'}
                            component={'h1'}
                            title={comment.author}
                        >
                            {author.name}
                        </Typography>

                        <Box display={'flex'}>
                            <Typography
                                fontSize={12}
                                component={'span'}
                                title={createdAt.toISO({ includeOffset: true }) ?? undefined}
                            >
                                {createdAt.toRelative()}
                            </Typography>
                            {hasBeenEdited && (
                                <Typography
                                    fontSize={12}
                                    ml={1}
                                    color={'var(--color-text-medium)'}
                                    component={'span'}
                                    title={updatedAt.toISO({ includeOffset: true }) ?? undefined}
                                >
                                    edited: {updatedAt.toRelative()}
                                </Typography>
                            )}
                        </Box>
                    </Box>
                </Box>
            </Box>
            <Box mt={1} pl={5}>
                <Box>
                    {isEditing ? (
                        <Box>
                            <CommentEditor
                                content={content}
                                onChange={(content) => setContent(content)}
                            />

                            <Box mt={2} display="flex" gap={2}>
                                <Button
                                    variant="contained"
                                    sx={{ fontSize: 12 }}
                                    color={'info'}
                                    disabled={updateCommentResult.loading}
                                    onClick={saveComment}
                                >
                                    {updateCommentResult.loading ? 'Saving' : 'Save'}
                                </Button>
                                <Button
                                    variant="contained"
                                    sx={{ fontSize: 12 }}
                                    color={'secondary'}
                                    onClick={() => {
                                        setContent(comment.content)
                                        setIsEditing(false)
                                        updateCommentResult.reset()
                                    }}
                                >
                                    Cancel
                                </Button>
                            </Box>
                            {updateCommentResult.error && (
                                <Box my={1}>
                                    <Alert severity="error">
                                        Error saving comment {updateCommentResult.error.message}
                                    </Alert>
                                </Box>
                            )}
                        </Box>
                    ) : (
                        <Box>
                            <CommentContent
                                content={content}
                                theme={isDarkMode ? 'dark' : 'light'}
                            />
                            <Box mt={2} display="flex">
                                {isTopLevel && canComment && (
                                    <CommentAction onClick={() => onReply()}>Reply</CommentAction>
                                )}
                                {canEdit && (
                                    <CommentAction onClick={() => setIsEditing(!isEditing)}>
                                        Edit
                                    </CommentAction>
                                )}
                                <CommentAction
                                    href={commentLink}
                                    preventDefault={false}
                                    onClick={(e) => {
                                        // Don't prevent default if meta key is held
                                        // to keep default behaviour of opening the link
                                        if (!e.metaKey) {
                                            e.preventDefault()
                                            e.stopPropagation()
                                            setShowCopiedState(true)
                                            copyLinkToClipboard(commentLink)
                                        }
                                    }}
                                >
                                    {copyLinkState.value && showCopiedState
                                        ? 'Copied!'
                                        : 'Copy Link'}
                                </CommentAction>
                                {comment.versions.length > 0 && (
                                    <CommentAction onClick={() => setIsShowingHistory(true)}>
                                        View Edits
                                    </CommentAction>
                                )}
                            </Box>
                        </Box>
                    )}
                </Box>
            </Box>
            <Modal
                open={isShowingHistory}
                onClose={() => setIsShowingHistory(false)}
                disableEnforceFocus
            >
                <Box>
                    <CommentHistory comment={comment} theme={isDarkMode ? 'dark' : 'light'} />
                </Box>
            </Modal>
        </Box>
    )
})
