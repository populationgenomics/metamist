import RobotIcon from '@mui/icons-material/SmartToy'
import {
    Alert,
    Avatar,
    AvatarGroup,
    Box,
    Button,
    Divider,
    SxProps,
    TextField,
    Typography,
} from '@mui/material'

import { DateTime } from 'luxon'
import React, { forwardRef, useEffect, useRef, useState } from 'react'
import Markdown from 'react-markdown'
import SyntaxHighlighter from 'react-syntax-highlighter'
import { dracula, vs } from 'react-syntax-highlighter/dist/esm/styles/prism'
import remarkGfm from 'remark-gfm'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import {
    CommentData,
    commentEntityTypeMap,
    CommentThreadData,
    getCommentEntityId,
} from './commentConfig'
import { CommentEntityLink } from './CommentEntityLink'
import { useAddCommentToThread, useUpdateComment } from './commentMutations'

const toTitleCase = (str: string) => {
    return str
        .split(' ')
        .map((ss) => `${ss.charAt(0).toLocaleUpperCase()}${ss.substring(1).toLocaleLowerCase()}`)
        .join(' ')
}

const colorFromStr = (str: string) => {
    let hash = 0
    if (str.length === 0) return `hsl(0 0 50)`
    for (let i = 0; i < str.length; i++) {
        hash = str.charCodeAt(i) + ((hash << 5) - hash)
        hash = hash & hash
    }
    return `hsl(${hash % 360} 30 50)`
}

const serviceAccountStr = '.iam.gserviceaccount.com'

// Convert the author email to something that is a bit more friendly
// Makes an attempt at converting service accounts to something readable
const parseAuthor = (author: string) => {
    const authorParts = author.split('@')
    const username = authorParts[0]
    const domain = authorParts[1]
    const isMachineUser = domain && domain.endsWith(serviceAccountStr)

    let name = ''

    if (isMachineUser) {
        name = toTitleCase(
            author.replace(serviceAccountStr, '').replace('@', ' @ ').replace(/-/g, ' ')
        )
    } else {
        name = toTitleCase(username.replace(/[.-]/g, ' '))
    }

    const initials = name
        .split(' ')
        .map((nn) => nn[0])
        .join('')
        .slice(0, 2)

    return {
        username: author,
        isMachineUser,
        name,
        initials,
    }
}

function CommentAvatar(props: ReturnType<typeof parseAuthor> & { size: 'full' | 'small' }) {
    const { isMachineUser, username, initials, name, size } = props
    const styleProps: SxProps =
        size === 'full'
            ? { width: 32, height: 32, fontSize: 14 }
            : { width: 28, height: 28, fontSize: 8 }
    return (
        <Avatar
            sx={{ bgcolor: colorFromStr(username), fontWeight: 'bold', ...styleProps }}
            alt={name}
        >
            {isMachineUser ? <RobotIcon /> : initials}
        </Avatar>
    )
}

export const CommentEditor = forwardRef<
    HTMLTextAreaElement,
    {
        content: string
        label?: string
        onChange: (content: string) => void
    }
>(function CommentEditor(props, ref) {
    return (
        <Box>
            <TextField
                inputRef={ref}
                label={props.label}
                multiline
                value={props.content}
                fullWidth
                onChange={(e) => props.onChange(e.target.value)}
            />
        </Box>
    )
})

function Comment(props: {
    comment: CommentData
    canComment: boolean
    viewerUser: string | null
    isTopLevel: boolean
    onReply: () => void
}) {
    const { comment, canComment, viewerUser, isTopLevel, onReply } = props
    const author = parseAuthor(comment.author)

    const canEdit = viewerUser === comment.author
    const [isEditing, setIsEditing] = useState(false)
    const [content, setContent] = useState(comment.content)

    const createdAt = DateTime.fromISO(comment.createdAt)
    const updatedAt = DateTime.fromISO(comment.updatedAt)
    const hasBeenEdited = comment.versions.length > 0

    const theme = React.useContext(ThemeContext)
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

    return (
        <Box component={'article'} my={2}>
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
                            <Markdown
                                remarkPlugins={[remarkGfm]}
                                components={{
                                    code({ node, inline, className, children, ...props_ }) {
                                        const match_ = /language-(\w+)/.exec(className || '')
                                        return !inline && match_ ? (
                                            <SyntaxHighlighter
                                                language={match_[1]}
                                                PreTag="div"
                                                // showLineNumbers={true}
                                                style={isDarkMode ? dracula : vs}
                                                {...props_}
                                            >
                                                {String(children).replace(/\n$/, '')}
                                            </SyntaxHighlighter>
                                        ) : (
                                            <code className={className} {...props_}>
                                                {children}
                                            </code>
                                        )
                                    },
                                }}
                            >
                                {content}
                            </Markdown>
                            <Box mt={2} display="flex">
                                {isTopLevel && canComment && (
                                    <Button
                                        variant="text"
                                        sx={{ fontSize: 12 }}
                                        onClick={() => onReply()}
                                    >
                                        Reply
                                    </Button>
                                )}
                                {canEdit && (
                                    <Button
                                        variant="text"
                                        sx={{ fontSize: 12 }}
                                        onClick={() => setIsEditing(!isEditing)}
                                    >
                                        Edit
                                    </Button>
                                )}

                                <Button variant="text" sx={{ fontSize: 12 }}>
                                    Copy Link
                                </Button>
                                <Button variant="text" sx={{ fontSize: 12 }}>
                                    View History
                                </Button>
                            </Box>
                        </Box>
                    )}
                </Box>
            </Box>
        </Box>
    )
}

export function CommentThread(props: {
    comment: CommentThreadData
    prevComment?: CommentThreadData
    canComment: boolean
    viewerUser: string | null
    showEntityInfo: boolean
    projectName: string
}) {
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
                        on {commentEntityTypeMap[comment.entity.__typename].name}{' '}
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
