import { Alert, Box, Button, Typography } from '@mui/material'
import { DateTime } from 'luxon'
import { useContext, useState } from 'react'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import { CommentAvatar } from './CommentAvatar'
import { CommentData } from './commentConfig'
import { CommentContent } from './CommentContent'
import { CommentEditor } from './CommentEditor'
import { useUpdateComment } from './data/commentMutations'

const toTitleCase = (str: string) => {
    return str
        .split(' ')
        .map((ss) => `${ss.charAt(0).toLocaleUpperCase()}${ss.substring(1).toLocaleLowerCase()}`)
        .join(' ')
}

const serviceAccountStr = '.iam.gserviceaccount.com'

// Convert the author email to something that is a bit more friendly
// Makes an attempt at converting service accounts to something readable
export const parseAuthor = (author: string) => {
    const authorParts = author.split('@')
    const username = authorParts[0]
    const domain: string | undefined = authorParts[1]
    const isMachineUser = domain ? domain.endsWith(serviceAccountStr) : false

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

export function Comment(props: {
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
                            <CommentContent
                                content={content}
                                theme={isDarkMode ? 'dark' : 'light'}
                            />
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
