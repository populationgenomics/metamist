import RobotIcon from '@mui/icons-material/SmartToy'
import { Avatar, AvatarGroup, Box, Button, Divider, SxProps, Typography } from '@mui/material'
import { DateTime } from 'luxon'
import React, { useState } from 'react'
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
    const isMachineUser = domain && domain.includes(serviceAccountStr)

    let name = ''

    if (isMachineUser) {
        name = toTitleCase(
            author.replace(serviceAccountStr, '').replace('@', ' @ ').replace(/-/g, ' ')
        )
    } else {
        name = toTitleCase(username.replace(/[\.-]/g, ' '))
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

function Comment(props: { comment: CommentData }) {
    const { comment } = props
    const author = parseAuthor(comment.author)

    const createdAt = DateTime.fromISO(comment.createdAt)
    const updatedAt = DateTime.fromISO(comment.updatedAt)

    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'

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
                            {!createdAt.equals(updatedAt) && (
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
                                <code className={className} {...props}>
                                    {children}
                                </code>
                            )
                        },
                    }}
                >
                    {comment.content}
                </Markdown>
            </Box>
        </Box>
    )
}

export function CommentThread(props: {
    comment: CommentThreadData
    prevComment?: CommentThreadData
    showEntityInfo: boolean
}) {
    const { comment, prevComment, showEntityInfo } = props
    const [expanded, setExpanded] = useState(false)
    const replyCount = comment.thread.length
    const replyAuthors = [...new Set(comment.thread.flatMap((comment) => comment.author))]

    // Only show the entity info if it wasn't already displayed on the previous comment
    const sameEntityAsPreviousComment =
        prevComment && getCommentEntityId(prevComment) === getCommentEntityId(comment)
    const shouldShowEntityInfo = showEntityInfo && !sameEntityAsPreviousComment

    return (
        <Box mb={2} px={2}>
            {shouldShowEntityInfo && (
                <>
                    {prevComment && <Divider />}
                    <Typography fontStyle={'italic'} mt={1}>
                        on {commentEntityTypeMap[comment.entity.__typename]}{' '}
                        <CommentEntityLink comment={comment} />
                    </Typography>
                </>
            )}
            <Comment comment={comment} />
            <Box display={'flex'} onClick={() => setExpanded(!expanded)}>
                <AvatarGroup>
                    {replyAuthors.map((authorStr) => (
                        <CommentAvatar {...parseAuthor(authorStr)} size={'small'} />
                    ))}
                </AvatarGroup>
                {replyCount > 0 && (
                    <Button variant="text">
                        {expanded ? 'Hide' : 'Show'} {replyCount}{' '}
                        {replyCount > 1 ? 'replies' : 'reply'}
                    </Button>
                )}
            </Box>
            {expanded && (
                <Box ml={2} pl={2} borderLeft={'2px solid var(--color-border-color)'}>
                    {comment.thread.map((cc) => (
                        <Comment comment={cc} />
                    ))}
                </Box>
            )}
        </Box>
    )
}
