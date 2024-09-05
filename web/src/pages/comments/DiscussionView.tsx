import { ApolloError } from '@apollo/client'
import AnnouncementIcon from '@mui/icons-material/Announcement'
import ChatIcon from '@mui/icons-material/Chat'
import {
    Alert,
    Badge,
    Box,
    Button,
    CircularProgress,
    List,
    ListItem,
    ListItemButton,
    ListItemIcon,
    Tooltip,
} from '@mui/material'
import { useContext, useState } from 'react'
import { DiscussionFragmentFragment, ProjectMemberRole } from '../../__generated__/graphql'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { ViewerContext } from '../../viewer'
import {
    commentEntityOrder,
    CommentEntityType,
    commentEntityTypeMap,
    CommentThreadData,
} from './commentConfig'
import { CommentEditor, CommentThread } from './CommentThread'

// Count how many comments are in a list of comments, including threaded ones
function countComments(comments: CommentThreadData[]) {
    return comments.reduce(
        (rr: { topLevel: number; total: number }, cc) => {
            return {
                topLevel: rr.topLevel + 1,
                total: rr.total + 1 + cc.thread.length,
            }
        },
        { topLevel: 0, total: 0 }
    )
}

export function DiscussionView(props: {
    discussionEntityType: CommentEntityType | undefined
    discussionLoading: boolean
    discussionError: ApolloError | undefined
    onToggleCollapsed: (collapsed: boolean) => void
    collapsed: boolean
    addingCommentLoading: boolean
    addingCommentError: ApolloError | undefined
    discussion: DiscussionFragmentFragment | undefined
    onReload: () => void
    onAddComment: (content: string) => Promise<void>
    projectName: string
}) {
    const {
        discussion,
        discussionError,
        discussionLoading,
        discussionEntityType,
        addingCommentLoading,
        addingCommentError,
        collapsed,
        onReload,
        onAddComment,
        projectName,
    } = props
    const [commentContent, setCommentContent] = useState('')
    const [selectedEntityType, setSelectedEntityType] = useState<CommentEntityType | undefined>(
        undefined
    )
    const selectedEntityTypeWithFallback = selectedEntityType || discussionEntityType

    const viewer = useContext(ViewerContext)

    const addComment = () => {
        onAddComment(commentContent)
            .then(() => {
                setCommentContent('')
            })
            .catch((err) => {
                console.error('Error adding comment', err)
            })
    }

    const canComment =
        viewer?.checkProjectAccessByName(projectName, [
            ProjectMemberRole.Writer,
            ProjectMemberRole.Contributor,
        ]) ?? false

    const groupedRelatedCommentsMap = discussion
        ? discussion.relatedComments.reduce((gg: Map<string, CommentThreadData[]>, cc) => {
              const entityType = cc.entity.__typename
              return gg.set(entityType, (gg.get(entityType) || []).concat(cc))
          }, new Map())
        : new Map()

    const groupedRelatedComments: [CommentEntityType, CommentThreadData[]][] = []

    for (const [_entityType, comments] of groupedRelatedCommentsMap) {
        const entityType = _entityType as CommentEntityType
        groupedRelatedComments.push([entityType, comments])
    }

    const directComments: [CommentEntityType, CommentThreadData[]][] =
        discussion && discussionEntityType
            ? [[discussionEntityType, discussion.directComments]]
            : []

    const sortedGroupedRelatedComments: [CommentEntityType, CommentThreadData[]][] = [
        ...directComments,
        ...groupedRelatedComments.sort(
            (a, b) => commentEntityOrder.indexOf(a[0]) - commentEntityOrder.indexOf(b[0])
        ),
    ]

    const selectedComments = sortedGroupedRelatedComments.find(
        ([entityType]) => entityType === selectedEntityTypeWithFallback
    )?.[1]

    const navbarWidth = 64

    let content = null

    if (discussionLoading && !collapsed) {
        content = <LoadingDucks />
    } else if (discussionError && !collapsed) {
        content = (
            <Box p={2}>
                <Alert
                    severity="error"
                    action={
                        <Button color="inherit" size="small" onClick={() => onReload()}>
                            Retry
                        </Button>
                    }
                >
                    Error loading comments: {discussionError.message}
                </Alert>
            </Box>
        )
    } else if (!collapsed) {
        content = (
            <>
                {selectedComments?.map((cc, index) => (
                    <CommentThread
                        key={cc.id}
                        comment={cc}
                        prevComment={selectedComments[index - 1]}
                        canComment={canComment}
                        viewerUser={viewer?.username ?? null}
                        showEntityInfo={selectedEntityTypeWithFallback !== discussionEntityType}
                        projectName={projectName}
                    />
                ))}
                {selectedEntityTypeWithFallback === discussionEntityType && (
                    <>
                        <Box>
                            <CommentEditor
                                content={commentContent}
                                onChange={(content) => setCommentContent(content)}
                            />
                            <Box mt={2} display="flex" gap={2}>
                                <Button
                                    variant="contained"
                                    sx={{ fontSize: 12 }}
                                    color={'info'}
                                    disabled={addingCommentLoading}
                                    onClick={() => addComment()}
                                >
                                    {addingCommentLoading ? 'Saving' : 'Save'}
                                </Button>
                            </Box>
                        </Box>
                        {addingCommentError && (
                            <Box my={1}>
                                <Alert severity="error">
                                    Error saving comment {addingCommentError.message}
                                </Alert>
                            </Box>
                        )}
                    </>
                )}
            </>
        )
    }

    let commentIcon = null
    if (discussionLoading) {
        commentIcon = <CircularProgress size={30} />
    } else if (discussionError) {
        commentIcon = <AnnouncementIcon color="error" fontSize="large" />
    } else {
        commentIcon = <ChatIcon fontSize={'large'} />
    }

    return (
        <>
            <Box
                position={'absolute'}
                top={0}
                left={0}
                height={'100%'}
                borderRight={'1px solid var(--color-border-color)'}
            >
                <List sx={{ width: '100%' }} disablePadding>
                    <ListItem
                        disablePadding
                        sx={{ borderBottom: '1px solid var(--color-border-color)' }}
                    >
                        <ListItemButton
                            sx={{ p: 0, py: 3 }}
                            onClick={() => {
                                props.onToggleCollapsed(!props.collapsed)
                            }}
                        >
                            <ListItemIcon sx={{ justifyContent: 'center' }}>
                                {commentIcon}
                            </ListItemIcon>
                        </ListItemButton>
                    </ListItem>
                    {sortedGroupedRelatedComments.map(([entityType, comments]) => {
                        const Icon = commentEntityTypeMap[entityType].Icon
                        const name = commentEntityTypeMap[entityType].name
                        const namePlural = commentEntityTypeMap[entityType].namePlural
                        const count = countComments(comments)

                        const tooltip =
                            entityType === discussionEntityType
                                ? `Comments on this ${name}`
                                : `Comments on related ${namePlural}`

                        return (
                            <Tooltip placement={'left'} title={tooltip} arrow key={entityType}>
                                <ListItem
                                    key={entityType}
                                    disablePadding
                                    sx={{ borderBottom: '1px solid var(--color-border-color)' }}
                                >
                                    <ListItemButton
                                        sx={{ p: 0, py: 2 }}
                                        onClick={() => {
                                            setSelectedEntityType(entityType)
                                            props.onToggleCollapsed(false)
                                        }}
                                        style={{
                                            background:
                                                entityType === selectedEntityTypeWithFallback &&
                                                !props.collapsed
                                                    ? 'var(--color-border-color)'
                                                    : undefined,
                                        }}
                                    >
                                        <ListItemIcon sx={{ justifyContent: 'center' }}>
                                            <Badge badgeContent={count.total} color="primary">
                                                <Icon fontSize={'large'} />
                                            </Badge>
                                        </ListItemIcon>
                                    </ListItemButton>
                                </ListItem>
                            </Tooltip>
                        )
                    })}
                </List>
            </Box>
            <Box
                position={'absolute'}
                left={navbarWidth}
                height={'100%'}
                right={0}
                overflow={'auto'}
            >
                {content}
            </Box>
        </>
    )
}
