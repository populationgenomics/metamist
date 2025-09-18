import { ApolloError } from '@apollo/client'
import AnnouncementIcon from '@mui/icons-material/Announcement'
import ChatIcon from '@mui/icons-material/Chat'
import RefreshIcon from '@mui/icons-material/Refresh'
import {
    Alert,
    Badge,
    Box,
    Button,
    CircularProgress,
    Divider,
    IconButton,
    List,
    ListItem,
    ListItemButton,
    ListItemIcon,
    Tooltip,
    Typography,
} from '@mui/material'
import { useContext, useEffect, useState } from 'react'
import { useLocation } from 'react-router-dom'
import { DiscussionFragmentFragment, ProjectMemberRole } from '../../__generated__/graphql'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { ViewerContext } from '../../viewer'
import {
    commentEntityOrder,
    CommentEntityType,
    commentEntityTypeMap,
    CommentThreadData,
} from './commentConfig'
import { CommentEditor } from './CommentEditor'
import { CommentThread } from './CommentThread'

// Count how many comments are in a list of comments, including threaded ones
function countComments(comments: CommentThreadData[]) {
    return comments.reduce(
        (rr: { topLevel: number; total: number }, cc) => {
            // Don't count deleted comments
            if (cc.status === 'deleted') return rr
            return {
                topLevel: rr.topLevel + 1,
                total: rr.total + 1 + cc.thread.length,
            }
        },
        { topLevel: 0, total: 0 }
    )
}

function findCommentById(id: number, comments: CommentThreadData[]) {
    return comments.flatMap((cc) => [cc, ...cc.thread]).find((cc) => cc.id === id)
}

type CommentSectionType = 'direct' | 'related'
type CommentSection = {
    entityType: CommentEntityType
    comments: CommentThreadData[]
    sectionType: CommentSectionType
}
type SelectedCommentSection = { entityType: CommentEntityType; sectionType: CommentSectionType }

function commentHeading({ entityType, sectionType }: SelectedCommentSection) {
    const name = commentEntityTypeMap[entityType].name
    const namePlural = commentEntityTypeMap[entityType].namePlural
    return sectionType === 'direct'
        ? `Comments on this ${name}`
        : `Comments on related ${namePlural}`
}

export function DiscussionView(props: {
    discussionEntityType: CommentEntityType
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
        onToggleCollapsed,
    } = props

    // Check if there is a deep linked comment in the url
    const location = useLocation()
    const search = new URLSearchParams(location.search)
    const showComments = search.get('show_comments')
    const searchCommentId = search.get('comment_id')

    // If there is a comment specified in the URL then find it in the comments
    const commentIdToShow =
        !searchCommentId || isNaN(parseInt(searchCommentId, 10))
            ? null
            : parseInt(searchCommentId, 10)

    // Need to handle direct and related comments separately as you can have
    // direct and related comments of the same type
    const directCommentToShow = commentIdToShow
        ? findCommentById(commentIdToShow, discussion?.directComments ?? [])
        : null

    const relatedCommentToShow = commentIdToShow
        ? findCommentById(commentIdToShow, discussion?.relatedComments ?? [])
        : null

    // Hold state for the content of a new comment
    const [commentContent, setCommentContent] = useState('')

    // State for which section of comments is selected
    const [commentSection, setCommentSection] = useState<SelectedCommentSection | undefined>()

    // Provide fallback for the selected comment section, if none is selected then we want to show
    // the direct comments by default
    const commentSectionWithFallback = commentSection || {
        entityType: discussionEntityType,
        sectionType: 'direct',
    }

    const {viewer} = useContext(ViewerContext)

    // toggle the side panel open if the specified param is in the url
    useEffect(() => {
        if (['1', 'true', 'yes'].includes(showComments || '0')) {
            onToggleCollapsed(false)
        }
    }, [showComments, onToggleCollapsed])

    // Set the state for the entity type to show based on being linked in the URL
    useEffect(() => {
        if (directCommentToShow) {
            setCommentSection({
                entityType: directCommentToShow.entity.__typename,
                sectionType: 'direct',
            })
        } else if (relatedCommentToShow) {
            setCommentSection({
                entityType: relatedCommentToShow.entity.__typename,
                sectionType: 'related',
            })
        }
    }, [directCommentToShow, relatedCommentToShow])

    const addComment = () => {
        onAddComment(commentContent)
            .then(() => {
                setCommentContent('')
            })
            .catch((err) => {
                console.error('Error adding comment', err)
            })
    }

    // This is used to hide action buttons if the user isn't allowed to comment/edit/delete
    const canComment =
        viewer?.checkProjectAccessByName(projectName, [
            ProjectMemberRole.Writer,
            ProjectMemberRole.Contributor,
        ]) ?? false

    // Group related comments by their entity type.
    let groupedRelatedCommentsMap: Map<string, CommentThreadData[]> = new Map()

    if (discussion) {
        groupedRelatedCommentsMap = discussion.relatedComments.reduce(
            (gg: Map<string, CommentThreadData[]>, cc) => {
                const entityType = cc.entity.__typename
                return gg.set(entityType, (gg.get(entityType) || []).concat(cc))
            },
            new Map()
        )
    }

    // Convert the comments map to an array of entries, this is done kinda jankily as
    // typescript is silly and can't retain the key type when calling `.entries`
    const groupedRelatedComments: CommentSection[] = []

    for (const [_entityType, comments] of groupedRelatedCommentsMap) {
        const entityType = _entityType as CommentEntityType
        groupedRelatedComments.push({ entityType, comments, sectionType: 'related' })
    }

    // Add direct comments to the start of the list of related comments
    let directComments: CommentSection[] = []
    if (discussion && discussionEntityType) {
        directComments = [
            {
                entityType: discussionEntityType,
                comments: discussion.directComments,
                sectionType: 'direct',
            },
        ]
    }

    const sortedCommentSections = [
        ...directComments,
        ...groupedRelatedComments.sort(
            (a, b) =>
                commentEntityOrder.indexOf(a.entityType) - commentEntityOrder.indexOf(b.entityType)
        ),
    ]

    // Find which type of comment the user has selected
    const selectedComments = sortedCommentSections.find(
        ({ entityType, sectionType }) =>
            entityType === commentSectionWithFallback.entityType &&
            sectionType === commentSectionWithFallback.sectionType
    )?.comments

    // Determine stateful content rendering
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
            <Box py={4} px={2}>
                <Box display={'flex'} mb={2} alignItems={'center'}>
                    <Box flexGrow={1}>
                        {commentSectionWithFallback && (
                            <Typography variant={'h1'} fontSize={18} fontWeight={'bold'}>
                                {commentSectionWithFallback.entityType &&
                                    commentHeading(commentSectionWithFallback)}
                            </Typography>
                        )}
                    </Box>
                    <Box>
                        <Tooltip title={'Refresh comments'} arrow>
                            <IconButton onClick={() => onReload()}>
                                <RefreshIcon color={'primary'} />
                            </IconButton>
                        </Tooltip>
                    </Box>
                </Box>

                {selectedComments?.map((cc, index) => (
                    <CommentThread
                        key={cc.id}
                        comment={cc}
                        commentToShow={(directCommentToShow || relatedCommentToShow) ?? null}
                        prevComment={selectedComments[index - 1]}
                        canComment={canComment}
                        viewerUser={viewer?.username ?? null}
                        showEntityInfo={commentSectionWithFallback.sectionType === 'related'}
                        projectName={projectName}
                    />
                ))}

                {!selectedComments ||
                    (selectedComments.length === 0 && (
                        <Box>
                            <Typography fontStyle={'italic'}>
                                There are no comments on this{' '}
                                {commentSectionWithFallback.entityType
                                    ? commentEntityTypeMap[commentSectionWithFallback.entityType]
                                          ?.name
                                    : ''}
                                , Add one below.
                            </Typography>
                            <Box my={3}>
                                <Divider />
                            </Box>
                        </Box>
                    ))}
                {/* Only show comment box if displaying direct comments */}
                {commentSectionWithFallback.sectionType === 'direct' && (
                    <>
                        <Box>
                            <CommentEditor
                                label={'New comment'}
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
            </Box>
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

    // Rendering of side navigation bar
    const sidebar = (
        <Box
            position={'absolute'}
            top={0}
            left={0}
            height={'100%'}
            style={{
                borderRight: collapsed ? 'none' : '1px solid var(--color-border-color)',
            }}
        >
            <List disablePadding>
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
                        <ListItemIcon sx={{ justifyContent: 'center' }}>{commentIcon}</ListItemIcon>
                    </ListItemButton>
                </ListItem>
                {sortedCommentSections.map(({ entityType, comments, sectionType }) => {
                    const Icon = commentEntityTypeMap[entityType].Icon
                    const count = countComments(comments)
                    const tooltip = commentHeading({ entityType, sectionType })

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
                                        setCommentSection({ entityType, sectionType })
                                        if (props.collapsed) {
                                            props.onToggleCollapsed(false)
                                        }
                                    }}
                                    style={{
                                        background:
                                            entityType === commentSectionWithFallback.entityType &&
                                            sectionType ===
                                                commentSectionWithFallback.sectionType &&
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
    )

    return (
        <Box component={'section'}>
            {sidebar}
            <Box position={'absolute'} left={64} height={'100%'} right={0} overflow={'auto'}>
                {content}
            </Box>
        </Box>
    )
}
