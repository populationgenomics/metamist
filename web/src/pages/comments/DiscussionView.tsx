import { ApolloError } from '@apollo/client'
import { Alert, Box, Button, ListItem, ListItemButton, Typography } from '@mui/material'
import { useContext, useState } from 'react'
import { DiscussionFragmentFragment, ProjectMemberRole } from '../../__generated__/graphql'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { ViewerContext } from '../../viewer'
import { CommentEditor, CommentThread } from './CommentThread'
import { CommentEntityType, commentEntityTypeMap, CommentThreadData } from './commentConfig'

function DiscussionSection(props: {
    entityType: CommentEntityType
    canComment: boolean
    viewerUser: string | null
    comments: CommentThreadData[]
    projectName: string
}) {
    const [expanded, setExpanded] = useState(false)

    const { entityType, comments, canComment, viewerUser, projectName } = props

    return (
        <Box>
            <ListItem disablePadding sx={{ borderTop: '1px solid var(--color-border-color)' }}>
                {/* @TODO add a line showing when the most recent comment was updated */}
                <ListItemButton onClick={() => setExpanded(!expanded)} sx={{ p: 2 }}>
                    <Typography fontWeight={'bold'}>
                        {comments.length} {commentEntityTypeMap[entityType]} comment
                        {comments.length > 1 ? 's' : ''}
                    </Typography>
                </ListItemButton>
            </ListItem>
            {expanded &&
                comments.map((cc, index) => {
                    const prevComment = comments[index - 1]
                    return (
                        <Box>
                            <CommentThread
                                comment={cc}
                                viewerUser={viewerUser}
                                canComment={canComment}
                                prevComment={prevComment}
                                showEntityInfo={true}
                                projectName={projectName}
                            />
                        </Box>
                    )
                })}
        </Box>
    )
}

export function DiscussionView(props: {
    discussionLoading: boolean
    discussionError: ApolloError | undefined
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
        addingCommentLoading,
        addingCommentError,
        onReload,
        onAddComment,
        projectName,
    } = props
    const [commentContent, setCommentContent] = useState('')
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

    if (discussionLoading) {
        return <LoadingDucks />
    }

    if (discussionError) {
        return (
            <Box my={1} px={2}>
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
    }

    if (!discussion) return null

    const canComment =
        viewer?.checkProjectAccessByName(projectName, [
            ProjectMemberRole.Writer,
            ProjectMemberRole.Contributor,
        ]) ?? false

    const groupedRelatedCommentsMap = discussion.relatedComments.reduce(
        (gg: Map<string, CommentThreadData[]>, cc) => {
            const entityType = cc.entity.__typename
            return gg.set(entityType, (gg.get(entityType) || []).concat(cc))
        },
        new Map()
    )

    const groupedRelatedComments: [CommentEntityType, CommentThreadData[]][] = []

    for (const [_entityType, comments] of groupedRelatedCommentsMap) {
        let entityType = _entityType as CommentEntityType
        groupedRelatedComments.push([entityType, comments])
    }

    return (
        <Box height={'100%'} sx={{ overflowY: 'auto' }}>
            <Box>
                {discussion.directComments.map((cc) => (
                    <CommentThread
                        key={cc.id}
                        comment={cc}
                        canComment={canComment}
                        viewerUser={viewer?.username ?? null}
                        showEntityInfo={false}
                        projectName={projectName}
                    />
                ))}
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
            </Box>
            {/* <Box>
                {groupedRelatedComments.map(([entityType, comments]) => (
                    <DiscussionSection
                        key={entityType}
                        entityType={entityType}
                        canComment={canComment}
                        viewerUser={viewer?.username ?? null}
                        comments={comments}
                        projectName={projectName}
                    />
                ))}
            </Box> */}
        </Box>
    )
}
