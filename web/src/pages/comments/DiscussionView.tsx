import { Box, ListItem, ListItemButton, Typography } from '@mui/material'
import { useState } from 'react'
import { DiscussionFragmentFragment } from '../../__generated__/graphql'
import { CommentThread } from './CommentThread'
import { CommentEntityType, commentEntityTypeMap, CommentThreadData } from './commentConfig'

function DiscussionSection(props: {
    entityType: CommentEntityType
    comments: CommentThreadData[]
}) {
    const [expanded, setExpanded] = useState(false)
    const { entityType, comments } = props

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
                                prevComment={prevComment}
                                showEntityInfo={true}
                            />
                        </Box>
                    )
                })}
        </Box>
    )
}

export function DiscussionView(props: { discussion: DiscussionFragmentFragment }) {
    const { discussion } = props

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
        <Box>
            <Box>
                {discussion.directComments.map((cc) => (
                    <CommentThread comment={cc} showEntityInfo={false} />
                ))}
            </Box>
            <Box>
                {groupedRelatedComments.map(([entityType, comments]) => (
                    <DiscussionSection entityType={entityType} comments={comments} />
                ))}
            </Box>
        </Box>
    )
}
