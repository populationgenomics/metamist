import { Box, List, ListItem, ListItemButton, ListItemText, Typography } from '@mui/material'
import { DateTime } from 'luxon'
import { useState } from 'react'
import { CommentData } from './commentConfig'
import { CommentContent } from './CommentContent'
import { parseAuthor } from './commentUtils'

export function CommentHistory(props: { comment: CommentData; theme: 'light' | 'dark' }) {
    const comment = props.comment
    // Add the current version to past versions
    const versions = [
        ...comment.versions.map((vv) => ({
            content: vv.content,
            author: vv.author,
            status: vv.status,
            timestamp: vv.timestamp,
        })),
        {
            content: comment.content,
            author: comment.author,
            status: comment.status,
            timestamp: comment.updatedAt,
        },
    ].reverse()

    const [selectedTimestamp, setSelectedTimestamp] = useState(comment.updatedAt)
    const selectedVersionIndex = versions.findIndex((vv) => vv.timestamp === selectedTimestamp)
    const selectedVersion = versions[selectedVersionIndex]
    const previousVersion = versions[selectedVersionIndex + 1]

    return (
        <Box
            display={'flex'}
            sx={{
                position: 'absolute',
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
                width: '80%',
                minWidth: 400,
                maxWidth: 1280,
                maxHeight: '90%',
                bgcolor: 'background.paper',
                border: '2px solid var(--color-border-color)',
                p: 4,
            }}
        >
            <Box mr={2} flexGrow={1} overflow={'auto'}>
                {previousVersion &&
                    selectedVersion &&
                    selectedVersion.status !== previousVersion.status && (
                        <Box mb={2}>
                            <Typography fontStyle={'italic'}>
                                Status changed from {previousVersion.status} to{' '}
                                {selectedVersion.status}
                            </Typography>
                        </Box>
                    )}
                <CommentContent content={selectedVersion?.content} theme={props.theme} />
            </Box>
            <Box
                borderLeft={'1px solid var(--color-border-color)'}
                minWidth={250}
                sx={{ overflowY: 'auto' }}
            >
                <List>
                    {versions.map((vv) => {
                        const time = DateTime.fromISO(vv.timestamp)

                        return (
                            <ListItem
                                key={vv.timestamp}
                                disablePadding
                                style={{
                                    background:
                                        vv.timestamp === selectedTimestamp
                                            ? 'var(--color-border-color)'
                                            : 'inherit',
                                }}
                            >
                                <ListItemButton onClick={() => setSelectedTimestamp(vv.timestamp)}>
                                    <ListItemText
                                        primary={time.toLocaleString(DateTime.DATETIME_MED)}
                                        secondary={parseAuthor(vv.author).name}
                                    />
                                </ListItemButton>
                            </ListItem>
                        )
                    })}
                </List>
            </Box>
        </Box>
    )
}
