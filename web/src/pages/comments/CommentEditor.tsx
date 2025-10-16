import { Box, TextField } from '@mui/material'
import { forwardRef } from 'react'

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
