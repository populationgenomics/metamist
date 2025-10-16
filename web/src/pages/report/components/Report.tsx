import { Box } from '@mui/material'

export default function Report(props: { children: React.ReactNode }) {
    return (
        <Box display={'flex'} flexDirection={'column'} gap={2}>
            {props.children}
        </Box>
    )
}
