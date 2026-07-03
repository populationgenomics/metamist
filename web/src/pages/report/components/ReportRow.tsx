import { Box } from '@mui/material'

export default function ReportRow(props: { children: React.ReactNode; gap?: number }) {
    return (
        <Box display={'flex'} gap={props.gap ?? 2} flexWrap={'wrap'}>
            {props.children}
        </Box>
    )
}
