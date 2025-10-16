import { Box } from '@mui/material'

export default function ReportRow(props: { children: React.ReactNode }) {
    return (
        <Box display={'flex'} gap={2} flexWrap={'wrap'}>
            {props.children}
        </Box>
    )
}
