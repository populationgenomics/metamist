import { Box, CircularProgress } from '@mui/material'

export default function ReportItemLoader() {
    return (
        <Box
            sx={{
                position: 'absolute',
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
            }}
        >
            <CircularProgress />
        </Box>
    )
}
