import { Box } from '@mui/material'

export function PaddedPage(props: { children: React.ReactNode }) {
    return (
        <Box p={10} pt={5}>
            {props.children}
        </Box>
    )
}
