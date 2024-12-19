import { Box, Card, CardContent, Divider, Typography } from '@mui/material'
import CircularProgress from '@mui/material/CircularProgress'
import { fromArrow } from 'arquero'
import { ArrowTable } from 'arquero/dist/types/format/types'
import { Fragment, ReactChild } from 'react'
import { useProjectDbQuery } from '../data/projectDatabase'

type Props = {
    project: string
    query: string
    title?: ReactChild
    subtitle?: ReactChild
    description?: ReactChild
}

export function MetricFromQueryCard(props: Props) {
    const { project, query } = props
    const result = useProjectDbQuery(project, query)

    const data = result && result.status === 'success' ? result.data : undefined

    if (!data || !result || result.status === 'loading') return <CircularProgress />

    const table = fromArrow(data as ArrowTable)

    const columns = table.columnNames()

    return (
        <Card sx={{ position: 'relative' }}>
            <CardContent>
                {(props.title || props.subtitle) && (
                    <Box mb={2}>
                        {props.title && (
                            <Typography fontWeight={'bold'} fontSize={16}>
                                {props.title}
                            </Typography>
                        )}
                        {props.subtitle && <Typography fontSize={14}>{props.subtitle}</Typography>}
                    </Box>
                )}
                <Box pt={props.title || props.subtitle ? 0 : 4}>
                    <Box display={'flex'} gap={2}>
                        {columns.map((col, index) => (
                            <Fragment key={col}>
                                {index !== 0 && <Divider orientation="vertical" flexItem />}
                                <Box>
                                    <Box>
                                        <Typography fontSize={12}>{col}</Typography>
                                    </Box>
                                    <Box>
                                        <Typography fontSize={40}>{table.get(col, 0)}</Typography>
                                    </Box>
                                </Box>
                            </Fragment>
                        ))}
                    </Box>
                </Box>
                {props.description && (
                    <Box mt={2}>
                        <Typography fontSize={12}>{props.description}</Typography>
                    </Box>
                )}
            </CardContent>
        </Card>
    )
}
