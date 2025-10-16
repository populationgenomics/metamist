import { Alert, Box, Divider, Typography } from '@mui/material'
import { fromArrow } from 'arquero'
import { ArrowTable } from 'arquero/dist/types/format/types'
import { Fragment } from 'react'
import { formatQuery, UnformattedQuery } from '../data/formatQuery'
import { useProjectDbQuery } from '../data/projectDatabase'
import ReportItemLoader from './ReportItemLoader'

export type MetricProps = {
    project: string
    query: UnformattedQuery
}

export function MetricFromQuery(props: MetricProps) {
    const { project } = props
    const query = formatQuery(props.query)
    const result = useProjectDbQuery(project, query)

    const data = result && result.status === 'success' ? result.data : undefined

    if (!result || result.status === 'loading') return <ReportItemLoader />

    if (result && result.status === 'error') {
        return (
            <Box>
                <Alert severity="error">{result.errorMessage}</Alert>
            </Box>
        )
    }

    const table = fromArrow(data as ArrowTable)

    const columns = table.columnNames()

    return (
        <Box>
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
    )
}
