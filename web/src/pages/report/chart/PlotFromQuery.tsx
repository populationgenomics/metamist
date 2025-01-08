import CodeIcon from '@mui/icons-material/Code'
import OpenInFullIcon from '@mui/icons-material/OpenInFull'
import TableChartIcon from '@mui/icons-material/TableChart'
import {
    Alert,
    Box,
    Card,
    CardActions,
    CardContent,
    CircularProgress,
    IconButton,
    Modal,
    Tooltip,
    Typography,
} from '@mui/material'
import * as Plot from '@observablehq/plot'
import { Table, TypeMap } from 'apache-arrow'
import { ReactChild, useEffect, useRef, useState } from 'react'
import { useMeasure } from 'react-use'
import { useProjectDbQuery } from '../data/projectDatabase'
import { TableFromQuery } from './TableFromQuery'

type PlotOptions = {
    width: number
}

type PlotInputFunc = (
    data: Table<TypeMap>,
    options: PlotOptions
) => (HTMLElement | SVGSVGElement) & Plot.Plot

type Props = {
    project: string
    query: string
    title?: ReactChild
    subtitle?: ReactChild
    description?: ReactChild
    plot: PlotInputFunc
}

const modalStyle = {
    position: 'absolute',
    top: '50%',
    left: '50%',
    transform: 'translate(-50%, -50%)',
    maxHeight: '100vh',
    overflow: 'hidden',
    width: 'calc(100% - 50px)',
    bgcolor: 'background.paper',
    boxShadow: 24,
    p: 4,
}

export function PlotFromQueryCard(props: Props) {
    const [showingTable, setShowingTable] = useState(false)
    const [expanded, setExpanded] = useState(false)

    return (
        <Card sx={{ position: 'relative' }}>
            <CardActions
                disableSpacing
                sx={{
                    position: 'absolute',
                    right: 0,
                }}
            >
                <Tooltip title="View/Edit SQL" arrow>
                    <IconButton
                        target={'_blank'}
                        href={`/project/${props.project}/query?query=${encodeURIComponent(props.query)}`}
                    >
                        <CodeIcon fontSize="small" />
                    </IconButton>
                </Tooltip>
                <Tooltip title="View data as table" arrow>
                    <IconButton onClick={() => setShowingTable(true)}>
                        <TableChartIcon fontSize="small" />
                    </IconButton>
                </Tooltip>
                <Tooltip title="Expand chart" arrow>
                    <IconButton onClick={() => setExpanded(true)}>
                        <OpenInFullIcon fontSize="small" />
                    </IconButton>
                </Tooltip>
            </CardActions>
            <CardContent>
                <PlotFromQuery {...props} />
            </CardContent>

            <Modal open={expanded} onClose={() => setExpanded(false)}>
                <Box sx={modalStyle}>
                    <PlotFromQuery {...props} />
                </Box>
            </Modal>

            <Modal open={showingTable} onClose={() => setShowingTable(false)}>
                <Box sx={modalStyle}>
                    <div
                        style={{
                            display: 'flex',
                            maxHeight: '70vh',
                            flexDirection: 'column',
                        }}
                    >
                        <TableFromQuery query={props.query} project={props.project} />
                    </div>
                </Box>
            </Modal>
        </Card>
    )
}

export function PlotFromQuery(props: Props) {
    const containerRef = useRef<HTMLDivElement>(null)

    const { project, query, plot } = props
    const result = useProjectDbQuery(project, query)

    const [measureRef, { width }] = useMeasure<HTMLDivElement>()

    const data = result && result.status === 'success' ? result.data : undefined

    useEffect(() => {
        if (!data) return
        const _plot = plot(data, { width })
        containerRef.current?.append(_plot)
        return () => _plot.remove()
    }, [data, width, plot])

    return (
        <Box>
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

            <div ref={measureRef} style={{ width: '100%', height: 0 }} />
            {!result || (result.status === 'loading' && <CircularProgress />)}
            <div ref={containerRef} />

            {result && result.status === 'error' && (
                <Alert severity="error">{result.errorMessage}</Alert>
            )}
            {props.description && (
                <Box mt={2}>
                    <Typography fontSize={12}>{props.description}</Typography>
                </Box>
            )}
        </Box>
    )
}
