import OpenInFullIcon from '@mui/icons-material/OpenInFull'
import {
    Alert,
    Box,
    Card,
    CardActions,
    CardContent,
    IconButton,
    Modal,
    Tooltip,
    Typography,
} from '@mui/material'
import CircularProgress from '@mui/material/CircularProgress'
import {
    DataGrid,
    GridColDef,
    GridToolbarContainer,
    GridToolbarExport,
    GridToolbarQuickFilter,
} from '@mui/x-data-grid'
import { fromArrow } from 'arquero'
import { ArrowTable } from 'arquero/dist/types/format/types'
import { ReactChild, useState } from 'react'
import { Link } from 'react-router-dom'
import { useProjectDbQuery } from '../data/projectDatabase'

type TableProps = {
    project: string
    query: string
    showToolbar?: boolean
}

type TableCardProps = TableProps & {
    height: number | string
    title?: ReactChild
    subtitle?: ReactChild
    description?: ReactChild
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

export function TableFromQueryCard(props: TableCardProps) {
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
                <Tooltip title="Expand table" arrow>
                    <IconButton onClick={() => setExpanded(true)}>
                        <OpenInFullIcon fontSize="small" />
                    </IconButton>
                </Tooltip>
            </CardActions>
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
                    <Box display={'flex'} flexDirection={'column'} height={props.height}>
                        <TableFromQuery {...props} />
                    </Box>
                </Box>
                {props.description && (
                    <Box mt={2}>
                        <Typography fontSize={12}>{props.description}</Typography>
                    </Box>
                )}
            </CardContent>

            <Modal open={expanded} onClose={() => setExpanded(false)}>
                <Box sx={modalStyle}>
                    <div
                        style={{
                            display: 'flex',
                            height: '70vh',
                            flexDirection: 'column',
                        }}
                    >
                        <TableFromQuery {...props} />
                    </div>
                </Box>
            </Modal>
        </Card>
    )
}

// Provide custom rendering for some known columns, this allows adding links to the table
const knownColumnMap: Record<string, Omit<GridColDef, 'field'>> = {
    participant_id: {
        renderCell: (params) => <Link to={`/participant/${params.value}`}>{params.value}</Link>,
    },
    sample_id: {
        renderCell: (params) => <Link to={`/sample/${params.value}`}>{params.value}</Link>,
    },
}

function CustomTableToolbar() {
    return (
        <GridToolbarContainer>
            <GridToolbarExport />
            <Box sx={{ flexGrow: 1 }} />
            <GridToolbarQuickFilter />
        </GridToolbarContainer>
    )
}

export function TableFromQuery(props: TableProps) {
    const { project, query, showToolbar } = props
    const result = useProjectDbQuery(project, query)

    const data = result && result.status === 'success' ? result.data : undefined

    if (result && result.status === 'error') {
        return <Alert severity="error">{result.errorMessage}</Alert>
    }

    if (!data || !result || result.status === 'loading') return <CircularProgress />

    const table = fromArrow(data as ArrowTable)

    const columns = table.columnNames().map(
        (colName): GridColDef => ({
            field: colName,
            ...(colName in knownColumnMap ? knownColumnMap[colName] : {}),
        })
    )

    const rows = table.objects().map((row, index) => ({ ...row, __index: index }))

    return (
        <DataGrid
            slots={{ toolbar: showToolbar ? CustomTableToolbar : undefined }}
            slotProps={{
                toolbar: {
                    showQuickFilter: true,
                },
            }}
            rows={rows}
            columns={columns}
            getRowId={(row) => row.__index}
            density="compact"
            sx={{
                fontFamily: 'monospace',
            }}
        />
    )
}
