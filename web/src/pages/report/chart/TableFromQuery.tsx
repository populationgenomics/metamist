import { Alert, Box } from '@mui/material'
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
import { Link } from 'react-router-dom'
import { useProjectDbQuery } from '../data/projectDatabase'

type TableProps = {
    project: string
    query: string
    showToolbar?: boolean
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
