import CodeIcon from '@mui/icons-material/Code'
import OpenInFullIcon from '@mui/icons-material/OpenInFull'
import TableChartIcon from '@mui/icons-material/TableChart'

import { Box, IconButton, Modal, Tooltip } from '@mui/material'
import { useState } from 'react'
import { PlotFromQuery, PlotProps } from './PlotFromQuery'
import { TableFromQuery } from './TableFromQuery'

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

export function ActionViewExpandedTable({ query, project }: { project: string; query: string }) {
    const [showingTable, setShowingTable] = useState(false)

    return (
        <>
            <Tooltip title="View expanded table" arrow>
                <IconButton onClick={() => setShowingTable(true)}>
                    <TableChartIcon fontSize="small" />
                </IconButton>
            </Tooltip>
            <Modal open={showingTable} onClose={() => setShowingTable(false)}>
                <Box sx={modalStyle}>
                    <div
                        style={{
                            display: 'flex',
                            maxHeight: '70vh',
                            flexDirection: 'column',
                        }}
                    >
                        <TableFromQuery query={query} project={project} showToolbar />
                    </div>
                </Box>
            </Modal>
        </>
    )
}

export function ActionViewEditSql({ query, project }: { project: string; query: string }) {
    return (
        <Tooltip title="View/Edit SQL" arrow>
            <IconButton
                target={'_blank'}
                href={`/project/${project}/query?query=${encodeURIComponent(query)}`}
            >
                <CodeIcon fontSize="small" />
            </IconButton>
        </Tooltip>
    )
}

export function ActionViewExpandedPlot(props: PlotProps) {
    const [expanded, setExpanded] = useState(false)

    return (
        <>
            <Tooltip title="Expand chart" arrow>
                <IconButton onClick={() => setExpanded(true)}>
                    <OpenInFullIcon fontSize="small" />
                </IconButton>
            </Tooltip>
            <Modal open={expanded} onClose={() => setExpanded(false)}>
                <Box sx={{ ...modalStyle, height: '80vh' }}>
                    <PlotFromQuery {...props} />
                </Box>
            </Modal>
        </>
    )
}
