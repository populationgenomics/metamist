// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'
import {
    SemanticCOLORS,
    Container,
    Card,
    CardContent,
    CardHeader,
    Table,
    TableHeader,
    TableHeaderCell,
    TableCell,
    TableRow,
    TableBody,
} from 'semantic-ui-react'

interface TableTileProps {
    color: SemanticCOLORS
    header: string
    // data: object
}

const Tile: React.FC<TableTileProps> = ({ color, header }) => {
    return (
        <>
            <Card
                className="ourdna-tile"
                color={color}
                style={{ width: '100%', height: '180px', margin: '2em' }}
            >
                <CardContent>
                    <CardHeader className="ourdna-tile-header">{header}</CardHeader>
                    <Container
                        className="ourdna-table-tile"
                        style={{
                            display: 'flex',
                            flexDirection: 'row',
                            height: '400px',
                            overflowY: 'auto',
                        }}
                    >
                        {/* Change this to a table element */}
                        <Table basic="very">
                            <TableHeader>
                                <TableRow>
                                    <TableHeaderCell>Name</TableHeaderCell>
                                    <TableHeaderCell>Status</TableHeaderCell>
                                    <TableHeaderCell>Notes</TableHeaderCell>
                                </TableRow>
                            </TableHeader>

                            <TableBody>
                                <TableRow>
                                    <TableCell>John</TableCell>
                                    <TableCell>Approved</TableCell>
                                    <TableCell>None</TableCell>
                                </TableRow>
                                <TableRow>
                                    <TableCell>Jamie</TableCell>
                                    <TableCell>Approved</TableCell>
                                    <TableCell>Requires call</TableCell>
                                </TableRow>
                                <TableRow>
                                    <TableCell>Jill</TableCell>
                                    <TableCell>Denied</TableCell>
                                    <TableCell>None</TableCell>
                                </TableRow>
                                <TableRow>
                                    <TableCell>John</TableCell>
                                    <TableCell>Approved</TableCell>
                                    <TableCell>None</TableCell>
                                </TableRow>
                                <TableRow>
                                    <TableCell>Jamie</TableCell>
                                    <TableCell>Approved</TableCell>
                                    <TableCell>Requires call</TableCell>
                                </TableRow>
                                <TableRow>
                                    <TableCell>Jill</TableCell>
                                    <TableCell>Denied</TableCell>
                                    <TableCell>None</TableCell>
                                </TableRow>
                            </TableBody>
                        </Table>
                    </Container>
                </CardContent>
            </Card>
        </>
    )
}

export default Tile
