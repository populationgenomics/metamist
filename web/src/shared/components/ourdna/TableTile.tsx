import * as React from 'react'
import { Card, Image, Table, Icon } from 'semantic-ui-react'

interface TableTileProps {
    header: string
    data: Record<string, string | number>
    columns: Array<string>
    tile_icon: string
}

const TableTile: React.FC<TableTileProps> = ({ header, data, columns, tile_icon }) => (
    <Card
        fluid
        style={{
            backgroundColor: 'var(--color-bg-card)',
            boxShadow: 'rgba(0, 0, 0, 0.24) 0px 3px 8px',
        }}
    >
        <Card.Content>
            <Card.Header className="dashboard-tile">
                <Image src={tile_icon} alt="Icon" size="mini" spaced="right" />
                {header}
            </Card.Header>
            <Card.Description>
                <Table celled style={{ backgroundColor: 'var(--color-bg-card)' }}>
                    <Table.Header>
                        <Table.Row>
                            {columns &&
                                columns.map((column, index) => (
                                    <Table.HeaderCell
                                        className="dashboard-tile"
                                        key={index}
                                        style={{ backgroundColor: 'var(--color-bg-card)' }}
                                    >
                                        {column}
                                    </Table.HeaderCell>
                                ))}
                        </Table.Row>
                    </Table.Header>
                    <Table.Body className="dashboard-tile">
                        {data &&
                            Object.keys(data).map((key, index) => (
                                <Table.Row key={index}>
                                    <Table.Cell>{key}</Table.Cell>
                                    <Table.Cell>{data[key]}</Table.Cell>
                                </Table.Row>
                            ))}
                    </Table.Body>
                </Table>
            </Card.Description>
        </Card.Content>
    </Card>
)

export default TableTile
