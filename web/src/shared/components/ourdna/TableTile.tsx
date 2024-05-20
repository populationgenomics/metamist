import * as React from 'react'
import { Card, Image, Table, Icon } from 'semantic-ui-react'

interface TableTileProps {
    header: string
    data: Record<string, string | number>
    columns: Array<string>
    tile_icon: string
}

const TableTile: React.FC<TableTileProps> = ({ header, data, columns, tile_icon }) => {
    return (
        <Card fluid style={{ backgroundColor: 'white' }}>
            <Card.Content>
                <Card.Header>
                    <Image src={tile_icon} alt="Icon" size="mini" spaced="right" />
                    {header}
                </Card.Header>
                <Card.Description>
                    <Table celled>
                        <Table.Header>
                            <Table.Row>
                                {columns &&
                                    columns.map((column, index) => (
                                        <Table.HeaderCell key={index}>{column}</Table.HeaderCell>
                                    ))}
                            </Table.Row>
                        </Table.Header>
                        <Table.Body>
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
}

export default TableTile
