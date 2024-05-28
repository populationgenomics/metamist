import * as React from 'react'
import { Card, Table } from 'semantic-ui-react'

interface TableTileProps {
    header: string
    data: Record<string, string | number>
    columns: Array<string>
    icon: React.ReactNode
}

const TableTile: React.FC<TableTileProps> = ({ header, data, columns, icon }) => (
    <Card
        fluid
        style={{
            backgroundColor: 'var(--color-bg-card)',
            boxShadow: 'var(--color-bg-card-shadow)',
        }}
    >
        <Card.Content>
            <Card.Header className="dashboard-tile">
                {icon}
                {header}
            </Card.Header>
            <Card.Description>
                <div style={{ maxHeight: '20vh', overflowY: 'auto' }}>
                    <Table celled style={{ backgroundColor: 'var(--color-bg-card)' }}>
                        <Table.Header
                            style={{
                                position: 'sticky',
                                top: 0,
                                zIndex: 1,
                                backgroundColor: 'var(--color-bg-card)',
                            }}
                        >
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
                </div>
            </Card.Description>
        </Card.Content>
    </Card>
)

export default TableTile
