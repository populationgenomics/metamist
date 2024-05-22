import * as React from 'react'
import { Card, Label, Statistic } from 'semantic-ui-react'

interface TileProps {
    header: string
    stat: string
    units: string
    unitsColour: string
    description: string
    icon: React.ReactNode
}

const Tile: React.FC<TileProps> = ({ header, stat, units, unitsColour, description, icon }) => (
    <Card
        fluid
        style={{
            backgroundColor: 'var(--color-bg-card)',
            boxShadow: 'rgba(0, 0, 0, 0.24) 0px 3px 8px',
        }}
    >
        <Card.Content>
            <Card.Header className="dashboard-tile" style={{ fontSize: '1.25rem' }}>
                {icon}
                {header}
            </Card.Header>
            <Card.Description>
                <Statistic size="small">
                    <Statistic.Value className="dashboard-tile">{stat}</Statistic.Value>
                    <Statistic.Label style={{ margin: 5 }}>
                        <Label
                            className="dashboard-tile"
                            style={{ backgroundColor: `var(--${unitsColour})` }}
                        >
                            {units}
                        </Label>
                    </Statistic.Label>
                </Statistic>
            </Card.Description>
        </Card.Content>
        <Card.Content className="dashboard-tile" extra>
            {description}
        </Card.Content>
    </Card>
)

export default Tile
