import * as React from 'react'
import { Card, Image, Label, Statistic } from 'semantic-ui-react'

interface TileProps {
    header: string
    stat: string
    units: string
    units_colour: string
    description: string
    tile_icon: string
}

const Tile: React.FC<TileProps> = ({
    header,
    stat,
    units,
    units_colour,
    description,
    tile_icon,
}) => (
    <Card
        fluid
        style={{
            backgroundColor: 'var(--color-bg-card)',
            boxShadow: 'rgba(0, 0, 0, 0.24) 0px 3px 8px',
        }}
    >
        <Card.Content>
            <Card.Header style={{ fontSize: '1.25rem' }}>
                <Image src={tile_icon} alt="Icon" size="mini" spaced="right" />
                {header}
            </Card.Header>
            <Card.Description>
                <Statistic size="small">
                    <Statistic.Value>{stat}</Statistic.Value>
                    <Statistic.Label style={{ margin: 5 }}>
                        <Label style={{ backgroundColor: `var(--${units_colour})` }}>{units}</Label>
                    </Statistic.Label>
                </Statistic>
            </Card.Description>
        </Card.Content>
        <Card.Content extra>{description}</Card.Content>
    </Card>
)

export default Tile
