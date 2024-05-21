import * as React from 'react'
import { Card, Image, Label, Statistic, Grid } from 'semantic-ui-react'

interface StatTileProps {
    header: string
    stats: { value: string; units: string; unitsColour: string }[]
    tile_icon: string
    description: string
}

const StatTile: React.FC<StatTileProps> = ({ header, stats, tile_icon, description }) => {
    return (
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
                    <Grid columns={stats.length}>
                        {stats.map((stat, index) => (
                            <Grid.Column key={index}>
                                <Statistic size="small">
                                    <Statistic.Value>{stat.value}</Statistic.Value>
                                    <Statistic.Label style={{ margin: 5 }}>
                                        <Label
                                            color="white"
                                            style={{ backgroundColor: stat.unitsColour }}
                                        >
                                            {stat.units}
                                        </Label>
                                    </Statistic.Label>
                                </Statistic>
                            </Grid.Column>
                        ))}
                    </Grid>
                </Card.Description>
            </Card.Content>
            <Card.Content extra>{description}</Card.Content>
        </Card>
    )
}

export default StatTile
