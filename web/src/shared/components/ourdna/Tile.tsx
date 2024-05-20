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
}) => {
    return (
        <Card fluid style={{ backgroundColor: 'white' }}>
            <Card.Content>
                <Card.Header>
                    <Image src={tile_icon} alt="Icon" size="mini" spaced="right" />
                    {header}
                </Card.Header>
                <Card.Description>
                    <Statistic>
                        <Statistic.Value>{stat}</Statistic.Value>
                        <Statistic.Label>
                            <Label color="white" style={{ backgroundColor: units_colour }}>
                                {units}
                            </Label>
                        </Statistic.Label>
                    </Statistic>
                </Card.Description>
            </Card.Content>
            <Card.Content extra>{description}</Card.Content>
        </Card>
    )
}

export default Tile
