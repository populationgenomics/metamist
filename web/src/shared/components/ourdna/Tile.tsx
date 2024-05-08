// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'
import {
    SemanticCOLORS,
    Container,
    Card,
    CardContent,
    CardHeader,
    CardDescription,
} from 'semantic-ui-react'

interface TileProps {
    color: SemanticCOLORS
    header: string
    stat: string
    units: string
    description: string
}

const Tile: React.FC<TileProps> = ({ color, header, stat, units, description }) => {
    return (
        <>
            <Card className="ourdna-tile" color={color}>
                <CardContent>
                    <CardHeader className="ourdna-tile-header">{header}</CardHeader>
                    <Container
                        className="ourdna-tile-stat"
                        style={{ display: 'flex', flexDirection: 'row' }}
                    >
                        <div style={{ fontSize: '2em' }}>{stat}</div>
                        <div style={{ marginLeft: '1em' }}>{units}</div>
                    </Container>
                    <CardDescription className="ourdna-tile-description">
                        {description}
                    </CardDescription>
                </CardContent>
            </Card>
        </>
    )
}

export default Tile
