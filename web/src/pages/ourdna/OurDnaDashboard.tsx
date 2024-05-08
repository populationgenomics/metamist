import React from 'react'
import { CardGroup, Container, Header, Image } from 'semantic-ui-react'

import Tile from '../../shared/components/ourdna/Tile'

const Dashboard = () => (
    <div>
        <Container fluid style={{ display: 'flex', flexDirection: 'column' }}>
            <Container
                fixed="top"
                style={{
                    display: 'flex',
                    flexDirection: 'row',
                    backgroundColor: '#FFFFFF',
                    paddingTop: '1em',
                    paddingBottom: '1em',
                }}
            >
                <Container>
                    {/* add the logo and search bar here */}
                    <Image src="/logo.png" size="small" />
                </Container>
                <Container>
                    <div>Search Bar</div>
                </Container>
            </Container>

            <Container text style={{ marginTop: '7em' }}>
                <CardGroup centered itemsPerRow={2}>
                    <Tile
                        color="green"
                        header="Awaiting Consent"
                        stat="100"
                        units="participants"
                        description="The number of people who have signed up but not consented."
                    />
                    <Tile
                        color="yellow"
                        header="Awaiting Collection"
                        stat="90"
                        units="participants"
                        description="The number of people who have consented but not given blood."
                    />
                    <Tile
                        color="red"
                        header="Samples Lost"
                        stat="43"
                        units="samples"
                        description="Blood has been collected, but was not processed within 72 hours. "
                    />
                    {/* Add more aggregated tile here */}
                    <Tile
                        color="blue"
                        header="Collection to Processing"
                        stat="90"
                        units="participants"
                        description="The number of people who have consented but not given blood."
                    />
                </CardGroup>
            </Container>
        </Container>
    </div>
)

export default Dashboard
