import React from 'react'
import { CardGroup, Container, Image } from 'semantic-ui-react'

import './OurDnaDashboard.css'

import Tile from '../../shared/components/ourdna/Tile'
import TableTile from '../../shared/components/ourdna/TableTile'

import OurDonutChart from '../../shared/components/ourdna/OurDonutChart'
import BarChart from '../../shared/components/ourdna/BarChart'

const Dashboard = () => (
    <div>
        <Container
            fluid
            style={{
                display: 'flex',
                flexDirection: 'column',
                backgroundColor: '#FFFFFF',
                borderRadius: '10px',
                paddingTop: '1em',
                paddingBottom: '1em',
            }}
        >
            <Container
                fluid
                fixed="top"
                style={{
                    display: 'flex',
                    flexDirection: 'row',
                    backgroundColor: '#FFFFFF',
                    paddingTop: '2em',
                    paddingBottom: '2em',
                    border: '1px solid',
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

            <div
                className="ourdna-dashboard-tiles"
                style={{
                    marginTop: '4em',
                    padding: '2em',
                    display: 'flex',
                    flexDirection: 'row',
                    border: '1px solid',
                    alignContent: 'center',
                }}
            >
                <Container fluid style={{ width: '60%' }}>
                    <CardGroup itemsPerRow={2}>
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
                <Container style={{ width: '40%' }}>
                    <OurDonutChart header="Sample Status" />
                </Container>
            </div>
            <div
                className="ourdna-dashboard-tiles"
                style={{
                    marginTop: '4em',
                    padding: '2em',
                    display: 'flex',
                    flexDirection: 'row',
                    border: '1px solid',
                    alignContent: 'center',
                }}
            >
                <Container style={{ width: '60%' }}>
                    <BarChart header="Samples by Site" />
                </Container>
                <Container style={{ width: '40%' }}>
                    <CardGroup itemsPerRow={1} style={{ width: '100%' }}>
                        <TableTile color="red" header="Westmead" />
                        {/* Add more aggregated tile here */}
                        <TableTile color="blue" header="RPA" />
                    </CardGroup>
                </Container>
            </div>
        </Container>
    </div>
)

export default Dashboard
