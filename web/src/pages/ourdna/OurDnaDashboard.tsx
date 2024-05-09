import React from 'react'
import { CardGroup, Container } from 'semantic-ui-react'

import {
    Box,
    Flex,
    Image,
    Avatar,
    Text,
    Button,
    Menu,
    MenuButton,
    MenuList,
    MenuItem,
    MenuDivider,
    useDisclosure,
    useColorModeValue,
    Stack,
    useColorMode,
    Center,
} from '@chakra-ui/react'

import { gql, useQuery } from '@apollo/client'
// import { gql } from '../../__generated__/gql'

import './OurDnaDashboard.css'

import Tile from '../../shared/components/ourdna/Tile'
import TableTile from '../../shared/components/ourdna/TableTile'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import MuckError from '../../shared/components/MuckError'
import OurDonutChart from '../../shared/components/ourdna/OurDonutChart'
import BarChart from '../../shared/components/ourdna/BarChart'

const GET_OURDNA_DASHBOARD = gql(`
  query DashboardQuery($project: String!) {
    project(name: $project) {
      ourdnaDashboard
    }
  }`)

const Dashboard = () => {
    const { loading, error, data } = useQuery(GET_OURDNA_DASHBOARD, {
        variables: { project: 'greek-myth' },
    })

    const samplesLostAfterCollections = {}
    data &&
        Object.keys(data.project.ourdnaDashboard[0].samples_lost_after_collection).forEach(
            (key) => {
                samplesLostAfterCollections[key] =
                    data.project.ourdnaDashboard[0].samples_lost_after_collection[key]
                        .time_to_process_start / 3600
            },
            []
        )

    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>

    return data ? (
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
                <Box px={4}>
                    <Flex h={16} alignItems={'center'} justifyContent={'space-between'}>
                        <Box>
                            <Image htmlWidth="150px" src="/logo.png" />
                        </Box>
                    </Flex>
                </Box>

                <div
                    className="ourdna-dashboard-tiles"
                    style={{
                        marginTop: '4em',
                        padding: '2em',
                        display: 'flex',
                        flexDirection: 'row',
                        alignContent: 'center',
                    }}
                >
                    <Container fluid style={{ width: '60%' }}>
                        <CardGroup itemsPerRow={2}>
                            <Tile
                                header="Awaiting Consent"
                                stat={`${data.project.ourdnaDashboard[0].participants_signed_not_consented.length}`}
                                units="participants"
                                description="The number of people who have signed up but not consented."
                            />
                            <Tile
                                header="Awaiting Collection"
                                stat={`${data.project.ourdnaDashboard[0].participants_consented_not_collected.length}`}
                                units="participants"
                                description="The number of people who have consented but not given blood."
                            />
                            <Tile
                                header="Samples Lost"
                                stat={`${
                                    Object.keys(
                                        data.project.ourdnaDashboard[0]
                                            .samples_lost_after_collection
                                    ).length
                                }`}
                                units="samples"
                                description="Blood has been collected, but was not processed within 72 hours. "
                            />
                            {/* Add more aggregated tile here */}
                            <Tile
                                header="Collection to Processing"
                                stat="90"
                                units="participants"
                                description="The number of people who have consented but not given blood."
                            />
                        </CardGroup>
                    </Container>
                    <Container style={{ width: '40%' }}>
                        <OurDonutChart
                            header="Sample Status"
                            data={
                                data.project.ourdnaDashboard[0]
                                    .total_samples_by_collection_event_name
                            }
                        />
                    </Container>
                </div>
                <div
                    className="ourdna-dashboard-tiles"
                    style={{
                        marginTop: '4em',
                        padding: '2em',
                        display: 'flex',
                        flexDirection: 'row',
                        alignContent: 'center',
                    }}
                >
                    <Container style={{ width: '60%' }}>
                        <BarChart
                            header="Samples by Site"
                            data={data.project.ourdnaDashboard[0].processing_times_by_site}
                        />
                    </Container>
                    <Container style={{ width: '40%' }}>
                        <CardGroup itemsPerRow={1} style={{ width: '100%' }}>
                            <TableTile
                                header="Viable Long Read"
                                data={data.project.ourdnaDashboard[0].samples_concentration_gt_1ug}
                                columns={['Sample ID', 'Concentration']}
                            />
                            {/* Add more aggregated tile here */}
                            <TableTile
                                header="Processed > 24h"
                                data={samplesLostAfterCollections}
                                columns={['Sample ID', 'Time (h)']}
                            />
                        </CardGroup>
                    </Container>
                </div>
            </Container>
        </div>
    ) : (
        <MuckError message={`Ah Muck, there's no data here`} />
    )
}

export default Dashboard
