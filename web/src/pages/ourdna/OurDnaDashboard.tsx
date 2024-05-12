import React from 'react'
import { CardGroup, Container } from 'semantic-ui-react'

import { Box, Flex, Image, Grid, GridItem } from '@chakra-ui/react'

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
        <>
            <Box marginTop={0} padding={5}>
                <Image htmlWidth="150px" src="/logo_option2.png" />
            </Box>
            <Grid h="100vh" templateRows="repeat(5, 1fr)" templateColumns="repeat(5, 1fr)" gap={4}>
                <GridItem rowSpan={2} colSpan={3} border={'1px solid'}>
                    <Grid
                        templateRows="repeat(2, 1fr)"
                        templateColumns="repeat(2, 1fr)"
                        gap={5}
                        paddingX={6}
                        paddingY={4}
                    >
                        <GridItem rowSpan={1} colSpan={1}>
                            <Tile
                                header="Awaiting Consent"
                                stat={`${data.project.ourdnaDashboard[0].participants_signed_not_consented.length}`}
                                units="participants"
                                units_colour="#71ace1"
                                description="The number of people who have signed up but not consented."
                                tile_icon="/dashboard_icons/blue_clipboard.svg"
                            />
                        </GridItem>
                        <GridItem rowSpan={1} colSpan={1}>
                            <Tile
                                header="Awaiting Collection"
                                stat={`${data.project.ourdnaDashboard[0].participants_consented_not_collected.length}`}
                                units="participants"
                                units_colour="#e8c71d"
                                description="The number of people who have consented but not given blood."
                                tile_icon="/dashboard_icons/yellow_syringe.svg"
                            />
                        </GridItem>
                        <GridItem rowSpan={1} colSpan={1}>
                            <Tile
                                header="Samples Lost"
                                stat={`${
                                    Object.keys(
                                        data.project.ourdnaDashboard[0]
                                            .samples_lost_after_collection
                                    ).length
                                }`}
                                units="samples"
                                units_colour="#bf003c"
                                description="Blood has been collected, but was not processed within 72 hours. "
                                tile_icon="/dashboard_icons/red_bloodsample.svg"
                            />
                        </GridItem>
                        {/* Add more aggregated tile here */}
                        <GridItem rowSpan={1} colSpan={1}>
                            <Tile
                                header="Collection to Processing"
                                stat="90"
                                units="participants"
                                units_colour="#a1c938"
                                description="The number of people who have consented but not given blood."
                                tile_icon="/dashboard_icons/green_truck.svg"
                            />
                        </GridItem>
                    </Grid>
                </GridItem>
                <GridItem rowSpan={2} colSpan={2} border={'1px solid'}>
                    <OurDonutChart
                        header="Where we collected our samples"
                        data={
                            data.project.ourdnaDashboard[0].total_samples_by_collection_event_name
                        }
                        icon="/dashboard_icons/blue_testtube.svg"
                    />
                </GridItem>
                <GridItem rowSpan={3} colSpan={4} border={'1px solid'}>
                    <BarChart
                        header="Processing time per site"
                        data={data.project.ourdnaDashboard[0].processing_times_by_site}
                        icon="/dashboard_icons/yellow_clock.svg"
                    />
                </GridItem>
                <GridItem rowSpan={3} colSpan={1} border={'1px solid'}>
                    <Grid
                        templateRows="repeat(2, minmax(100px, 50%))"
                        templateColumns="repeat(1, 100%)"
                        height="100%"
                        gap={5}
                    >
                        <GridItem rowSpan={1} colSpan={1} border={'1px solid'}>
                            <TableTile
                                header="Viable Long Read"
                                data={data.project.ourdnaDashboard[0].samples_concentration_gt_1ug}
                                columns={['Sample ID', 'Concentration']}
                                tile_icon="/dashboard_icons/green_rocket.svg"
                            />
                        </GridItem>
                        <GridItem rowSpan={1} colSpan={1} border={'1px solid'}>
                            <TableTile
                                header="Processed > 24h"
                                data={samplesLostAfterCollections}
                                columns={['Sample ID', 'Time (h)']}
                                tile_icon="/dashboard_icons/red_alarm.svg"
                            />
                        </GridItem>
                    </Grid>
                </GridItem>
            </Grid>
        </>
    ) : (
        <MuckError message={`Ah Muck, there's no data here`} />
    )
}

export default Dashboard
