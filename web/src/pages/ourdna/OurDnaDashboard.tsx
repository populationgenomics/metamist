import React from 'react'
import { CardGroup, Container } from 'semantic-ui-react'

import { Box, Flex, Image, Grid, GridItem, useBreakpointValue } from '@chakra-ui/react'

import { gql, useQuery } from '@apollo/client'
// import { gql } from '../../__generated__/gql'

// import './OurDnaDashboard.css'

import Tile from '../../shared/components/ourdna/Tile'
import TableTile from '../../shared/components/ourdna/TableTile'
import StatTile from '../../shared/components/ourdna/StatTile'

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

const icons = {
    clipboard: '/dashboard_icons/blue_clipboard.svg',
    syringe: '/dashboard_icons/yellow_syringe.svg',
    red_bloodsample: '/dashboard_icons/red_bloodsample.svg',
    green_truck: '/dashboard_icons/green_truck.svg',
    yellow_clock: '/dashboard_icons/yellow_clock.svg',
    blue_testtube: '/dashboard_icons/blue_testtube.svg',
    green_rocket: '/dashboard_icons/green_rocket.svg',
    red_alarm: '/dashboard_icons/red_alarm.svg',
} as const

const Dashboard = () => {
    const { loading, error, data } = useQuery(GET_OURDNA_DASHBOARD, {
        variables: { project: 'greek-myth' },
    })

    const containerSizesRow = useBreakpointValue({
        base: 'repeat(2, 1fr)',
        sm: 'repeat(8, 1fr)',
        md: 'repeat(2, 1fr)',
        lg: 'repeat(2, 1fr)',
    })
    const containerSizesColumn = useBreakpointValue({
        base: 'repeat(5, 1fr)',
        sm: 'repeat(1, 1fr)',
        md: 'repeat(5, 1fr)',
        lg: 'repeat(5, 1fr)',
    })
    const tileSizesRow = useBreakpointValue({
        base: 'repeat(2, 1fr)',
        sm: 'repeat(4, 1fr)',
        md: 'repeat(4, 1fr)',
        lg: 'repeat(2, 1fr)',
    })
    const tileSizesColumn = useBreakpointValue({
        base: 'repeat(2, 1fr)',
        sm: 'repeat(1, 1fr)',
        md: 'repeat(1, 1fr)',
        lg: 'repeat(2, 1fr)',
    })

    if (!data) return <MuckError message={`Ah Muck, there's no data here`} />
    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>

    const samplesLostAfterCollections = Object.entries(
        data.project.ourdnaDashboard[0].samples_lost_after_collection
    ).reduce((rr: Record<string, number>, [key, sample]) => {
        rr[key] = sample.time_to_process_start / 3600
        return rr
    }, {})

    return (
        <>
            <Box marginTop={0} paddingBottom={5}>
                <Image maxWidth={['100px', '100px', '150px']} src="/logo_option2.png" />
            </Box>
            <Grid
                h="100vh"
                templateRows={containerSizesRow}
                templateColumns={containerSizesColumn}
                gap={6}
            >
                <GridItem
                    rowSpan={1}
                    colSpan={3}
                    // border={'1px solid'}
                >
                    <Grid
                        templateRows="auto"
                        templateColumns={tileSizesColumn}
                        gap={[3, 6]}
                        h="100%"
                        // border={'1px solid blue'}
                    >
                        <GridItem rowSpan={1} colSpan={1}>
                            <Tile
                                header="Awaiting Consent"
                                stat={`${data.project.ourdnaDashboard[0].participants_signed_not_consented.length}`}
                                units="participants"
                                units_colour="#71ace1"
                                description="The number of people who have signed up but not consented."
                                tile_icon={icons['clipboard']}
                            />
                        </GridItem>
                        <GridItem rowSpan={1} colSpan={1}>
                            <Tile
                                header="Awaiting Collection"
                                stat={`${data.project.ourdnaDashboard[0].participants_consented_not_collected.length}`}
                                units="participants"
                                units_colour="#e8c71d"
                                description="The number of people who have consented but not given blood."
                                tile_icon={icons['syringe']}
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
                                tile_icon={icons['red_bloodsample']}
                            />
                        </GridItem>
                        {/* Add more aggregated tile here */}
                        <GridItem rowSpan={1} colSpan={1}>
                            <StatTile
                                header="Collection to Processing"
                                stats={[
                                    {
                                        value: `${(
                                            data.project.ourdnaDashboard[0]
                                                .collection_to_process_end_time_statistics.min /
                                            3600
                                        ).toFixed(1)}`,
                                        units: 'shortest (h)',
                                        unitsColour: '#a1c938',
                                    },
                                    {
                                        value: `${(
                                            data.project.ourdnaDashboard[0]
                                                .collection_to_process_end_time_statistics.max /
                                            3600
                                        ).toFixed(1)}`,
                                        units: 'longest (h)',
                                        unitsColour: '#bf003c',
                                    },
                                    {
                                        value: `${(
                                            data.project.ourdnaDashboard[0]
                                                .collection_to_process_end_time_statistics.average /
                                            3600
                                        ).toFixed(1)}`,
                                        units: 'average (h)',
                                        unitsColour: '#71ace1',
                                    },
                                ]}
                                description="The time between collection and processing for each sample."
                                tile_icon={icons['green_truck']}
                            />
                        </GridItem>
                    </Grid>
                </GridItem>
                <GridItem
                    rowSpan={1}
                    colSpan={2}
                    // border={'1px solid'}
                >
                    <OurDonutChart
                        header="Where we collected our samples"
                        data={
                            data.project.ourdnaDashboard[0].total_samples_by_collection_event_name
                        }
                        icon={icons['blue_testtube']}
                    />
                </GridItem>
                <GridItem
                    rowSpan={1}
                    colSpan={4}
                    // border={'1px solid'}
                >
                    <BarChart
                        header="Processing time per site"
                        data={data.project.ourdnaDashboard[0].processing_times_by_site}
                        icon={icons['yellow_clock']}
                    />
                </GridItem>
                <GridItem
                    rowSpan={1}
                    colSpan={1}
                    // border={'1px solid'}
                >
                    <Grid
                        templateRows="auto"
                        templateColumns="1fr"
                        height="100%"
                        maxHeight="50vh"
                        rowGap="2vh"
                    >
                        <GridItem
                            rowSpan={1}
                            colSpan={1}
                            // border={'1px solid'}
                        >
                            <TableTile
                                header="Viable Long Read"
                                data={data.project.ourdnaDashboard[0].samples_concentration_gt_1ug}
                                columns={['Sample ID', 'Concentration']}
                                tile_icon={icons['green_rocket']}
                            />
                        </GridItem>
                        <GridItem
                            rowSpan={1}
                            colSpan={1}
                            // border={'1px solid'}
                        >
                            <TableTile
                                header="Processed > 24h"
                                data={samplesLostAfterCollections}
                                columns={['Sample ID', 'Time (h)']}
                                tile_icon={icons['red_alarm']}
                            />
                        </GridItem>
                    </Grid>
                </GridItem>
            </Grid>
        </>
    )
}

export default Dashboard
