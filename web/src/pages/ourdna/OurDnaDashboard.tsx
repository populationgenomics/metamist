import React from 'react'
import { CardGroup, Container, Grid, GridRow, GridColumn, Image } from 'semantic-ui-react'
import { gql, useQuery } from '@apollo/client'

import Tile from '../../shared/components/ourdna/Tile'
import TableTile from '../../shared/components/ourdna/TableTile'
import StatTile from '../../shared/components/ourdna/StatTile'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import MuckError from '../../shared/components/MuckError'
import OurDonutChart from '../../shared/components/ourdna/OurDonutChart'
import BarChart from '../../shared/components/ourdna/BarChart'

const GET_OURDNA_DASHBOARD = gql`
    query DashboardQuery($project: String!) {
        project(name: $project) {
            ourdnaDashboard
        }
    }
`

const icons = {
    clipboard: '/dashboard_icons/blue_clipboard.svg',
    syringe: '/dashboard_icons/yellow_syringe.svg',
    red_bloodsample: '/dashboard_icons/red_bloodsample.svg',
    green_truck: '/dashboard_icons/green_truck.svg',
    yellow_clock: '/dashboard_icons/yellow_clock.svg',
    blue_testtube: '/dashboard_icons/blue_testtube.svg',
    green_rocket: '/dashboard_icons/green_rocket.svg',
    red_alarm: '/dashboard_icons/red_alarm.svg',
}

type OurDNADashboardData = {
    project: {
        ourdnaDashboard: {
            participants_signed_not_consented: number[]
            participants_consented_not_collected: number[]
            samples_lost_after_collection: Record<string, Record<string, any>>
            collection_to_process_end_time: Record<string, number>
            collection_to_process_end_time_statistics: {
                min: number
                max: number
                average: number
            }
            collection_to_process_end_time_24h: Record<string, number>
            total_samples_by_collection_event_name: Record<string, number>
            processing_times_by_site: Record<string, Record<number, number>>
            samples_concentration_gt_1ug: Record<string, number>
        }
    }
}
const Dashboard = () => {
    const { loading, error, data } = useQuery<OurDNADashboardData>(GET_OURDNA_DASHBOARD, {
        variables: { project: import.meta.env.VITE_OURDNA_PROJECT_NAME || 'ourdna' },
    })

    if (!data) return <MuckError message={`Ah Muck, there's no data here`} />
    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>

    const samplesLostAfterCollections = Object.entries(
        data.project.ourdnaDashboard.samples_lost_after_collection
    ).reduce((rr, [key, sample]) => {
        rr[key] = sample.time_to_process_start / 3600
        return rr
    }, {})

    return (
        <>
            <Container style={{ marginTop: 0, paddingBottom: '20px' }}>
                <Image src="/logo_option2.png" size="small" centered />
            </Container>
            <Grid stackable>
                <GridRow centered>
                    <GridColumn width={10}>
                        <Grid stackable columns={2}>
                            <GridColumn>
                                <Tile
                                    header="Awaiting Consent"
                                    stat={`${data.project.ourdnaDashboard.participants_signed_not_consented.length}`}
                                    units="participants"
                                    units_colour="ourdna-blue-transparent"
                                    description="The number of people who have signed up but not consented."
                                    tile_icon={icons.clipboard}
                                />
                            </GridColumn>
                            <GridColumn>
                                <Tile
                                    header="Awaiting Collection"
                                    stat={`${data.project.ourdnaDashboard.participants_consented_not_collected.length}`}
                                    units="participants"
                                    units_colour="ourdna-yellow-transparent"
                                    description="The number of people who have consented but not given blood."
                                    tile_icon={icons.syringe}
                                />
                            </GridColumn>
                            <GridColumn>
                                <Tile
                                    header="Samples Lost"
                                    stat={`${
                                        Object.keys(
                                            data.project.ourdnaDashboard
                                                .samples_lost_after_collection
                                        ).length
                                    }`}
                                    units="samples"
                                    units_colour="ourdna-red-transparent"
                                    description="Blood has been collected, but was not processed within 72 hours."
                                    tile_icon={icons.red_bloodsample}
                                />
                            </GridColumn>
                            <GridColumn>
                                <StatTile
                                    header="Collection to Processing"
                                    stats={[
                                        {
                                            value: `${(
                                                data.project.ourdnaDashboard
                                                    .collection_to_process_end_time_statistics.min /
                                                3600
                                            ).toFixed(1)}`,
                                            units: 'shortest (h)',
                                            unitsColour: 'ourdna-green-transparent',
                                        },
                                        {
                                            value: `${(
                                                data.project.ourdnaDashboard
                                                    .collection_to_process_end_time_statistics.max /
                                                3600
                                            ).toFixed(1)}`,
                                            units: 'longest (h)',
                                            unitsColour: 'ourdna-red-transparent',
                                        },
                                        {
                                            value: `${(
                                                data.project.ourdnaDashboard
                                                    .collection_to_process_end_time_statistics
                                                    .average / 3600
                                            ).toFixed(1)}`,
                                            units: 'average (h)',
                                            unitsColour: 'ourdna-blue-transparent',
                                        },
                                    ]}
                                    description="The time between collection and processing for each sample."
                                    tile_icon={icons.green_truck}
                                />
                            </GridColumn>
                        </Grid>
                    </GridColumn>
                    <GridColumn width={6}>
                        <OurDonutChart
                            header="Where we collected our samples"
                            data={
                                data.project.ourdnaDashboard.total_samples_by_collection_event_name
                            }
                            icon={icons.blue_testtube}
                        />
                    </GridColumn>
                </GridRow>
                <GridRow centered>
                    <GridColumn width={10}>
                        <BarChart
                            header="Processing time per site"
                            data={data.project.ourdnaDashboard.processing_times_by_site}
                            icon={icons.yellow_clock}
                        />
                    </GridColumn>
                    <GridColumn width={6} stackable>
                        <GridColumn>
                            <TableTile
                                header="Viable Long Read"
                                data={data.project.ourdnaDashboard.samples_concentration_gt_1ug}
                                columns={['Sample ID', 'Concentration']}
                                tile_icon={icons.green_rocket}
                            />
                        </GridColumn>
                        <GridColumn>
                            <TableTile
                                header="Processed > 24h"
                                data={samplesLostAfterCollections}
                                columns={['Sample ID', 'Time (h)']}
                                tile_icon={icons.red_alarm}
                            />
                        </GridColumn>
                    </GridColumn>
                </GridRow>
            </Grid>
        </>
    )
}

export default Dashboard
