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
import { ourdnaColours } from '../../shared/components/ourdna/Colours'

import ClipboardIcon from '../../shared/components/ourdna/dashboardIcons/ClipboardIcon'
import SyringeIcon from '../../shared/components/ourdna/dashboardIcons/SyringeIcon'
import TestTubeIcon from '../../shared/components/ourdna/dashboardIcons/TestTubeIcon'
import BloodSampleIcon from '../../shared/components/ourdna/dashboardIcons/BloodSampleIcon'
import TruckIcon from '../../shared/components/ourdna/dashboardIcons/TruckIcon'
import ClockIcon from '../../shared/components/ourdna/dashboardIcons/ClockIcon'
import RocketIcon from '../../shared/components/ourdna/dashboardIcons/RocketIcon'
import AlarmIcon from '../../shared/components/ourdna/dashboardIcons/AlarmIcon'

const GET_OURDNA_DASHBOARD = gql`
    query DashboardQuery($project: String!) {
        project(name: $project) {
            ourdnaDashboard
        }
    }
`

const iconStyle = {
    marginRight: '10px',
    width: '24px',
    height: '24px',
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
                                    unitsColour="ourdna-blue-transparent"
                                    description="The number of people who have signed up but not consented."
                                    icon={
                                        <ClipboardIcon
                                            fill={ourdnaColours.blue}
                                            style={{ ...iconStyle }}
                                        />
                                    }
                                />
                            </GridColumn>
                            <GridColumn>
                                <Tile
                                    header="Awaiting Collection"
                                    stat={`${data.project.ourdnaDashboard.participants_consented_not_collected.length}`}
                                    units="participants"
                                    unitsColour="ourdna-yellow-transparent"
                                    description="The number of people who have consented but not given blood."
                                    icon={
                                        <SyringeIcon
                                            fill={ourdnaColours.yellow}
                                            style={{ ...iconStyle }}
                                        />
                                    }
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
                                    unitsColour="ourdna-red-transparent"
                                    description="Blood has been collected, but was not processed within 72 hours."
                                    icon={
                                        <BloodSampleIcon
                                            fill={ourdnaColours.red}
                                            style={{ ...iconStyle }}
                                        />
                                    }
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
                                    icon={
                                        <TruckIcon
                                            fill={ourdnaColours.green}
                                            style={{ ...iconStyle }}
                                        />
                                    }
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
                            icon={
                                <TestTubeIcon fill={ourdnaColours.blue} style={{ ...iconStyle }} />
                            }
                        />
                    </GridColumn>
                </GridRow>
                <GridRow centered>
                    <GridColumn width={10}>
                        <BarChart
                            header="Processing time per site"
                            data={data.project.ourdnaDashboard.processing_times_by_site}
                            icon={
                                <ClockIcon fill={ourdnaColours.yellow} style={{ ...iconStyle }} />
                            }
                        />
                    </GridColumn>
                    <GridColumn width={6} stackable>
                        <GridColumn>
                            <TableTile
                                header="Viable Long Read"
                                data={data.project.ourdnaDashboard.samples_concentration_gt_1ug}
                                columns={['Sample ID', 'Concentration']}
                                icon={
                                    <RocketIcon
                                        fill={ourdnaColours.green}
                                        style={{ ...iconStyle }}
                                    />
                                }
                            />
                        </GridColumn>
                        <GridColumn>
                            <TableTile
                                header="Processed > 24h"
                                data={samplesLostAfterCollections}
                                columns={['Sample ID', 'Time (h)']}
                                icon={
                                    <AlarmIcon fill={ourdnaColours.red} style={{ ...iconStyle }} />
                                }
                            />
                        </GridColumn>
                    </GridColumn>
                </GridRow>
            </Grid>
        </>
    )
}

export default Dashboard
