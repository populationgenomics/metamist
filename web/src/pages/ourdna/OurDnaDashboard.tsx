import React from 'react'
import { Container, Grid, GridRow, GridColumn } from 'semantic-ui-react'
import { useQuery } from '@apollo/client'
import { gql } from '../../__generated__'

import Tile from '../../shared/components/ourdna/Tile'
import TableTile from '../../shared/components/ourdna/TableTile'
import StatTile from '../../shared/components/ourdna/StatTile'

import OurDNALogo from '../../shared/components/OurDNALogo'
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

const GET_OURDNA_DASHBOARD = gql(`
    query DashboardQuery($project: String!) {
        project(name: $project) {
            ourdnaDashboard {
                collectionToProcessEndTime
                collectionToProcessEndTime24h
                collectionToProcessEndTimeStatistics
                participantsConsentedNotCollected
                participantsSignedNotConsented
                processingTimesBySite
                samplesConcentrationGt1ug
                samplesLostAfterCollection {
                    collectionLab
                    collectionTime
                    courier
                    courierActualDropoffTime
                    courierActualPickupTime
                    courierScheduledDropoffTime
                    courierScheduledPickupTime
                    courierTrackingNumber
                    processEndTime
                    processStartTime
                    receivedBy
                    receivedTime
                    sampleId
                    timeSinceCollection
                }
                totalSamplesByCollectionEventName
            }
        }
    }
`)

const iconStyle = {
    marginRight: '10px',
    width: '24px',
    height: '24px',
}

const Dashboard = () => {
    const { loading, error, data } = useQuery(GET_OURDNA_DASHBOARD, {
        variables: { project: import.meta.env.VITE_OURDNA_PROJECT_NAME || 'ourdna' },
    })

    if (!data) return <MuckError message={`Ah Muck, there's no data here`} />
    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>

    const samplesLostAfterCollections =
        data.project.ourdnaDashboard.samplesLostAfterCollection.reduce(
            (rr: Record<string, any>, sample) => {
                rr[sample.sampleId] = sample.timeSinceCollection / 3600
                return rr
            },
            {}
        )

    return (
        <>
            <Container
                style={{
                    marginTop: 0,
                    paddingBottom: '20px',
                    display: 'flex',
                    justifyContent: 'center',
                    alignItems: 'center',
                }}
            >
                <OurDNALogo className="ourdna-logo-responsive" />
            </Container>
            <Grid stackable>
                <GridRow centered>
                    <GridColumn width={10}>
                        <Grid stackable columns={2}>
                            <GridColumn>
                                <Tile
                                    header="Awaiting Consent"
                                    stat={`${data.project.ourdnaDashboard.participantsSignedNotConsented.length}`}
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
                                    stat={`${data.project.ourdnaDashboard.participantsConsentedNotCollected.length}`}
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
                                            data.project.ourdnaDashboard.samplesLostAfterCollection
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
                                                    .collectionToProcessEndTimeStatistics.min / 3600
                                            ).toFixed(1)}`,
                                            units: 'shortest (h)',
                                            unitsColour: 'ourdna-green-transparent',
                                        },
                                        {
                                            value: `${(
                                                data.project.ourdnaDashboard
                                                    .collectionToProcessEndTimeStatistics.max / 3600
                                            ).toFixed(1)}`,
                                            units: 'longest (h)',
                                            unitsColour: 'ourdna-red-transparent',
                                        },
                                        {
                                            value: `${(
                                                data.project.ourdnaDashboard
                                                    .collectionToProcessEndTimeStatistics.average /
                                                3600
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
                            data={data.project.ourdnaDashboard.totalSamplesByCollectionEventName}
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
                            data={data.project.ourdnaDashboard.processingTimesBySite}
                            icon={
                                <ClockIcon fill={ourdnaColours.yellow} style={{ ...iconStyle }} />
                            }
                        />
                    </GridColumn>
                    <GridColumn width={6}>
                        <Grid stackable columns={1}>
                            <GridColumn>
                                <TableTile
                                    header="Viable Long Read"
                                    data={data.project.ourdnaDashboard.samplesConcentrationGt1ug}
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
                                    header="Lost Samples"
                                    data={samplesLostAfterCollections}
                                    columns={['Sample ID', 'Time (h)']}
                                    icon={
                                        <AlarmIcon
                                            fill={ourdnaColours.red}
                                            style={{ ...iconStyle }}
                                        />
                                    }
                                />
                            </GridColumn>
                        </Grid>
                    </GridColumn>
                </GridRow>
            </Grid>
        </>
    )
}

export default Dashboard
