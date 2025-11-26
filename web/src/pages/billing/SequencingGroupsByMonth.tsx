import { useQuery } from '@apollo/client'
import * as React from 'react'
import { useRef } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { useMeasure } from 'react-use'
import { Button, Card, Checkbox, Grid, Message } from 'semantic-ui-react'
import { gql } from '../../__generated__/gql'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import ProjectSelector, { IMetamistProject } from '../project/ProjectSelector'

import * as Plot from '@observablehq/plot'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import './components/BillingCostByTimeTable.css'

const GET_SG_BY_MONTH = gql(`
    query SequencingGroupHistory($name: String!, $split: Boolean!) {
        project(name: $name) {
            sequencingGroupHistory {
                date
                type
                technology @include(if: $split)
                count
            }
        }
    }
`)

interface ISGByMonthDisplayProps {
    projectName: string
    groupTypes: boolean
}

interface ITypeData {
    date: Date
    type: string
    grouping: string
    count: number
}

const SequencingGroupsByMonth: React.FunctionComponent = () => {
    // Use navigate and update url params
    const navigate = useNavigate()
    const { projectName } = useParams()

    const [groupTypes, setGroupTypes] = React.useState<boolean>(false)

    // Callback to update the url.
    const onProjectSelect = (_project: IMetamistProject) => {
        navigate(`/billing/sequencingGroupsByMonth/${_project.name}`)
    }

    const content = () => {
        if (!projectName) {
            return <Message negative>No project selected</Message>
        }

        return <SequencingGroupsByMonthDisplay projectName={projectName} groupTypes={groupTypes} />
    }

    return (
        <>
            <Card fluid style={{ padding: '20px' }} id="billing-container">
                <div
                    className="header-container"
                    style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'flex-start',
                        flexWrap: 'wrap',
                        gap: '10px',
                    }}
                >
                    <h1
                        style={{
                            fontSize: 40,
                            margin: 0,
                            flex: '1 1 200px',
                        }}
                    >
                        Sequencing Groups By Month
                    </h1>
                </div>

                <Grid columns="equal" stackable doubling>
                    <Grid.Column className="field-selector-label">
                        <ProjectSelector onProjectSelect={onProjectSelect} />
                    </Grid.Column>
                    <Grid.Column width={2}>
                        <Checkbox
                            label="Group by Type"
                            fitted
                            toggle
                            checked={groupTypes}
                            onChange={() => setGroupTypes(!groupTypes)}
                            style={{ position: 'absolute', bottom: '20px' }}
                        />
                    </Grid.Column>
                </Grid>
            </Card>

            {content()}
        </>
    )
}

const SequencingGroupsByMonthDisplay: React.FunctionComponent<ISGByMonthDisplayProps> = ({
    projectName,
    groupTypes,
}) => {
    // Data loading
    const { loading, error, data } = useQuery(GET_SG_BY_MONTH, {
        variables: { name: projectName, split: !groupTypes },
        notifyOnNetworkStatusChange: true,
    })

    // Chart refs
    const containerRef = useRef<HTMLDivElement>(null)
    const [measureRef, { width, height }] = useMeasure<HTMLDivElement>()

    // Converting the date that GraphQL gives from a string to a Date type.
    const plotData = React.useMemo(() => {
        if (loading || error || !data) return []

        if (groupTypes) {
        }
        return data.project.sequencingGroupHistory.map((item) => {
            return {
                date: new Date(item.date),
                type: groupTypes ? item.type : item.type + ': ' + item.technology,
                grouping: item.type,
                count: item.count,
            } as ITypeData
        })
    }, [data, loading, error, groupTypes])

    // Code for generating the sequencing groups by month plot.
    React.useEffect(() => {
        if (plotData.length == 0) return

        const plot = Plot.plot({
            color: {
                scheme: 'spectral',
                legend: true,
            },
            x: { interval: 'month' },
            y: { grid: true },
            width,
            height,
            marks: [
                Plot.barY(
                    plotData,
                    Plot.stackY({
                        x: 'date',
                        y: 'count',
                        interval: 'month',
                        fill: 'type',
                        order: '-sum',
                        z: 'grouping',
                        tip: true,
                        inset: 0,
                    })
                ),
            ],
        })
        containerRef.current?.append(plot)

        return () => plot.remove()
    }, [plotData, width, height])

    // Component to display a message when data isn't loaded or there's an error.
    const messageComponent = () => {
        if (error) {
            return (
                <Message negative>
                    <p>{error.message}</p>
                    <Button negative onClick={() => window.location.reload()}>
                        Retry
                    </Button>
                </Message>
            )
        }

        if (loading) {
            return (
                <div>
                    <LoadingDucks />
                    <p style={{ textAlign: 'center', marginTop: '5px' }}>
                        <em>This query takes a while...</em>
                    </p>
                </div>
            )
        }
        return null
    }

    // Component to display the sequencing groups by month plot when data is available.
    const dataComponent = () => {
        if (error || loading) {
            return null
        }

        if (!error && !loading && plotData.length === 0) {
            return (
                <Card
                    fluid
                    style={{ padding: '20px', overflowX: 'scroll' }}
                    id="billing-container-charts"
                >
                    <Message negative>No Data</Message>
                </Card>
            )
        }

        return (
            <Card
                fluid
                style={{ padding: '20px', overflowX: 'scroll' }}
                id="billing-container-charts"
            >
                <Grid>
                    <Grid.Column width={16} className="chart-card">
                        <div
                            ref={measureRef}
                            style={{
                                width: '100%',
                                height: '100%',
                                maxHeight: '35em',
                                position: 'absolute',
                                zIndex: -1,
                                top: 0,
                                left: 0,
                            }}
                        />
                        <div ref={containerRef} />
                    </Grid.Column>
                </Grid>
            </Card>
        )
    }

    return (
        <>
            {messageComponent()}

            {dataComponent()}
        </>
    )
}

export default function BillingStorageCostPerSamplePage() {
    return (
        <PaddedPage>
            <SequencingGroupsByMonth />
        </PaddedPage>
    )
}
