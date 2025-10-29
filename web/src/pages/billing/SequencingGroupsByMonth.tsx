import { debounce } from 'lodash'
import * as React from 'react'
import { useRef } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { useMeasure } from 'react-use'
import { Button, Card, Grid, Message } from 'semantic-ui-react'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import ProjectSelector, { IMetamistProject } from '../project/ProjectSelector'

import * as Plot from '@observablehq/plot'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import { SequencingGroupApi } from '../../sm-api'
import './components/BillingCostByTimeTable.css'

type ProjectHistory = {
    [key: string]: {
        [key: string]: number
    }
}

interface TypeData {
    date: Date
    type: string
    count: number
}

const SequencingGroupsByMonth: React.FunctionComponent = () => {
    // Use navigate and update url params
    const navigate = useNavigate()
    const { projectName } = useParams()

    // Data loading
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [message, setMessage] = React.useState<string | undefined>()
    const [data, setData] = React.useState<TypeData[]>([])

    // Chart refs
    const containerRef = useRef<HTMLDivElement>(null)
    const [measureRef, { width, height }] = useMeasure<HTMLDivElement>()

    // Callback to get data for plotting.
    const onProjectSelect = (_project: IMetamistProject) => {
        navigate(`/billing/sequencingGroupsByMonth/${_project.name}`)
    }

    const getData = React.useCallback(async (project: string | undefined) => {
        if (project == undefined) {
            return
        }

        setIsLoading(true)
        setError(undefined)
        setMessage(undefined)

        try {
            const sg_api = new SequencingGroupApi()
            const result = await sg_api.sequencingGroupHistory(project)
            setIsLoading(false)

            const resultData = result.data as ProjectHistory
            const newDataset: TypeData[] = []
            for (const [date, counts] of Object.entries(resultData)) {
                for (const [typeName, count] of Object.entries(counts)) {
                    newDataset.push({
                        date: new Date(date),
                        type: typeName,
                        count: count,
                    })
                }
            }

            setData(newDataset)
        } catch (er: any) {
            setIsLoading(false)
            setError(er.message)
        }
    }, [])

    // Memo of debounced getData so that the debounce function will only be regenerated when the project changes.
    const debouncedGetData = React.useMemo(() => {
        return debounce(() => {
            getData(projectName)
        }, 1000)
    }, [projectName, getData])

    // Cleanup debounced function on unmount.
    React.useEffect(() => {
        return () => {
            debouncedGetData.cancel()
        }
    }, [projectName, debouncedGetData])

    // Effect to trigger data retrieval on initial load and project value changes.
    React.useEffect(() => {
        debouncedGetData()
    }, [debouncedGetData])

    React.useEffect(() => {
        if (!data) return

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
                    data,
                    Plot.stackY({
                        x: 'date',
                        y: 'count',
                        interval: 'month',
                        fill: 'type',
                        order: '-sum',
                        tip: true,
                    })
                ),
                Plot.ruleY([0]),
            ],
        })
        containerRef.current?.append(plot)

        return () => plot.remove()
    }, [data, width, height])

    const messageComponent = () => {
        if (message) {
            return (
                <Message negative onDismiss={() => setError(undefined)}>
                    {message}
                </Message>
            )
        }
        if (error) {
            return (
                <Message negative onDismiss={() => setError(undefined)}>
                    {error}
                    <br />
                    <Button negative onClick={() => window.location.reload()}>
                        Retry
                    </Button>
                </Message>
            )
        }
        if (isLoading) {
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

    const dataComponent = () => {
        if (message || error || isLoading) {
            return null
        }

        if (!message && !error && !isLoading && (!data || data.length === 0)) {
            return (
                <Card
                    fluid
                    style={{ padding: '20px', overflowX: 'scroll' }}
                    id="billing-container-charts"
                >
                    No Data
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
                    <div
                        className="button-container"
                        style={{
                            display: 'flex',
                            gap: '10px',
                            alignItems: 'stretch',
                            flex: '0 0 auto',
                            minWidth: '115px',
                        }}
                    ></div>
                </div>

                <Grid columns="equal" stackable doubling>
                    <Grid.Column className="field-selector-label">
                        <ProjectSelector onProjectSelect={onProjectSelect} />
                    </Grid.Column>
                </Grid>
            </Card>

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
