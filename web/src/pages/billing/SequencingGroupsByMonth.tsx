import { SelectChangeEvent } from '@mui/material/Select'
import { debounce } from 'lodash'
import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { Button, Card, Dropdown, Grid, Message } from 'semantic-ui-react'
import {
    getCurrentInvoiceMonth,
    getCurrentInvoiceYearStart,
} from '../../shared/utilities/formatDates'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import { exportTable } from '../../shared/utilities/exportTable'
import generateUrl from '../../shared/utilities/generateUrl'
import {
    BillingApi,
    BillingColumn,
    SequencingGroupApi,
} from '../../sm-api'
import './components/BillingCostByTimeTable.css'
import FieldSelector from './components/FieldSelector'
import CostByTimeBarChart from './components/CostByTimeBarChart'
import { RequiredError } from '../../sm-api/base'

type TypeCounts = Record<string, number>
type ProjectHistory = Record<string, TypeCounts>

/* eslint-disable @typescript-eslint/no-explicit-any  -- too many anys in the file to fix right now but would be good to sort out when we can */
const SequencingGroupsByMonth: React.FunctionComponent = () => {
const [searchParams] = useSearchParams()

    // Pull search params for use in the component
    const inputTopics = searchParams.get('topics')
    const initialTopics = inputTopics ? inputTopics.split(',').filter((t) => t.trim() !== '') : []

    const [start, setStart] = React.useState<string>(
        searchParams.get('start') ?? getCurrentInvoiceYearStart()
    )
    const [end, setEnd] = React.useState<string>(
        searchParams.get('end') ?? getCurrentInvoiceMonth()
    )

    // Topic filtering state
    const [selectedProjects, setSelectedProjects] = React.useState<string[]>(initialTopics)
    const [availableProjects, setAvailableProjects] = React.useState<string[]>([])

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    // Data loading
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [message, setMessage] = React.useState<string | undefined>()
    const [months, setMonths] = React.useState<string[]>([])
    const [data, setData] = React.useState<any>([])
    const [groups, setGroups] = React.useState<string[]>([])

    const updateNav = (st: string, ed: string, projects?: string[]) => {
        const url = generateUrl(location, {
            start: st,
            end: ed,
            projects: projects && projects.length > 0 ? projects.join(',') : undefined,
        })
        navigate(url)
    }

    const changeDate = (name: string, value: string) => {
        let start_update = start
        let end_update = end
        if (name === 'start') start_update = value
        if (name === 'end') end_update = value
        setStart(start_update)
        setEnd(end_update)
        updateNav(start_update, end_update, selectedProjects)
    }

    // Pre-fetch projects data on component mount
    React.useEffect(() => {
        // Pre-fetch projects in parallel (consistent with other files pattern)
        Promise.all([new BillingApi().getGcpProjects()])
            .then(([projectsResponse]) => {
                setAvailableProjects(projectsResponse.data || [])
            })
            .catch((error) => {
                console.error('Error pre-fetching data:', error)
                setAvailableProjects([])
            })
            .finally(() => {})
    }, [])

    // Callback to get data for plotting.
    const onProjectSelect = (
        event: SelectChangeEvent<string | string[]> | undefined,
        data: { value: string | string[] }
    ) => {
        const value = Array.isArray(data.value) ? data.value : [data.value]
        setSelectedProjects(value)
        updateNav(start, end, value)
    }

    const getData = React.useCallback(async (query: Array<string>) => {
        setIsLoading(true)
        setError(undefined)
        setMessage(undefined)
        try {
            const sg_api = new SequencingGroupApi()
            const result = await sg_api.sequencingGroupHistory(query)
            setIsLoading(false)
            
            const resultData = result.data as Record<string, ProjectHistory>

            const newGroups = new Set<string>()
            const newDataset = Object.entries(resultData["1"]).map(([date, typeCounts]) => ({
                date: new Date(date),
                values: typeCounts
            }))
            
            newDataset.forEach((dateRecord) => {
                Object.keys(dateRecord.values).forEach((typeKey) => newGroups.add(typeKey))
            })

            setData(newDataset)
            setGroups(Array.from(newGroups))
        } catch(er: any) {
            setError(er.message)
        }
    }, [])

    // Memo of debounced getData so that its only re-runs when selectedProjects changes.
    const debouncedGetData = React.useMemo(() => {return debounce(() => {getData(selectedProjects)}, 1000)},
        [selectedProjects]
    )

    // Cleanup debounced function on unmount.
    React.useEffect(() => {
        return () => {
            debouncedGetData.cancel()
        }
    }, [selectedProjects])

    // Effect to trigger data retrieval on initial load and filter value changes.
    React.useEffect(() => {
        debouncedGetData()
    }, [debouncedGetData])

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
        console.log(data)
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
            <>
                <Card
                    fluid
                    style={{ padding: '20px', overflowX: 'scroll' }}
                    id="billing-container-charts"
                >
                    <Grid>
                        <Grid.Column width={16} className="chart-card">
                            <CostByTimeBarChart
                            isLoading={isLoading}
                            accumulate={false}
                            data={data}
                            extrapolate={false}
                        />
                        </Grid.Column>
                    </Grid>
                </Card>
            </>
        )
    }

    const onMonthStart = (event: any, data: any) => {
        changeDate('start', data.value)
    }

    const onMonthEnd = (event: any, data: any) => {
        changeDate('end', data.value)
    }

    /* eslint-disable react-hooks/exhaustive-deps */
    React.useEffect(() => {
        if (Boolean(start) && Boolean(end)) {
            // Check if start date is after end date
            if (start > end) {
                setIsLoading(false)
                setError(undefined)
                setMessage(
                    'Start date cannot be later than end date. Please adjust your selection.'
                )
                return
            }
            // Use debounced function for topic filtering
            // debouncedGetData(start, end, selectedProjects)
        } else {
            // invalid selection,
            setIsLoading(false)
            setError(undefined)

            if (start === undefined || start === null || start === '') {
                setMessage('Please select Start date')
            } else if (end === undefined || end === null || end === '') {
                setMessage('Please select End date')
            }
        }
    }, [start, end, selectedProjects]) //, debouncedGetData])
    /* eslint-enable react-hooks/exhaustive-deps */

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
                    >
                        <Dropdown
                            button
                            className="icon"
                            floating
                            labeled
                            icon="download"
                            text="Export"
                            style={{
                                minWidth: '115px',
                                height: '36px',
                            }}
                        >
                            <Dropdown.Menu>
                                <Dropdown.Item
                                    key="csv"
                                    text="Export to CSV"
                                    icon="file excel"
                                    // onClick={() => exportToFile('csv')}
                                    onClick={() => {}}
                                />
                                <Dropdown.Item
                                    key="tsv"
                                    text="Export to TSV"
                                    icon="file text outline"
                                    // onClick={() => exportToFile('tsv')}
                                    onClick={() => {}}
                                />
                            </Dropdown.Menu>
                        </Dropdown>
                    </div>
                </div>

                <Grid columns="equal" stackable doubling>
                    <Grid.Column className="field-selector-label">
                        <FieldSelector
                            label="Start"
                            fieldName={BillingColumn.InvoiceMonth}
                            onClickFunction={onMonthStart}
                            selected={start}
                        />
                    </Grid.Column>

                    <Grid.Column className="field-selector-label">
                        <FieldSelector
                            label="Finish"
                            fieldName={BillingColumn.InvoiceMonth}
                            onClickFunction={onMonthEnd}
                            selected={end}
                        />
                    </Grid.Column>

                    <Grid.Column className="field-selector-label">
                        <FieldSelector
                            label="Filter GCP Projects"
                            fieldName={BillingColumn.GcpProject}
                            selected={selectedProjects}
                            multiple={false}
                            preloadedData={availableProjects}
                            onClickFunction={onProjectSelect}
                        />
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

/* eslint-enable @typescript-eslint/no-explicit-any   */
