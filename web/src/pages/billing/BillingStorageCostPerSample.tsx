import { SelectChangeEvent } from '@mui/material/Select'
import { debounce } from 'lodash'
import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { Button, Card, Checkbox, Dropdown, Grid, Input, Message } from 'semantic-ui-react'
import {
    generateInvoiceMonths,
    getAdjustedDay,
    getCurrentInvoiceMonth,
    getCurrentInvoiceYearStart,
} from '../../shared/utilities/formatDates'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import { exportTable } from '../../shared/utilities/exportTable'
import generateUrl from '../../shared/utilities/generateUrl'
import {
    BillingApi,
    BillingColumn,
    BillingSource,
    BillingTimePeriods,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
} from '../../sm-api'
import './components/BillingCostByTimeTable.css'
import FieldSelector from './components/FieldSelector'

/* eslint-disable @typescript-eslint/no-explicit-any  -- too many anys in the file to fix right now but would be good to sort out when we can */
const BillingStorageCostPerSample: React.FunctionComponent = () => {
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
    const [selectedTopics, setSelectedTopics] = React.useState<string[]>(initialTopics)
    const [availableTopics, setAvailableTopics] = React.useState<string[]>([])

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    // Data loading
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [message, setMessage] = React.useState<string | undefined>()
    const [months, setMonths] = React.useState<string[]>([])
    const [data, setData] = React.useState<any>([])

    // Helper function to get topics in the order they appear in the data
    const getOrderedTopics = React.useCallback(() => {
        if (data && data._topicOrder) {
            return data._topicOrder
        }
        return Object.keys(data)
    }, [data])

    const updateNav = (st: string, ed: string, topics?: string[]) => {
        const url = generateUrl(location, {
            start: st,
            end: ed,
            topics: topics && topics.length > 0 ? topics.join(',') : undefined,
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
        updateNav(start_update, end_update, selectedTopics)
    }

    const convertInvoiceMonth = (invoiceMonth: string, start: boolean) => {
        const year = invoiceMonth.substring(0, 4)
        const month = invoiceMonth.substring(4, 6)
        if (start) return `${year}-${month}-01`
        // get last day of month
        const lastDay = new Date(parseInt(year), parseInt(month), 0).getDate()
        return `${year}-${month}-${lastDay}`
    }

    // Pre-fetch topics data on component mount
    React.useEffect(() => {
        // Pre-fetch topics in parallel (consistent with other files pattern)
        Promise.all([new BillingApi().getTopics()])
            .then(([topicsResponse]) => {
                setAvailableTopics(topicsResponse.data || [])
            })
            .catch((error) => {
                console.error('Error pre-fetching data:', error)
                setAvailableTopics([])
            })
            .finally(() => {})
    }, [])

    // Callback to get data for plotting
    // const getData = React.useCallback((query: BillingTotalCostQueryModel) => {...}

    const onTopicsSelect = (
        event: SelectChangeEvent<string | string[]> | undefined,
        data: { value: string | string[] }
    ) => {
        const value = Array.isArray(data.value) ? data.value : [data.value]
        setSelectedTopics(value)
        updateNav(start, end, value)
    }

    // Create a debounced version of getData for topic selections
    // const debouncedGetData = React.useMemo(
    //     () => debounce((start: string, end: string, topics: string[]) => {...}, 1000),
    //     [getData]
    // )

    // Cleanup debounced function on unmount
    // React.useEffect(() => {
    //     return () => {
    //         debouncedGetData.cancel()
    //     }
    // }, [debouncedGetData])

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
            // debouncedGetData(start, end, selectedTopics)
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
    }, [start, end, selectedTopics]) //, debouncedGetData])
    /* eslint-enable react-hooks/exhaustive-deps */

    const exportToFile = (format: 'csv' | 'tsv') => {
        // Export all topics
        const allTopics = getOrderedTopics()
        const headerFields = ['Topic', 'Cost Type', ...months]

        const matrix: string[][] = []

        allTopics.forEach((topic: string) => {
        })

        exportTable(
            {
                headerFields,
                matrix,
            },
            format,
            'billing_cost_by_month'
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
                        Storage Cost Per Sample
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
                                    onClick={() => exportToFile('csv')}
                                />
                                <Dropdown.Item
                                    key="tsv"
                                    text="Export to TSV"
                                    icon="file text outline"
                                    onClick={() => exportToFile('tsv')}
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
                            label="Filter Topics"
                            fieldName={BillingColumn.Topic}
                            selected={selectedTopics}
                            multiple={true}
                            preloadedData={availableTopics}
                            onClickFunction={onTopicsSelect}
                        />
                    </Grid.Column>
                </Grid>
            </Card>
        </>
    )
}

export default function BillingStorageCostPerSamplePage() {
    return (
        <PaddedPage>
            <BillingStorageCostPerSample />
        </PaddedPage>
    )
}

/* eslint-enable @typescript-eslint/no-explicit-any   */
