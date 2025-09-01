import { SelectChangeEvent } from '@mui/material/Select'
import { debounce } from 'lodash'
import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Button, Card, Dropdown, Grid, Message } from 'semantic-ui-react'

import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { exportTable } from '../../shared/utilities/exportTable'
import {
    generateInvoiceMonths,
    getAdjustedDay,
    getCurrentInvoiceMonth,
    getCurrentInvoiceYearStart,
} from '../../shared/utilities/formatDates'
import generateUrl from '../../shared/utilities/generateUrl'
import {
    BillingApi,
    BillingColumn,
    BillingSource,
    BillingTimePeriods,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
} from '../../sm-api'
import BillingCostByMonthTable from './components/BillingCostByMonthTable'
import './components/BillingCostByTimeTable.css'
import FieldSelector from './components/FieldSelector'

enum CloudSpendCategory {
    STORAGE_COST = 'Storage Cost',
    COMPUTE_COST = 'Compute Cost',
}

/* eslint-disable @typescript-eslint/no-explicit-any  -- too many anys in the file to fix right now but would be good to sort out when we can */
const BillingCostByTime: React.FunctionComponent = () => {
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

    const convertCostCategory = (costCategory: string | null | undefined) => {
        if (costCategory?.startsWith('Cloud Storage')) {
            return CloudSpendCategory.STORAGE_COST
        }
        return CloudSpendCategory.COMPUTE_COST
    }

    // Pre-fetch topics data on component mount
    React.useEffect(() => {
        const preFetchTopics = async () => {
            try {
                const topicsResponse = await new BillingApi().getTopics()
                setAvailableTopics(topicsResponse.data || [])
            } catch (error) {
                console.error('Error pre-fetching topics:', error)
                setAvailableTopics([])
            }
        }

        preFetchTopics()
    }, [])

    const getData = React.useCallback((query: BillingTotalCostQueryModel) => {
        setIsLoading(true)
        setError(undefined)
        setMessage(undefined)
        new BillingApi()
            .getTotalCost(query)
            .then((response) => {
                setIsLoading(false)

                // calc totals per topic, month and category
                interface RecTotals {
                    [topic: string]: {
                        [day: string]: {
                            [category in CloudSpendCategory]?: number
                        }
                    }
                }
                const recTotals: RecTotals = {}
                const recMonths: string[] = []
                const topicOrder: string[] = []

                // Process only regular rows (no All Topics from backend)
                const filteredRows = response.data.filter(
                    (item: BillingTotalCostRecord) => !item.topic?.startsWith('All ')
                )

                filteredRows.forEach((item: BillingTotalCostRecord) => {
                    const { day, cost_category, topic, cost } = item
                    const ccat = convertCostCategory(cost_category)
                    const _topic = topic || ''
                    if (!day) return
                    if (recMonths.indexOf(day) === -1) {
                        recMonths.push(day)
                    }
                    if (!recTotals[_topic]) {
                        recTotals[_topic] = {}
                        // Track topic order as we first encounter them
                        if (topicOrder.indexOf(_topic) === -1) {
                            topicOrder.push(_topic)
                        }
                    }
                    if (!recTotals[_topic][day]) {
                        recTotals[_topic][day] = {}
                    }
                    if (!recTotals[_topic][day][ccat]) {
                        recTotals[_topic][day][ccat] = 0
                    }

                    recTotals[_topic][day][ccat] = (recTotals[_topic][day][ccat] || 0) + cost
                })

                // Create All Topics rows by aggregating across topics for each day/category
                const allTopicsKey = 'All Topics'
                if (!recTotals[allTopicsKey]) {
                    recTotals[allTopicsKey] = {}
                    topicOrder.unshift(allTopicsKey) // Add to beginning
                }

                recMonths.forEach((month) => {
                    if (!recTotals[allTopicsKey][month]) {
                        recTotals[allTopicsKey][month] = {}
                    }

                    Object.values(CloudSpendCategory).forEach((category) => {
                        // Sum costs across all regular topics for this month/category
                        let totalCost = 0
                        Object.keys(recTotals).forEach((topic) => {
                            if (topic !== allTopicsKey && recTotals[topic][month]?.[category]) {
                                totalCost += recTotals[topic][month][category]
                            }
                        })

                        if (totalCost > 0) {
                            recTotals[allTopicsKey][month][category] = totalCost
                        }
                    })
                })

                // Sort topics with "All Topics" at the top
                topicOrder.sort((a, b) => {
                    // Move "All Topics" to the top
                    if (a.startsWith('All ')) return -1
                    if (b.startsWith('All ')) return 1
                    return a.localeCompare(b)
                })

                setMonths(recMonths)
                // Store both the data and the topic order as a property
                const dataWithOrder = Object.assign(recTotals, { _topicOrder: topicOrder })
                setData(dataWithOrder)
            })
            .catch((er) => setError(er.message))
    }, [])

    const onTopicsSelect = (
        event: SelectChangeEvent<string | string[]> | undefined,
        data: { value: string | string[] }
    ) => {
        const value = Array.isArray(data.value) ? data.value : [data.value]
        setSelectedTopics(value)
        updateNav(start, end, value)
    }

    // Create a debounced version of getData for topic selections
    const debouncedGetData = React.useMemo(
        () =>
            debounce((start: string, end: string, topics: string[]) => {
                const queryFilters: any = {
                    invoice_month: generateInvoiceMonths(start, end),
                }

                // Add topic filtering if topics are selected
                if (topics.length > 0) {
                    queryFilters.topic = topics
                }

                getData({
                    fields: [BillingColumn.Topic, BillingColumn.CostCategory],
                    start_date: getAdjustedDay(convertInvoiceMonth(start, true), -2),
                    end_date: getAdjustedDay(convertInvoiceMonth(end, false), 3),
                    order_by: { day: false, topic: false },
                    source: BillingSource.Aggregate,
                    time_periods: BillingTimePeriods.InvoiceMonth,
                    filters: queryFilters,
                })
            }, 1000),
        [getData]
    )

    // Cleanup debounced function on unmount
    React.useEffect(() => {
        return () => {
            debouncedGetData.cancel()
        }
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
                    id="billing-container-data"
                >
                    <BillingCostByMonthTable
                        start={start}
                        end={end}
                        isLoading={isLoading}
                        data={data}
                        months={months}
                        orderedTopics={getOrderedTopics()}
                    />
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
            // Use debounced function for topic filtering
            debouncedGetData(start, end, selectedTopics)
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
    }, [start, end, selectedTopics, debouncedGetData])
    /* eslint-enable react-hooks/exhaustive-deps */

    const exportToFile = (format: 'csv' | 'tsv') => {
        // Export all topics
        const allTopics = getOrderedTopics()
        const headerFields = ['Topic', 'Cost Type', ...months]

        const matrix: string[][] = []

        allTopics.forEach((topic: string) => {
            // Storage cost row
            const storageRow: [string, string, ...string[]] = [
                topic,
                CloudSpendCategory.STORAGE_COST.toString(),
                ...months.map((m) => {
                    const val = data[topic]?.[m]?.[CloudSpendCategory.STORAGE_COST]
                    return val === undefined ? '' : val.toFixed(2)
                }),
            ]
            matrix.push(storageRow)

            const computeRow: [string, string, ...string[]] = [
                topic,
                CloudSpendCategory.COMPUTE_COST.toString(),
                ...months.map((m) => {
                    const val = data[topic]?.[m]?.[CloudSpendCategory.COMPUTE_COST]
                    return val === undefined ? '' : val.toFixed(2)
                }),
            ]
            matrix.push(computeRow)
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
        <PaddedPage>
            <Card fluid style={{ padding: '20px' }} id="billing-container">
                <div
                    style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'flex-start',
                        flexWrap: 'wrap',
                        gap: '10px',
                        marginBottom: '20px',
                    }}
                >
                    <h1
                        style={{
                            fontSize: 40,
                            margin: 0,
                            flex: '1 1 200px',
                        }}
                    >
                        Cost Across Invoice Months (Topic only)
                    </h1>
                    <Dropdown
                        button
                        className="icon"
                        floating
                        labeled
                        icon="download"
                        text="Export"
                        style={{
                            minWidth: '115px',
                            maxWidth: '115px',
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

            {messageComponent()}

            {dataComponent()}
        </PaddedPage>
    )
}

export default BillingCostByTime

/* eslint-enable @typescript-eslint/no-explicit-any  */
