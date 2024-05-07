import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Button, Card, Grid, Input, Message, Table as SUITable } from 'semantic-ui-react'
import {
    BillingApi,
    BillingColumn,
    BillingSource,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
} from '../../sm-api'

import {
    getAdjustedDay,
    generateInvoiceMonths,
    getCurrentInvoiceMonth,
    getCurrentInvoiceYearStart,
} from '../../shared/utilities/formatDates'
import { IStackedAreaByDateChartData } from '../../shared/components/Graphs/StackedAreaByDateChart'
import BillingCostByMonthTable from './components/BillingCostByMonthTable'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import generateUrl from '../../shared/utilities/generateUrl'
import FieldSelector from './components/FieldSelector'

const BillingCostByTime: React.FunctionComponent = () => {
    const [searchParams] = useSearchParams()

    const [start, setStart] = React.useState<string>(
        searchParams.get('start') ?? getCurrentInvoiceYearStart()
    )
    const [end, setEnd] = React.useState<string>(
        searchParams.get('end') ?? getCurrentInvoiceMonth()
    )

    // Data loading
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [message, setMessage] = React.useState<string | undefined>()
    const [months, setMonths] = React.useState<string[]>([])
    const [data, setData] = React.useState<IStackedAreaByDateChartData[]>([])

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (st: string, ed: string) => {
        const url = generateUrl(location, {
            start: st,
            end: ed,
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
        updateNav(start_update, end_update)
    }

    const convertInvoiceMonth = (invoiceMonth: string, start: Boolean) => {
        const year = invoiceMonth.substring(0, 4)
        const month = invoiceMonth.substring(4, 6)
        if (start) return `${year}-${month}-01`
        // get last day of month
        const lastDay = new Date(parseInt(year), parseInt(month), 0).getDate()
        return `${year}-${month}-${lastDay}`
    }

    const convertCostCategory = (costCategory: string) => {
        if (costCategory.startsWith('Cloud Storage')) {
            return 'Storage Cost'
        }
        return 'Compute Cost'
    }

    const getData = (query: BillingTotalCostQueryModel) => {
        setIsLoading(true)
        setError(undefined)
        setMessage(undefined)
        new BillingApi()
            .getTotalCost(query)
            .then((response) => {
                setIsLoading(false)

                // calc totals per topic, month and category
                const recTotals: { [key: string]: { [key: string]: number } } = {}
                const recMonths: string[] = []

                response.data.forEach((item: BillingTotalCostRecord) => {
                    const { day, cost_category, topic, cost } = item
                    const ccat = convertCostCategory(cost_category)
                    if (recMonths.indexOf(day) === -1) {
                        recMonths.push(day)
                    }
                    if (!recTotals[topic]) {
                        recTotals[topic] = {}
                    }
                    if (!recTotals[topic][day]) {
                        recTotals[topic][day] = {}
                    }
                    if (!recTotals[topic][day][ccat]) {
                        recTotals[topic][day][ccat] = 0
                    }
                    recTotals[topic][day][ccat] += cost
                })

                setMonths(recMonths)
                setData(recTotals)
            })
            .catch((er) => setError(er.message))
    }

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
                    <Button negative onClick={() => setStart(start)}>
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

    React.useEffect(() => {
        if (Boolean(start) && Boolean(end)) {
            // valid selection, retrieve data
            getData({
                fields: [BillingColumn.Topic, BillingColumn.CostCategory],
                start_date: getAdjustedDay(convertInvoiceMonth(start, true), -2),
                end_date: getAdjustedDay(convertInvoiceMonth(end, false), 3),
                order_by: { day: false },
                source: BillingSource.Aggregate,
                time_periods: 'invoice_month',
                filters: {
                    invoice_month: generateInvoiceMonths(start, end),
                },
            })
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
    }, [start, end])

    return (
        <>
            <Card fluid style={{ padding: '20px' }} id="billing-container">
                <h1
                    style={{
                        fontSize: 40,
                    }}
                >
                    Cost Across Invoice Months (Topic only)
                </h1>

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
                </Grid>
            </Card>

            {messageComponent()}

            {dataComponent()}
        </>
    )
}

export default BillingCostByTime
