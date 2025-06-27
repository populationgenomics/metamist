import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Button, Card, Checkbox, CheckboxProps, Dropdown, Grid, Message } from 'semantic-ui-react'
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
import FieldSelector from './components/FieldSelector'

enum CloudSpendCategory {
    STORAGE_COST = 'Storage Cost',
    COMPUTE_COST = 'Compute Cost',
}

/* eslint-disable @typescript-eslint/no-explicit-any  -- too many anys in the file to fix right now but would be good to sort out when we can */
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
    const [data, setData] = React.useState<any>([])

    // State for column visibility
    const [visibleColumns, setVisibleColumns] = React.useState<Set<string>>(new Set())
    const [isColumnsDropdownOpen, setColumnsDropdownOpen] = React.useState<boolean>(false)

    // Initialize visible columns when months data changes
    React.useEffect(() => {
        if (months.length > 0) {
            // Show all months initially, plus the fixed columns
            const allColumns = new Set(['topic', 'compute_type', ...months])
            setVisibleColumns(allColumns)
        }
    }, [months])

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    // Handle outside clicks and keyboard events for dropdown
    React.useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            const target = event.target as HTMLElement
            const columnsDropdown = document.querySelector('.columns-dropdown')

            if (isColumnsDropdownOpen && columnsDropdown && !columnsDropdown.contains(target)) {
                setColumnsDropdownOpen(false)
            }
        }

        const handleKeyDown = (event: KeyboardEvent) => {
            if (event.key === 'Escape' && isColumnsDropdownOpen) {
                setColumnsDropdownOpen(false)
            }
        }

        document.addEventListener('mousedown', handleClickOutside)
        document.addEventListener('keydown', handleKeyDown)

        return () => {
            document.removeEventListener('mousedown', handleClickOutside)
            document.removeEventListener('keydown', handleKeyDown)
        }
    }, [isColumnsDropdownOpen])

    // Add hover styles for dropdown items
    React.useEffect(() => {
        const style = document.createElement('style')
        style.textContent = `
            .column-checkbox-item:hover {
                background-color: #f1f3f4 !important;
            }

            .column-checkbox-item.selected:hover {
                background-color: #bbdefb !important;
            }

            .dropdown-menu-content {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
            }

            .dropdown-menu-content .ui.checkbox label {
                color: #495057 !important;
            }

            .dropdown-menu-content .ui.header {
                font-size: 0.85em !important;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
        `
        document.head.appendChild(style)

        return () => {
            document.head.removeChild(style)
        }
    }, [])

    // Helper functions for column visibility
    const toggleColumnVisibility = (category: string, event?: React.SyntheticEvent) => {
        if (event) {
            event.stopPropagation()
        }

        setVisibleColumns((prev) => {
            const newSet = new Set(prev)
            if (newSet.has(category)) {
                // Don't allow hiding required columns
                if (category !== 'topic' && category !== 'compute_type') {
                    newSet.delete(category)
                }
            } else {
                newSet.add(category)
            }
            return newSet
        })
    }

    // Toggle all columns in a group
    const toggleColumnGroup = (categoryGroup: string[], visible: boolean) => {
        setVisibleColumns((prev) => {
            const newSet = new Set(prev)
            categoryGroup.forEach((category) => {
                // Don't allow hiding required columns
                if (category !== 'topic' && category !== 'compute_type') {
                    if (visible) {
                        newSet.add(category)
                    } else {
                        newSet.delete(category)
                    }
                }
            })
            return newSet
        })
    }

    // Check if a column is visible
    const isColumnVisible = (category: string): boolean => {
        // Required columns are always visible
        if (category === 'topic' || category === 'compute_type') {
            return true
        }
        return visibleColumns.has(category)
    }

    // Reusable column checkbox component
    const ColumnCheckbox = ({ category, label }: { category: string; label: string }) => {
        const handleItemClick = (e: React.MouseEvent) => {
            e.stopPropagation()
            e.preventDefault()
            toggleColumnVisibility(category, e)
        }

        const handleChange = (e: React.FormEvent<HTMLInputElement>, _data: CheckboxProps) => {
            e.stopPropagation()
            toggleColumnVisibility(category, e)
        }

        const isVisible = isColumnVisible(category)
        const isDisabled = category === 'topic' || category === 'compute_type'

        return (
            <Dropdown.Item
                onClick={handleItemClick}
                onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                role="menuitemcheckbox"
                aria-checked={isVisible}
                disabled={isDisabled}
                style={{
                    padding: '8px 16px',
                    backgroundColor: isVisible ? '#e3f2fd' : '#ffffff',
                    borderLeft: isVisible ? '3px solid #2196f3' : '3px solid transparent',
                    transition: 'all 0.2s ease',
                    cursor: 'pointer',
                    minHeight: '36px',
                    display: 'flex',
                    alignItems: 'center',
                }}
                className={`column-checkbox-item ${isVisible ? 'selected' : ''}`}
            >
                <div style={{ display: 'flex', alignItems: 'center' }}>
                    <Checkbox
                        checked={isVisible}
                        onChange={handleChange}
                        onClick={(e: React.MouseEvent) => e.stopPropagation()}
                        disabled={isDisabled}
                        style={{ margin: 0 }}
                    />
                    <span
                        style={{
                            marginLeft: '8px',
                            color: '#495057',
                            fontWeight: isVisible ? '500' : '400',
                        }}
                    >
                        {label}
                    </span>
                </div>
            </Dropdown.Item>
        )
    }

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

    const getData = (query: BillingTotalCostQueryModel) => {
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

                response.data.forEach((item: BillingTotalCostRecord) => {
                    const { day, cost_category, topic, cost } = item
                    const ccat = convertCostCategory(cost_category)
                    const _topic = topic || ''
                    if (!day) return
                    if (recMonths.indexOf(day) === -1) {
                        recMonths.push(day)
                    }
                    if (!recTotals[_topic]) {
                        recTotals[_topic] = {}
                    }
                    if (!recTotals[_topic][day]) {
                        recTotals[_topic][day] = {}
                    }
                    if (!recTotals[_topic][day][ccat]) {
                        recTotals[_topic][day][ccat] = 0
                    }
                    // Ensure recTotals[_topic] is initialized
                    if (!recTotals[_topic]) {
                        recTotals[_topic] = {}
                    }
                    // Ensure recTotals[_topic][day] is initialized
                    if (!recTotals[_topic][day]) {
                        recTotals[_topic][day] = {}
                    }
                    // Ensure recTotals[_topic][day][ccat] is initialized and add cost
                    recTotals[_topic][day][ccat] = (recTotals[_topic][day][ccat] || 0) + cost
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
                        visibleColumns={visibleColumns}
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
            // valid selection, retrieve data
            getData({
                fields: [BillingColumn.Topic, BillingColumn.CostCategory],
                start_date: getAdjustedDay(convertInvoiceMonth(start, true), -2),
                end_date: getAdjustedDay(convertInvoiceMonth(end, false), 3),
                order_by: { day: false },
                source: BillingSource.Aggregate,
                time_periods: BillingTimePeriods.InvoiceMonth,
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
    /* eslint-enable react-hooks/exhaustive-deps */

    const exportToFile = (format: 'csv' | 'tsv') => {
        // Filter months based on visibility
        const visibleMonths = months.filter((month) => isColumnVisible(month))
        const headerFields = ['Topic', 'Cost Type', ...visibleMonths]

        const matrix: string[][] = []

        Object.keys(data)
            .sort((a, b) => a.localeCompare(b))
            .forEach((topic) => {
                // Storage cost row
                const storageRow: [string, string, ...string[]] = [
                    topic,
                    CloudSpendCategory.STORAGE_COST.toString(),
                    ...visibleMonths.map((m) => {
                        const val = data[topic]?.[m]?.[CloudSpendCategory.STORAGE_COST]
                        return val === undefined ? '' : val.toFixed(2)
                    }),
                ]
                matrix.push(storageRow)

                const computeRow: [string, string, ...string[]] = [
                    topic,
                    CloudSpendCategory.COMPUTE_COST.toString(),
                    ...visibleMonths.map((m) => {
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
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <h1
                        style={{
                            fontSize: 40,
                        }}
                    >
                        Cost Across Invoice Months (Topic only)
                    </h1>
                    <div style={{ textAlign: 'right' }}>
                        <Dropdown
                            button
                            className="icon columns-dropdown"
                            floating
                            labeled
                            icon="columns"
                            text="Columns"
                            style={{ marginRight: '10px' }}
                            open={isColumnsDropdownOpen}
                            onClick={(e) => {
                                // Prevent toggling if clicking on dropdown menu content
                                const target = e.target as HTMLElement
                                if (target && target.closest('.dropdown-menu-content')) {
                                    return
                                }
                                setColumnsDropdownOpen(!isColumnsDropdownOpen)
                            }}
                            closeOnBlur={false}
                            closeOnChange={false}
                            closeOnEscape={true}
                        >
                            <Dropdown.Menu
                                className="dropdown-menu-content"
                                style={{
                                    backgroundColor: '#f8f9fa',
                                    border: '1px solid #e9ecef',
                                    borderRadius: '4px',
                                    boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)',
                                    minWidth: '250px',
                                    maxHeight: '500px',
                                    overflowY: 'auto',
                                }}
                            >
                                <Dropdown.Header
                                    icon="table"
                                    content="Column Visibility"
                                    style={{
                                        backgroundColor: '#e9ecef',
                                        color: '#495057',
                                        fontWeight: 'bold',
                                        padding: '12px 16px',
                                        borderBottom: '1px solid #dee2e6',
                                        position: 'sticky',
                                        top: 0,
                                        zIndex: 10,
                                    }}
                                />

                                <Dropdown.Item
                                    onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                    onClick={(e: React.MouseEvent) => e.stopPropagation()}
                                    style={{
                                        padding: '12px 16px',
                                        backgroundColor: '#f8f9fa',
                                        borderBottom: '1px solid #dee2e6',
                                    }}
                                >
                                    <div
                                        style={{
                                            display: 'flex',
                                            justifyContent: 'space-between',
                                            gap: '8px',
                                        }}
                                    >
                                        <Button
                                            compact
                                            size="mini"
                                            color="blue"
                                            onMouseDown={(e: React.MouseEvent) =>
                                                e.stopPropagation()
                                            }
                                            onClick={(e: React.MouseEvent) => {
                                                e.stopPropagation()
                                                e.preventDefault()
                                                toggleColumnGroup(months, true)
                                            }}
                                        >
                                            Select All
                                        </Button>
                                        <Button
                                            compact
                                            size="mini"
                                            color="grey"
                                            onMouseDown={(e: React.MouseEvent) =>
                                                e.stopPropagation()
                                            }
                                            onClick={(e: React.MouseEvent) => {
                                                e.stopPropagation()
                                                e.preventDefault()
                                                toggleColumnGroup(months, false)
                                            }}
                                        >
                                            Hide All
                                        </Button>
                                    </div>
                                </Dropdown.Item>

                                {/* Required Columns - Always visible */}
                                <div style={{ borderBottom: '1px solid #dee2e6' }}>
                                    <Dropdown.Header
                                        content="Required Columns"
                                        style={{
                                            color: '#6c757d',
                                            fontSize: '0.85em',
                                            padding: '8px 16px',
                                            margin: 0,
                                            position: 'sticky',
                                            top: '61px',
                                            backgroundColor: '#f8f9fa',
                                            zIndex: 9,
                                        }}
                                    />
                                    <Dropdown.Item
                                        disabled
                                        style={{
                                            padding: '8px 16px',
                                            opacity: 0.7,
                                            backgroundColor: '#f8f9fa',
                                        }}
                                    >
                                        <Checkbox
                                            label={
                                                <span
                                                    style={{ color: '#495057', marginLeft: '8px' }}
                                                >
                                                    Topic
                                                </span>
                                            }
                                            checked={true}
                                            readOnly
                                            disabled
                                        />
                                    </Dropdown.Item>
                                    <Dropdown.Item
                                        disabled
                                        style={{
                                            padding: '8px 16px',
                                            opacity: 0.7,
                                            backgroundColor: '#f8f9fa',
                                        }}
                                    >
                                        <Checkbox
                                            label={
                                                <span
                                                    style={{ color: '#495057', marginLeft: '8px' }}
                                                >
                                                    Compute Type
                                                </span>
                                            }
                                            checked={true}
                                            readOnly
                                            disabled
                                        />
                                    </Dropdown.Item>
                                </div>

                                {/* Month Columns */}
                                <div>
                                    <Dropdown.Header
                                        content="Invoice Months"
                                        style={{
                                            color: '#6c757d',
                                            fontSize: '0.85em',
                                            padding: '8px 16px',
                                            margin: 0,
                                            position: 'sticky',
                                            top: '61px',
                                            backgroundColor: '#f8f9fa',
                                            zIndex: 9,
                                        }}
                                    />
                                    {months.map((month) => {
                                        const monthLabel = new Date(month).toLocaleDateString(
                                            'en-US',
                                            {
                                                year: 'numeric',
                                                month: 'short',
                                            }
                                        )
                                        return (
                                            <ColumnCheckbox
                                                key={month}
                                                category={month}
                                                label={monthLabel}
                                            />
                                        )
                                    })}
                                </div>
                            </Dropdown.Menu>
                        </Dropdown>

                        <Dropdown
                            button
                            className="icon"
                            floating
                            labeled
                            icon="download"
                            text="Export"
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
                </Grid>
            </Card>

            {messageComponent()}

            {dataComponent()}
        </PaddedPage>
    )
}

export default BillingCostByTime

/* eslint-enable @typescript-eslint/no-explicit-any  */
