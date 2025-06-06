import orderBy from 'lodash/orderBy'
import * as React from 'react'
import { Link, useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import {
    Button,
    Checkbox,
    CheckboxProps,
    Dropdown,
    DropdownProps,
    Grid,
    Message,
    Table as SUITable,
} from 'semantic-ui-react'
import { HorizontalStackedBarChart } from '../../shared/components/Graphs/HorizontalStackedBarChart'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import Table from '../../shared/components/Table'
import { exportTable } from '../../shared/utilities/exportTable'
import { convertFieldName } from '../../shared/utilities/fieldName'
import formatMoney from '../../shared/utilities/formatMoney'
import generateUrl from '../../shared/utilities/generateUrl'
import { BillingApi, BillingColumn, BillingCostBudgetRecord } from '../../sm-api'
import FieldSelector from './components/FieldSelector'

const BillingCurrentCost = () => {
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [openRows, setOpenRows] = React.useState<string[]>([])

    const [costRecords, setCosts] = React.useState<BillingCostBudgetRecord[]>([])
    const [error, setError] = React.useState<string | undefined>()
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: 'undefined',
        direction: 'undefined',
    })

    const [showAsChart, setShowAsChart] = React.useState<boolean>(true)

    // State for column visibility
    const [visibleColumns, setVisibleColumns] = React.useState<Set<string>>(new Set([
        'field', 'compute_daily', 'storage_daily', 'total_daily',
        'compute_monthly', 'storage_monthly', 'total_monthly', 'budget_spent'
    ]))

    // State to control dropdown menu open/close
    const [isColumnsDropdownOpen, setColumnsDropdownOpen] = React.useState<boolean>(false)

    // Pull search params for use in the component
    const [searchParams] = useSearchParams()
    const inputGroupBy: string | null = searchParams.get('groupBy')
    const fixedGroupBy: BillingColumn = inputGroupBy
        ? (inputGroupBy as BillingColumn)
        : BillingColumn.GcpProject
    const inputInvoiceMonth = searchParams.get('invoiceMonth')

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (grp: BillingColumn, invoiceMonth: string | undefined) => {
        const url = generateUrl(location, {
            groupBy: grp,
            invoiceMonth: invoiceMonth,
        })
        navigate(url)
    }

    // toISOString() will give you YYYY-MM-DDTHH:mm:ss.sssZ
    // toISOString().substring(0, 7) will give you YYYY-MM
    // .replace('-', '') will give you YYYYMM
    const currentDate = new Date()
    const dayOfMonth = new Date().toISOString().substring(8, 10)
    if (parseInt(dayOfMonth, 10) < 3) {
        // first 2 days of the month, show previous month as default
        currentDate.setMonth(currentDate.getMonth() - 1)
    }
    const thisMonth = currentDate.toISOString().substring(0, 7).replace('-', '')

    const [groupBy, setGroupBy] = React.useState<BillingColumn>(
        fixedGroupBy ?? BillingColumn.GcpProject
    )
    const [invoiceMonth, setInvoiceMonth] = React.useState<string>(inputInvoiceMonth ?? thisMonth)

    const [lastLoadedDay, setLastLoadedDay] = React.useState<string>()

    const getCosts = (grp: BillingColumn, invoiceMth: string | undefined) => {
        updateNav(grp, invoiceMth)
        setIsLoading(true)
        setError(undefined)
        let source = 'aggregate'
        if (grp === BillingColumn.GcpProject) {
            source = 'gcp_billing'
        }
        new BillingApi()
            // @ts-ignore
            .getRunningCost(grp, invoiceMth, source)
            .then((response) => {
                setIsLoading(false)
                if (response.data.length > 0) {
                    setCosts(response.data)
                    setLastLoadedDay(response.data[0].last_loaded_day || '')
                }
            })
            .catch((er) => setError(er.message))
    }

    const onGroupBySelect = (event: unknown, data: DropdownProps) => {
        const value = data.value
        if (typeof value == 'string') {
            setGroupBy(value as BillingColumn)
            getCosts(value as BillingColumn, invoiceMonth)
        }
    }

    const onInvoiceMonthSelect = (event: unknown, data: DropdownProps) => {
        const value = data.value
        if (typeof value == 'string') {
            setInvoiceMonth(value)
            getCosts(groupBy, value)
        }
    }

    /* eslint-disable react-hooks/exhaustive-deps */
    React.useEffect(() => {
        getCosts(groupBy, invoiceMonth)
    }, [])

    // Handle outside clicks and keyboard events for dropdown
    React.useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            const target = event.target as HTMLElement
            const columnsDropdown = document.querySelector('.columns-dropdown')

            // If clicking outside the dropdown and dropdown is open, close it
            if (isColumnsDropdownOpen && columnsDropdown && !columnsDropdown.contains(target)) {
                setColumnsDropdownOpen(false)
            }
        }

        const handleKeyDown = (event: KeyboardEvent) => {
            // Close dropdown on ESC key press
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
    /* eslint-enable react-hooks/exhaustive-deps */

    const HEADER_FIELDS = [
        { category: 'field', title: groupBy.toUpperCase(), show_always: true },
        { category: 'compute_daily', title: 'C', show_always: false },
        { category: 'storage_daily', title: 'S', show_always: false },
        { category: 'total_daily', title: 'Total', show_always: false },
        { category: 'compute_monthly', title: 'C', show_always: true },
        { category: 'storage_monthly', title: 'S', show_always: true },
        { category: 'total_monthly', title: 'Total', show_always: true },
    ]

    // Define column groups for easier management
    const DAILY_COLUMNS = ['compute_daily', 'storage_daily', 'total_daily']
    const MONTHLY_COLUMNS = ['compute_monthly', 'storage_monthly', 'total_monthly']
    const BUDGET_COLUMNS = ['budget_spent']
    const ALL_COLUMNS = [...DAILY_COLUMNS, ...MONTHLY_COLUMNS, ...BUDGET_COLUMNS]

    // Helper functions for column visibility
    const toggleColumnVisibility = (category: string, event?: React.SyntheticEvent) => {
        // Stop propagation to prevent dropdown from closing
        if (event) {
            event.stopPropagation()
        }

        setVisibleColumns(prev => {
            const newSet = new Set(prev)
            if (newSet.has(category)) {
                // Don't allow hiding 'field' column
                if (category !== 'field') {
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
        setVisibleColumns(prev => {
            const newSet = new Set(prev)
            categoryGroup.forEach(category => {
                if (category !== 'field') { // Don't allow hiding 'field' column
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

    // Reusable column checkbox component
    const ColumnCheckbox = ({ category, label }: { category: string, label: string }) => {
        const handleItemClick = (e: React.MouseEvent) => {
            e.stopPropagation()
            e.preventDefault()
            toggleColumnVisibility(category, e)
        }

        // Use the correct type for Semantic UI's onChange handler
        const handleChange = (e: React.FormEvent<HTMLInputElement>, _data: CheckboxProps) => {
            e.stopPropagation()
            toggleColumnVisibility(category, e)
        }

        const isVisible = isColumnVisible(category)

        return (
            <Dropdown.Item
                onClick={handleItemClick}
                onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                role="menuitemcheckbox"
                aria-checked={isVisible}
            >
                <Checkbox
                    label={label}
                    checked={isVisible}
                    onChange={handleChange}
                    onClick={(e: React.MouseEvent) => e.stopPropagation()}
                />
            </Dropdown.Item>
        )
    }

    // Check if a column is visible
    const isColumnVisible = (category: string): boolean => {
        // Field column is always visible
        if (category === 'field') {
            return true
        }
        return visibleColumns.has(category)
    }

    const handleToggle = (field: string) => {
        if (!openRows.includes(field)) {
            setOpenRows([...openRows, field])
        } else {
            setOpenRows(openRows.filter((i) => i !== field))
        }
    }

    function percFormat(num: number | undefined | null): string {
        if (num === undefined || num === null) {
            return ''
        }

        return `${num.toFixed(0).toString()} % `
    }

    if (error)
        return (
            <Message negative>
                {error}
                <br />
                <Button negative onClick={() => getCosts(groupBy, invoiceMonth)}>
                    Retry
                </Button>
            </Message>
        )

    const rowColor = (p: BillingCostBudgetRecord) => {
        if (p.budget_spent === undefined || p.budget_spent === null) {
            return ''
        }
        if (p.budget_spent > 90) {
            return 'billing-over-budget'
        }
        if (p.budget_spent > 50) {
            return 'billing-half-budget'
        }
        return 'billing-under-budget'
    }

    const handleSort = (clickedColumn: string) => {
        if (sort.column !== clickedColumn) {
            setSort({ column: clickedColumn, direction: 'ascending' })
            return
        }
        if (sort.direction === 'ascending') {
            setSort({ column: clickedColumn, direction: 'descending' })
            return
        }
        setSort({ column: null, direction: null })
    }

    const checkDirection = (category: string) => {
        if (sort.column === category && sort.direction !== null) {
            return sort.direction === 'ascending' ? 'ascending' : 'descending'
        }
        return undefined
    }

    const linkTo = (data: string) => {
        // convert invoice month to start and end dates
        const year = invoiceMonth.substring(0, 4)
        const month = invoiceMonth.substring(4, 6)
        let nextYear = year
        let nextMonth = (parseInt(month, 10) + 1).toString().padStart(2, '0')
        if (month === '12') {
            nextYear = (parseInt(year, 10) + 1).toString()
            nextMonth = '01'
        }
        const startDate = `${year}-${month}-01`
        const nextMth = new Date(`${nextYear}-${nextMonth}-01`)
        nextMth.setDate(-0.01)
        const endDate = nextMth.toISOString().substring(0, 10)
        return `/billing/costByTime?groupBy=${groupBy}&selectedData=${data}&start=${startDate}&end=${endDate}`
    }

    const exportToFile = (format: 'csv' | 'tsv') => {
        // Filter based on user-selected visible columns
        const visibleFields = HEADER_FIELDS.filter(
            (k) => isColumnVisible(k.category)
        )
        const headerFields: string[] = visibleFields.map((k) => convertFieldName(k.category))

        // Add Budget % spend if it should be shown and is selected
        const budgetSpendVisible =
            groupBy === BillingColumn.GcpProject &&
            invoiceMonth === thisMonth &&
            isColumnVisible('budget_spent')

        if (budgetSpendVisible) headerFields.push('Budget Spend %')

        // Prepare 2D matrix of data strings
        const matrix = costRecords.map((rec) => {
            const row = visibleFields.map((k) =>
                k.category === 'field'
                    ? String(rec[k.category] ?? '')
                    : formatMoney(
                          (rec as unknown as Record<string, number | undefined>)[k.category] ?? 0
                      )
            )
            if (budgetSpendVisible) {
                row.push(percFormat(rec.budget_spent))
            }
            return row
        })

        exportTable({ headerFields, matrix }, format, 'billing-cost-by-invoice-month')
    }

    return (
        <>
            <h1>Cost By Invoice Month</h1>

            <Grid columns="equal" stackable doubling>
                <Grid.Column>
                    <FieldSelector
                        label="Group By"
                        fieldName="Group"
                        onClickFunction={onGroupBySelect}
                        selected={groupBy}
                    />
                </Grid.Column>
                <Grid.Column>
                    <FieldSelector
                        label="Invoice Month"
                        fieldName={BillingColumn.InvoiceMonth}
                        onClickFunction={onInvoiceMonthSelect}
                        selected={invoiceMonth}
                    />
                </Grid.Column>

                <Grid.Column>
                    <Checkbox
                        label="Show as Chart / Table"
                        fitted
                        toggle
                        checked={showAsChart}
                        onChange={() => setShowAsChart(!showAsChart)}
                    />
                </Grid.Column>
                <Grid.Column textAlign="right">
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
                            // Only toggle if the dropdown button itself is clicked
                            if (e.target && (e.target as HTMLElement).closest('.ui.dropdown > .text, .ui.dropdown > .icon')) {
                                setColumnsDropdownOpen(!isColumnsDropdownOpen)
                            }
                        }}
                        // Don't use onClose or onOpen, as we want to manually control it
                        closeOnBlur={false}
                        closeOnChange={false}
                        closeOnEscape={true}
                    >
                        <Dropdown.Menu className="dropdown-menu-content">
                            <Dropdown.Header icon='table' content='Column Visibility' />
                            <Dropdown.Item
                                onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                onClick={(e: React.MouseEvent) => e.stopPropagation()}
                            >
                                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                                    <Button
                                        compact
                                        size="mini"
                                        onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                        onClick={(e: React.MouseEvent) => {
                                            e.stopPropagation();
                                            e.preventDefault();
                                            toggleColumnGroup(ALL_COLUMNS, true);
                                        }}
                                    >
                                        Select All
                                    </Button>
                                    <Button
                                        compact
                                        size="mini"
                                        onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                        onClick={(e: React.MouseEvent) => {
                                            e.stopPropagation();
                                            e.preventDefault();
                                            toggleColumnGroup(ALL_COLUMNS, false);
                                        }}
                                    >
                                        Hide All
                                    </Button>
                                </div>
                            </Dropdown.Item>
                            <Dropdown.Divider />

                            {/* ID Column - Always visible */}
                            <Dropdown.Item disabled>
                                <Checkbox
                                    label={convertFieldName(groupBy.toUpperCase())}
                                    checked={true}
                                    readOnly
                                />
                            </Dropdown.Item>

                            {/* Daily Columns */}
                            {invoiceMonth === thisMonth && (
                                <>
                                    <Dropdown.Header content='Daily Costs' />
                                    <Dropdown.Item
                                        onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                        onClick={(e: React.MouseEvent) => e.stopPropagation()}
                                    >
                                        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                                            <Button
                                                compact
                                                size="mini"
                                                onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                                onClick={(e: React.MouseEvent) => {
                                                    e.stopPropagation();
                                                    e.preventDefault();
                                                    toggleColumnGroup(DAILY_COLUMNS, true);
                                                }}
                                            >
                                                All
                                            </Button>
                                            <Button
                                                compact
                                                size="mini"
                                                onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                                onClick={(e: React.MouseEvent) => {
                                                    e.stopPropagation();
                                                    e.preventDefault();
                                                    toggleColumnGroup(DAILY_COLUMNS, false);
                                                }}
                                            >
                                                None
                                            </Button>
                                        </div>
                                    </Dropdown.Item>
                                    <ColumnCheckbox category="compute_daily" label="Compute (Daily)" />
                                    <ColumnCheckbox category="storage_daily" label="Storage (Daily)" />
                                    <ColumnCheckbox category="total_daily" label="Total (Daily)" />
                                </>
                            )}

                            {/* Monthly Columns */}
                            <Dropdown.Header content='Monthly Costs' />
                            <Dropdown.Item
                                onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                onClick={(e: React.MouseEvent) => e.stopPropagation()}
                            >
                                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                                    <Button
                                        compact
                                        size="mini"
                                        onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                        onClick={(e: React.MouseEvent) => {
                                            e.stopPropagation();
                                            e.preventDefault();
                                            toggleColumnGroup(MONTHLY_COLUMNS, true);
                                        }}
                                    >
                                        All
                                    </Button>
                                    <Button
                                        compact
                                        size="mini"
                                        onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
                                        onClick={(e: React.MouseEvent) => {
                                            e.stopPropagation();
                                            e.preventDefault();
                                            toggleColumnGroup(MONTHLY_COLUMNS, false);
                                        }}
                                    >
                                        None
                                    </Button>
                                </div>
                            </Dropdown.Item>
                            <ColumnCheckbox category="compute_monthly" label="Compute (Monthly)" />
                            <ColumnCheckbox category="storage_monthly" label="Storage (Monthly)" />
                            <ColumnCheckbox category="total_monthly" label="Total (Monthly)" />

                            {/* Budget Column */}
                            {groupBy === BillingColumn.GcpProject && invoiceMonth === thisMonth && (
                                <>
                                    <Dropdown.Header content='Budget' />
                                    <ColumnCheckbox category="budget_spent" label="Budget Spend %" />
                                </>
                            )}
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
                </Grid.Column>
            </Grid>

            {(() => {
                if (!showAsChart) return null
                if (String(invoiceMonth) === String(thisMonth)) {
                    return (
                        <Grid columns={2} stackable doubling>
                            <Grid.Column width={8} className="chart-card">
                                <HorizontalStackedBarChart
                                    data={costRecords}
                                    title={`24H (day UTC ${lastLoadedDay})`}
                                    series={['compute_daily', 'storage_daily']}
                                    labels={['Compute', 'Storage']}
                                    total_series="total_daily"
                                    threshold_values={[90, 50]}
                                    threshold_series="budget_spent"
                                    sorted_by="total_monthly"
                                    isLoading={isLoading}
                                    showLegend={false}
                                />
                            </Grid.Column>
                            <Grid.Column width={8} className="chart-card donut-chart">
                                <HorizontalStackedBarChart
                                    data={costRecords}
                                    title="Invoice Month (Acc)"
                                    series={['compute_monthly', 'storage_monthly']}
                                    labels={['Compute', 'Storage']}
                                    total_series="total_monthly"
                                    threshold_values={[90, 50]}
                                    threshold_series="budget_spent"
                                    sorted_by="total_monthly"
                                    isLoading={isLoading}
                                    showLegend={true}
                                />
                            </Grid.Column>
                        </Grid>
                    )
                }
                return (
                    <Grid>
                        <Grid.Column width={12}>
                            <HorizontalStackedBarChart
                                data={costRecords}
                                title="Invoice Month (Acc)"
                                series={['compute_monthly', 'storage_monthly']}
                                labels={['Compute', 'Storage']}
                                total_series="total_monthly"
                                sorted_by="total_monthly"
                                isLoading={isLoading}
                                showLegend={true}
                                // @ts-ignore
                                threshold_values={undefined}
                            />
                        </Grid.Column>
                    </Grid>
                )
            })()}

            {!showAsChart ? (
                <Table celled compact sortable>
                    <SUITable.Header>
                        <SUITable.Row>
                            <SUITable.HeaderCell></SUITable.HeaderCell>

                            <SUITable.HeaderCell></SUITable.HeaderCell>

                            {(() => {
                                // Calculate how many daily columns are visible
                                if (invoiceMonth === thisMonth) {
                                    const visibleCount = ['compute_daily', 'storage_daily', 'total_daily'].filter(
                                        col => isColumnVisible(col)
                                    ).length;

                                    return visibleCount > 0 ? (
                                        <SUITable.HeaderCell colSpan={visibleCount}>
                                            24H (day UTC {lastLoadedDay})
                                        </SUITable.HeaderCell>
                                    ) : null;
                                }
                                return null;
                            })()}

                            {(() => {
                                // Calculate how many monthly columns are visible
                                const baseColumns = ['compute_monthly', 'storage_monthly', 'total_monthly'];
                                const budgetColumn = (groupBy === BillingColumn.GcpProject &&
                                                     invoiceMonth === thisMonth &&
                                                     isColumnVisible('budget_spent')) ? 1 : 0;

                                const visibleCount = baseColumns.filter(
                                    col => isColumnVisible(col)
                                ).length + budgetColumn;

                                return visibleCount > 0 ? (
                                    <SUITable.HeaderCell colSpan={visibleCount}>
                                        Invoice Month (Acc)
                                    </SUITable.HeaderCell>
                                ) : null;
                            })()}
                        </SUITable.Row>
                        <SUITable.Row>
                            <SUITable.HeaderCell></SUITable.HeaderCell>

                            {HEADER_FIELDS.map((k) => {
                                // Only show columns that are visible
                                if (isColumnVisible(k.category)) {
                                    return (
                                        <SUITable.HeaderCell
                                            key={k.category}
                                            sorted={checkDirection(k.category)}
                                            onClick={() => handleSort(k.category)}
                                            style={{
                                                borderBottom: 'none',
                                                position: 'sticky',
                                                resize: 'horizontal',
                                            }}
                                        >
                                            {convertFieldName(k.title)}
                                        </SUITable.HeaderCell>
                                    )
                                }
                                return null
                            })}

                            {groupBy === BillingColumn.GcpProject &&
                              invoiceMonth === thisMonth &&
                              isColumnVisible('budget_spent') ? (
                                <SUITable.HeaderCell
                                    key={'budget_spent'}
                                    sorted={checkDirection('budget_spent')}
                                    onClick={() => handleSort('budget_spent')}
                                    style={{
                                        borderBottom: 'none',
                                        position: 'sticky',
                                        resize: 'horizontal',
                                    }}
                                >
                                    Budget Spend %
                                </SUITable.HeaderCell>
                            ) : null}
                        </SUITable.Row>
                    </SUITable.Header>
                    <SUITable.Body>
                        {orderBy(
                            costRecords,
                            [sort.column],
                            sort.direction === 'ascending' ? ['asc'] : ['desc']
                        ).map((p) => (
                            // @ts-ignore
                            <React.Fragment key={`total - ${p.field}`}>
                                <SUITable.Row
                                    // @ts-ignore
                                    className={`${rowColor(p)}`}
                                    // @ts-ignore
                                    key={`total-row-${p.field}`}
                                    // @ts-ignore
                                    id={`total-row-${p.field}`}
                                >
                                    <SUITable.Cell collapsing>
                                        <Checkbox
                                            // @ts-ignore
                                            checked={openRows.includes(p.field)}
                                            slider
                                            // @ts-ignore
                                            onChange={() => handleToggle(p.field)}
                                        />
                                    </SUITable.Cell>
                                    {HEADER_FIELDS.map((k) => {
                                        // Skip columns that are not visible
                                        if (!isColumnVisible(k.category)) {
                                            return null
                                        }

                                        switch (k.category) {
                                            case 'field':
                                                return (
                                                    <SUITable.Cell key={k.category} className="billing-href">
                                                        <b>
                                                            <Link
                                                                to={
                                                                    // @ts-ignore
                                                                    linkTo(p[k.category])
                                                                }
                                                            >
                                                                {
                                                                    // @ts-ignore
                                                                    p[k.category]
                                                                }
                                                            </Link>
                                                        </b>
                                                    </SUITable.Cell>
                                                )
                                            default:
                                                // We already checked visibility above, so just render
                                                return (
                                                    <SUITable.Cell key={k.category}>
                                                        {
                                                            // @ts-ignore
                                                            formatMoney(p[k.category])
                                                        }
                                                    </SUITable.Cell>
                                                )
                                        }
                                    })}

                                    {groupBy === BillingColumn.GcpProject &&
                                    invoiceMonth === thisMonth &&
                                    isColumnVisible('budget_spent') ? (
                                        <SUITable.Cell>
                                            {
                                                // @ts-ignore
                                                percFormat(p.budget_spent)
                                            }
                                        </SUITable.Cell>
                                    ) : null}
                                </SUITable.Row>
                                {typeof p === 'object' &&
                                    'details' in p &&
                                    orderBy(p?.details, ['monthly_cost'], ['desc']).map((dk) => (
                                        <SUITable.Row
                                            style={{
                                                // @ts-ignore
                                                display: openRows.includes(p.field)
                                                    ? 'table-row'
                                                    : 'none',
                                                backgroundColor: 'var(--color-bg)',
                                            }}
                                            key={`${dk.cost_category} - ${p.field}`}
                                            id={`${dk.cost_category} - ${p.field}`}
                                        >
                                            <SUITable.Cell style={{ border: 'none' }} />
                                            <SUITable.Cell>{dk.cost_category}</SUITable.Cell>

                                            {dk.cost_group === 'C' ? (
                                                <React.Fragment>
                                                    {invoiceMonth === thisMonth && isColumnVisible('compute_daily') ? (
                                                        <SUITable.Cell>
                                                            {formatMoney(dk.daily_cost)}
                                                        </SUITable.Cell>
                                                    ) : null}

                                                    {/* Calculate colspan dynamically based on visible columns */}
                                                    {(() => {
                                                        // For the daily section, we need to check visibility of storage_daily and total_daily
                                                        if (invoiceMonth === thisMonth) {
                                                            const visibleCount =
                                                                (isColumnVisible('storage_daily') ? 1 : 0) +
                                                                (isColumnVisible('total_daily') ? 1 : 0);

                                                            return visibleCount > 0 ? (
                                                                <SUITable.Cell colSpan={visibleCount} />
                                                            ) : null;
                                                        }
                                                        return null;
                                                    })()}

                                                    {isColumnVisible('compute_monthly') ? (
                                                        <SUITable.Cell>
                                                            {formatMoney(dk.monthly_cost)}
                                                        </SUITable.Cell>
                                                    ) : null}

                                                    {/* Calculate colspan dynamically based on visible columns */}
                                                    {(() => {
                                                        // For monthly section, check visibility of storage_monthly and total_monthly
                                                        const visibleCount =
                                                            (isColumnVisible('storage_monthly') ? 1 : 0) +
                                                            (isColumnVisible('total_monthly') ? 1 : 0);

                                                        return visibleCount > 0 ? (
                                                            <SUITable.Cell colSpan={visibleCount} />
                                                        ) : null;
                                                    })()}
                                                </React.Fragment>
                                            ) : (
                                                <React.Fragment>
                                                    {isColumnVisible('compute_daily') ? (
                                                        <SUITable.Cell />
                                                    ) : null}

                                                    {invoiceMonth === thisMonth && isColumnVisible('storage_daily') ? (
                                                        <SUITable.Cell>
                                                            {formatMoney(dk.daily_cost)}
                                                        </SUITable.Cell>
                                                    ) : null}

                                                    {/* Calculate colspan for total_daily */}
                                                    {invoiceMonth === thisMonth && isColumnVisible('total_daily') ? (
                                                        <SUITable.Cell />
                                                    ) : null}

                                                    {isColumnVisible('compute_monthly') ? (
                                                        <SUITable.Cell />
                                                    ) : null}

                                                    {isColumnVisible('storage_monthly') ? (
                                                        <SUITable.Cell>
                                                            {formatMoney(dk.monthly_cost)}
                                                        </SUITable.Cell>
                                                    ) : null}

                                                    {isColumnVisible('total_monthly') ? (
                                                        <SUITable.Cell />
                                                    ) : null}
                                                </React.Fragment>
                                            )}

                                            {groupBy === BillingColumn.GcpProject &&
                                             invoiceMonth === thisMonth &&
                                             isColumnVisible('budget_spent') ? (
                                                <SUITable.Cell />
                                            ) : null}
                                        </SUITable.Row>
                                    ))}
                            </React.Fragment>
                        ))}
                    </SUITable.Body>
                </Table>
            ) : null}
        </>
    )
}

export default function BillingCurrentCostPage() {
    return (
        <PaddedPage>
            <BillingCurrentCost />
        </PaddedPage>
    )
}
