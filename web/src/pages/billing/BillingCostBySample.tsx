import { ApolloError } from '@apollo/client'
import {
    Alert,
    Box,
    ToggleButton,
    ToggleButtonGroup,
    Typography,
} from '@mui/material'
import { DatePicker } from '@mui/x-date-pickers/DatePicker'
import { DateTime } from 'luxon'
import { useContext, useEffect, useMemo, useState } from 'react'

import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import {
    BillingApi,
    BillingColumn,
    BillingSampleQueryModel,
    BillingTotalCostRecord,
} from '../../sm-api'

import {
    DataGrid,
    GridColDef,
    GridToolbarContainer,
    GridToolbarExport,
    GridToolbarQuickFilter,
} from '@mui/x-data-grid'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import formatMoney from '../../shared/utilities/formatMoney'
import { ViewerContext } from '../../viewer'
import IdSelector from './components/IdSelector'

const billingApi = new BillingApi()

// A map of billing column enums to titles, these are the supported fields to break down by
const BILLING_COLUMN_MAP: Map<BillingColumn, string> = new Map([
    [BillingColumn.SequencingGroup, 'Sequencing Group / Sample'],
    [BillingColumn.Topic, 'Topic'],
    [BillingColumn.ArGuid, 'AR guid'],
    [BillingColumn.CostCategory, 'Cost Category'],
    [BillingColumn.Stage, 'Stage'],
])


// Remove all the columns that weren't selected from the result, so that the remaining ones
// can be shown in the table
function transformBillingResultRow(result: BillingTotalCostRecord, breakDownBy: BillingColumn[]) {
    const cost = result.cost
    const month = result.invoice_month
    const sequencingGroup = result.sequencing_group
    const sample = result.sample

    const breakDownCols = breakDownBy.reduce(
        (rr: Partial<{ [key in BillingColumn]: number | string | object | null }>, col) => {
            if (col in result) {
                // @ts-ignore ts can't figure out that the col in result check is ensuring that the
                // column exists and can be used to index the result type
                rr[col] = result[col]
            }
            return rr
        },
        {}
    )

    return {
        ...breakDownCols,
        cost,
        month,
        sample,
        sequencingGroup: sequencingGroup ? [sequencingGroup] : [],
    }
}

type BillingResultRow = ReturnType<typeof transformBillingResultRow>

type BillingDataResult = {
    loading: boolean
    data?: BillingResultRow[]
    error?: ApolloError
    warnings?: string[]
}

// This is the main hook for fetching data from the billing API
function useBillingCostBySampleData(
    ids: string[],
    dateRange: [DateTime, DateTime],
    breakDownBy: BillingColumn[]
): BillingDataResult {

    const [billingResult, setBillingResult] = useState<BillingDataResult>({ loading: false })

    const start = dateRange[0].toISODate()
    const end = dateRange[1].endOf('month').toISODate()

    if (!start || !end) throw new Error('Missing start or end')

    useEffect(() => {
        // Will get set to true if effect becomes stale, in which case we don't want to return data
        // This happens when a request is made, and then another one is made before the first
        // one completes
        let ignore = false
        // Allow returning warnings
        const warnings: string[] = []

        // Clear table/error/loading state if no ids entered
        if (ids.length === 0) {
            setBillingResult({
                loading: false,
            })
            return
        }

        // Don't run query if there's no ids to filter to
        if (ids.length === 0) {
            setBillingResult({ loading: false, data: [] })
            return
        }

        const query: BillingSampleQueryModel = {
            start_date: start,
            end_date: end,
            search_ids: ids,
            fields: [...breakDownBy],
        }

        setBillingResult({
            loading: true,
        })
        billingApi
            .costBySample(query)
            .then((result) => {
                // If this has been superseded by another request, then ignore it
                if (ignore) return

                const data = result.data?.map((row) => transformBillingResultRow(row, breakDownBy))
                setBillingResult({
                    warnings,
                    loading: false,
                    data: data,
                })
            })
            .catch((err) => {
                if (ignore) return
                setBillingResult({
                    loading: false,
                    error: err,
                })
            })

        return () => {
            ignore = true
        }
    }, [ids, start, end, breakDownBy])

    return billingResult
}

function Loading() {
    return (
        <Box mt={2}>
            <LoadingDucks />
            <Typography fontStyle={'italic'} textAlign={'center'}>
                This query takes a while...
            </Typography>
        </Box>
    )
}

function CustomTableToolbar() {
    return (
        <GridToolbarContainer>
            <GridToolbarExport />
            <Box sx={{ flexGrow: 1 }} />
            <GridToolbarQuickFilter />
        </GridToolbarContainer>
    )
}

const sampleCol: GridColDef<BillingResultRow>[] = [
    {
        field: 'sample',
        width: 120,
        headerName: 'Sample',
    },
]

function BillingCostBySampleTable(props: { data: BillingResultRow[] }) {
    const { data } = props

    const columns: GridColDef<BillingResultRow>[] = useMemo(() => {
        const firstRow = data[0]
        if (!firstRow) return []
        const {
            cost: _cost,
            month: _month,
            sequencingGroup: _sequencingGroup,
            sample: _sample,
            ...breakDownCols
        } = firstRow

        // check if any of data record contains a non-empty sample string
        const hasSample = data.some((row) => typeof row.sample === 'string')

        const breakDownColDefs = Object.keys(breakDownCols).map(
            (col): GridColDef<BillingResultRow> => ({
                field: col,
                width: 120,
                headerName: BILLING_COLUMN_MAP.get(col as BillingColumn) || col,
            })
        )

        return [
            {
                field: 'month',
                headerName: 'Month',
                valueGetter: (_value, row) => row.month?.slice(0, 7),
            },
            ... (hasSample ? sampleCol : []),
            ...breakDownColDefs,
            {
                field: 'cost',
                headerName: 'Cost',
                renderCell: (params) => formatMoney(params.row.cost),
            },
        ]
    }, [data])

    return (
        <Box
            minHeight={360}
            maxHeight={'calc(100vh - 100px)'}
            display={'flex'}
            flexDirection={'column'}
        >
            <DataGrid
                slots={{ toolbar: CustomTableToolbar }}
                slotProps={{
                    toolbar: {
                        showQuickFilter: true,
                    },
                }}
                rows={data}
                getRowId={(row) => Object.values(row).join('|||')}
                columns={columns}
                density="compact"
            />
        </Box>
    )
}

function BillingCostBySample() {
    const [idList, setIdList] = useState<string[]>([])
    const [dateRange, setDateRange] = useState<[DateTime, DateTime]>([
        // first month of collected AR-GUIDs
        DateTime.fromISO('2023-10-01').startOf('month'),
        DateTime.now().startOf('month'),
    ])
    const [breakDownBy, setBreakDownBy] = useState<BillingColumn[]>([BillingColumn.SequencingGroup])
    const viewer = useContext(ViewerContext)

    const idPrefixes: Record<string> | undefined = viewer?.metamistSettings
        ? {
              sample: viewer.metamistSettings.samplePrefix,
              sequencing_group: viewer.metamistSettings.sequencingGroupPrefix,
          }
        : undefined

    // Calculate which id type has the most values

    const typeFrequency = idPrefixes
        ? Object.entries(idPrefixes)
              .map(([prefix]) => {
                 const count = idList.filter((id) => id.startsWith(prefix)).length
                  return { prefix, count }
              }, {})
              .filter(({ count }) => count > 0)
              .sort((a, b) => b.count - a.count)
        : undefined

    // get search params from url on first load. The URL isn't the source of the state so
    // it is only read from once, and then written back to when filters change.
    useEffect(() => {
        const search = new URLSearchParams(window.location.search)
        const start = search.get('start')
        const end = search.get('end')
        const breakDownBy = search.get('breakDownBy')
        const ids = search.get('idList')

        if (start && end) {
            const parsedStart = DateTime.fromISO(start)
            const parsedEnd = DateTime.fromISO(end)
            if (parsedStart.isValid && parsedEnd.isValid && parsedEnd > parsedStart) {
                setDateRange([parsedStart, parsedEnd])
            }
        }

        if (breakDownBy) {
            const validBillingColumns = new Set(
                Object.entries(BillingColumn).map(([_, value]) => value)
            )
            const trimmedBreakDownBy = breakDownBy
                .split(',')
                .map((ii) => ii.trim()) as BillingColumn[]
            const validBreakDownBy = trimmedBreakDownBy.filter((ii) => validBillingColumns.has(ii))
            setBreakDownBy(validBreakDownBy)
        }

        if (ids) {
            setIdList(ids.split(',').map((id) => id.trim().toUpperCase()))
        }
    }, [])

    // Update url with filters when they change
    useEffect(() => {
        const idListVal = idList.join(',')
        const breakDownByVal = breakDownBy.join(',')
        const startVal = dateRange[0].toISODate()
        const endVal = dateRange[1].toISODate()

        const update = setTimeout(() => {
            const search = new URLSearchParams({
                idList: idListVal,
                breakDownBy: breakDownByVal,
                ...(startVal ? { start: startVal } : {}),
                ...(endVal ? { end: endVal } : {}),
            })

            const url = new URL(window.location.href)
            url.search = search.toString()
            window.history.replaceState({}, '', url)
        }, 300)

        return () => {
            clearTimeout(update)
        }
    }, [breakDownBy, dateRange, idList])

    const { loading, data, error, warnings } = useBillingCostBySampleData(
        idList,
        dateRange,
        breakDownBy
    )

    return (
        <PaddedPage>
            <Typography variant="h1" fontSize={28} fontWeight={'bold'}>
                Billing cost by sample
            </Typography>

            <Box mt={2}>
                <IdSelector
                    idPrefixes={idPrefixes ? Object.values(idPrefixes) : []}
                    idList={idList}
                    onChange={(value) => setIdList(value)}
                />
            </Box>

            <Box mt={2} display={'flex'} gap={4}>
                <Box>
                    <Typography fontSize={14} fontWeight={'bold'} mb={1}>
                        Months:
                    </Typography>
                    <Box display={'flex'} gap={2}>
                        <DatePicker
                            label={'Start'}
                            views={['year', 'month']}
                            openTo={'month'}
                            value={dateRange[0]}
                            onChange={(value) => {
                                if (value) setDateRange([value, dateRange[1]])
                            }}
                            slotProps={{ textField: { size: 'small' } }}
                        />
                        <DatePicker
                            label={'End'}
                            views={['year', 'month']}
                            openTo={'month'}
                            onChange={(value) => {
                                if (value) setDateRange([dateRange[0], value])
                            }}
                            value={dateRange[1]}
                            slotProps={{ textField: { size: 'small' } }}
                        />
                    </Box>
                </Box>
                <Box>
                    <Typography fontSize={14} fontWeight={'bold'} mb={1}>
                        Break down by:
                    </Typography>
                    <ToggleButtonGroup
                        color="primary"
                        size={'small'}
                        value={breakDownBy}
                        onChange={(_e, value) => {
                            setBreakDownBy(value)
                        }}
                    >
                        {[...BILLING_COLUMN_MAP.entries()].map(([id, col]) => (
                            <ToggleButton value={id} key={id}>
                                {col}
                            </ToggleButton>
                        ))}
                    </ToggleButtonGroup>
                </Box>
            </Box>

            {typeFrequency && typeFrequency.length > 1 ? (
                <Box mt={2}>
                    <Alert severity="warning">
                        More than one type of ID has been entered, but billing data can only be
                        loaded for one type of id at a time.
                    </Alert>
                </Box>
            ) : null}

            {error && (
                <Box mt={2}>
                    <Alert severity="error">{error.message}</Alert>
                </Box>
            )}

            {warnings &&
                warnings.map((warning) => (
                    <Box mt={2} key={warning}>
                        <Alert severity="warning">{warning}</Alert>
                    </Box>
                ))}

            {loading && <Loading />}

            {!loading && (
                <Box mt={4}>
                    <BillingCostBySampleTable data={data ?? []} />
                </Box>
            )}
        </PaddedPage>
    )
}

export default BillingCostBySample
