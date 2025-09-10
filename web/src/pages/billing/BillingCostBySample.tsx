import { ApolloError } from '@apollo/client'
import {
    Alert,
    Box,
    Button,
    Modal,
    Link as MuiLink,
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

// A map of billing column enums to names for the breakdown selector
const BREAK_DOWN_MAP: Map<BillingColumn, string> = new Map([
    [BillingColumn.SequencingGroup, 'Sequencing Group / Sample'],
    [BillingColumn.Topic, 'Topic'],
    [BillingColumn.ArGuid, 'AR guid'],
    [BillingColumn.CostCategory, 'Cost Category'],
    [BillingColumn.Stage, 'Stage'],
])

// A map of billing column enums to column titles
const COLUMN_NAME_MAP: Map<BillingColumn, string> = new Map([
    ...BREAK_DOWN_MAP.entries(),
    // Override the title name for sequencing group column as it won't show samples
    [BillingColumn.SequencingGroup, 'Sequencing Group'],
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

type DataQuery = {
    ids: string[]
    dateRange: [DateTime, DateTime]
    breakDownBy: BillingColumn[]
}

// This is the main hook for fetching data from the billing API
function useBillingCostBySampleData({ ids, dateRange, breakDownBy }: DataQuery): BillingDataResult {
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
                headerName: COLUMN_NAME_MAP.get(col as BillingColumn) || col,
                renderCell: (params) => {
                    const value = params.value
                    if (typeof value === 'string') {
                        return value.split(',').join(', ')
                    }
                    return value
                },
            })
        )

        return [
            {
                field: 'month',
                headerName: 'Month',
                width: 120,
                renderCell: (params) => {
                    if (!params.row.month) return ''
                    return DateTime.fromFormat(params.row.month, 'yyyyMM').toFormat('LLL yyyy')
                },
            },
            ...(hasSample ? sampleCol : []),
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

    // Store data query separately from individual filters so that we only run the data request
    // when the search button is clicked. This avoids over-requesting when users are changing filters
    const [dataQuery, setDataQuery] = useState<DataQuery>({ ids: [], dateRange, breakDownBy })

    const [showingInfo, setShowingInfo] = useState(false)
    const viewer = useContext(ViewerContext)

    const idPrefixes: Record<string, string> | undefined = viewer?.metamistSettings
        ? {
              sample: viewer.metamistSettings.samplePrefix,
              sequencing_group: viewer.metamistSettings.sequencingGroupPrefix,
          }
        : undefined

    // get search params from url on first load. The URL isn't the source of the state so
    // it is only read from once, and then written back to when filters change.
    useEffect(() => {
        const search = new URLSearchParams(window.location.search)
        const startQs = search.get('start')
        const endQs = search.get('end')
        const breakDownByQs = search.get('breakDownBy')
        const idsQs = search.get('idList')

        let dateRange: [DateTime, DateTime] | null = null
        let ids: string[] | null = null
        let breakDownBy: BillingColumn[] | null = null

        if (startQs && endQs) {
            const parsedStart = DateTime.fromISO(startQs)
            const parsedEnd = DateTime.fromISO(endQs)
            if (parsedStart.isValid && parsedEnd.isValid && parsedEnd > parsedStart) {
                setDateRange([parsedStart, parsedEnd])
                dateRange = [parsedStart, parsedEnd]
            }
        }

        if (breakDownByQs) {
            const validBillingColumns = new Set(
                Object.entries(BillingColumn).map(([_, value]) => value)
            )
            const trimmedBreakDownBy = breakDownByQs
                .split(',')
                .map((ii) => ii.trim()) as BillingColumn[]
            const validBreakDownBy = trimmedBreakDownBy.filter((ii) => validBillingColumns.has(ii))
            setBreakDownBy(validBreakDownBy)
            breakDownBy = validBreakDownBy
        }

        if (idsQs) {
            ids = idsQs.split(',').map((id) => id.trim().toUpperCase())
            setIdList(ids)
        }

        setDataQuery((prev) => ({
            dateRange: dateRange ?? prev.dateRange,
            ids: ids ?? prev.ids,
            breakDownBy: breakDownBy ?? prev.breakDownBy,
        }))
    }, [])

    // Update url with filters when they change
    useEffect(() => {
        const { ids, dateRange, breakDownBy } = dataQuery
        const idListVal = ids.join(',')
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
    }, [dataQuery])

    const { loading, data, error, warnings } = useBillingCostBySampleData(dataQuery)

    // Calculate whether to enable the search button. Dirty state refers to whether the current
    // filters match the query for which results are displayed.
    const isDirty = useMemo(() => {
        let dirty = false
        if (dataQuery.ids.join(',') !== idList.join(',')) dirty = true
        if (dataQuery.breakDownBy.join(',') !== breakDownBy.join(',')) dirty = true
        if (dataQuery.dateRange.join(',') !== dateRange.join(',')) dirty = true

        return dirty
    }, [dataQuery, idList, breakDownBy, dateRange])

    return (
        <PaddedPage>
            <Box display="flex">
                <Box mr={2}>
                    <Typography variant="h1" fontSize={28} fontWeight={'bold'}>
                        Billing cost by sample
                    </Typography>
                </Box>
                <Box alignSelf={'end'}>
                    <MuiLink
                        href="#"
                        sx={{ fontSize: 12, mr: 2 }}
                        color="info"
                        onClick={(e) => {
                            e.preventDefault()
                            setShowingInfo(true)
                        }}
                    >
                        How is this calculated?
                    </MuiLink>
                </Box>
            </Box>

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
                            sx={{ maxWidth: 180 }}
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
                            sx={{ maxWidth: 180 }}
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
                        {[...BREAK_DOWN_MAP.entries()].map(([id, col]) => (
                            <ToggleButton value={id} key={id}>
                                {col}
                            </ToggleButton>
                        ))}
                    </ToggleButtonGroup>
                </Box>
                {/* Align to end of row */}
                <Box justifyContent={'end'} alignSelf={'end'} flexGrow={1} display={'flex'}>
                    <Button
                        disabled={!isDirty}
                        variant="contained"
                        onClick={() => {
                            setDataQuery({ ids: idList, dateRange, breakDownBy })
                        }}
                    >
                        Search
                    </Button>
                </Box>
            </Box>

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

            <Modal open={showingInfo} onClose={() => setShowingInfo(false)}>
                <Box
                    sx={{
                        position: 'absolute',
                        top: '50%',
                        left: '50%',
                        transform: 'translate(-50%, -50%)',
                        width: '100%',
                        maxWidth: 640,
                        bgcolor: 'background.paper',
                        boxShadow: 24,
                        p: 4,
                    }}
                >
                    <Typography variant="h5" component="h2" mb={2}>
                        How are Sample and Sequencing Group costs calculated?
                    </Typography>
                    <Alert severity="info">
                        Due to constraints in the cost reporting available in GCP some costs
                        displayed on this page are estimated.
                    </Alert>

                    <Typography variant="h6" component="h2" mt={2}>
                        Storage cost calculations
                    </Typography>
                    <Typography mb={2}>
                        GCP cost reporting only provides storage costs at a bucket level. The cost
                        figures displayed on this page are estimated using sample CRAM file sizes to
                        apportion costs to a sequencing group level. The calculation is:
                    </Typography>
                    <Typography mb={2}>
                        <code>
                            sg storage cost = (sg cram file size / total of cram file sizes in
                            project) * total project storage cost
                        </code>
                    </Typography>
                    <Typography mb={2}>
                        If a sequencing group does not have a active CRAM file then no storage cost
                        will be allocated.
                    </Typography>
                    <Typography mb={2}>
                        The <em>current</em> CRAM file size is used, so recent changes to CRAM files
                        can impact historical cost reporting.
                    </Typography>
                    <Typography mb={2}>
                        A sequencing group will be allocated storage costs for any month that it was
                        "active". Active months are defined as the time period from when the sample
                        was first seen either in metamist audit logs, or in billing compute records.
                    </Typography>

                    <Typography mb={2} fontWeight={'bold'}>
                        Storage costs are intended to be used as an indicative estimate and should
                        not be used as precise measurements.
                    </Typography>

                    <Typography variant="h6" component="h2" mt={2}>
                        Compute cost calculations
                    </Typography>

                    <Typography mb={2}>
                        If a compute billing record contains multiple sequencing groups, the cost is
                        allocated evenly across all sequencing groups for that record.
                    </Typography>
                </Box>
            </Modal>
        </PaddedPage>
    )
}

export default BillingCostBySample
