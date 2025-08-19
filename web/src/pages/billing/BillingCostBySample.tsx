import { ApolloError, useQuery } from '@apollo/client'
import {
    Alert,
    Box,
    CircularProgress,
    ToggleButton,
    ToggleButtonGroup,
    Typography,
} from '@mui/material'
import { DatePicker } from '@mui/x-date-pickers/DatePicker'
import { DateTime } from 'luxon'
import { useContext, useEffect, useMemo, useState } from 'react'
import { gql } from '../../__generated__/gql'

import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import {
    BillingApi,
    BillingColumn,
    BillingSource,
    BillingTimePeriods,
    BillingTotalCostQueryModel,
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
    [BillingColumn.Topic, 'Topic'],
    [BillingColumn.ArGuid, 'AR guid'],
    [BillingColumn.CostCategory, 'Cost Category'],
    [BillingColumn.Stage, 'Stage'],
])

type EntityType = 'sequencing_group' | 'sample'

const SAMPLE_SG_QUERY = gql(`
    query SampleSgMap($sampleIds: [String!]!) {
        myProjects {
            samples(id: {in_: $sampleIds}) {
                id
                sequencingGroups {
                    id
                }
            }
        }
    }
`)

type BillingSampleSgMapResult = {
    loading: boolean
    data?: {
        sampleToSgs: Record<string, string[]>
        sgToSample: Record<string, string>
    }
    error?: ApolloError
}

// Get maps describing the relationships between samples and sequencing groups.
// This is needed for when you fetch billing data for samples as we only have billing data in
// the backend organized by sequencing group, so it is necessary to map from the SG data back
// to sample data and vice versa
function useBillingSampleSgMap(entityType: EntityType, ids: string[]): BillingSampleSgMapResult {
    // This map is only needed when requesting sample billing data
    const mapNeeded = entityType === 'sample' && ids.length > 0

    const { loading, data, error } = useQuery(SAMPLE_SG_QUERY, {
        variables: { sampleIds: ids },
        skip: !mapNeeded, // Only need to run this when working with sample ids
    })

    const result = useMemo(() => {
        if (!mapNeeded)
            return {
                loading: false,
                data: undefined,
                error: undefined,
            }
        if (loading || !data || error) return { loading, data: undefined, error }

        const idPairs = data.myProjects
            .flatMap((pp) => pp.samples)
            .flatMap((ss) => ss.sequencingGroups.map((sg) => ({ sgId: sg.id, sampleId: ss.id })))

        const sampleToSgs = idPairs.reduce((rr: Record<string, string[]>, ids) => {
            rr[ids.sampleId] = rr[ids.sampleId] ?? []
            rr[ids.sampleId].push(ids.sgId)
            return rr
        }, {})

        const sgToSample = idPairs.reduce((rr: Record<string, string>, ids) => {
            rr[ids.sgId] = ids.sampleId
            return rr
        }, {})

        return {
            loading,
            error,
            data: { sampleToSgs, sgToSample },
        }
    }, [loading, data, error, mapNeeded])

    return result
}

// Remove all the columns that weren't selected from the result, so that the remaining ones
// can be shown in the table
function transformBillingResultRow(result: BillingTotalCostRecord, breakDownBy: BillingColumn[]) {
    const cost = result.cost
    const month = result.day
    const sequencingGroup = result.sequencing_group

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
        sample: null as string | null,
        sequencingGroup: sequencingGroup ? [sequencingGroup] : [],
    }
}

type BillingResultRow = ReturnType<typeof transformBillingResultRow>

// Take the result rows and aggregate them up to the sample level
function aggregateToSampleLevel(
    data: BillingResultRow[],
    sgToSample: Record<string, string>
): BillingResultRow[] {
    if (data.length === 0) return []

    // Get the order of keys to use to compute composite key, just do this from the first
    // row to ensure that key is constructed in same order for all rows
    const firstRow = data[0]
    const { cost: cost, sequencingGroup: _sequencingGroup, ...breakDownCols } = firstRow
    const keyOrder = Object.keys(breakDownCols) as (keyof typeof breakDownCols)[]

    // Group the rows by a composite key composed of the field that we want to group by
    // This allows us to sum up the cost for all the sgs in a sample
    const groupedData = data.reduce((rr: Map<string, BillingResultRow>, row) => {
        const { cost, sequencingGroup, ...breakDownCols } = row
        if (!sequencingGroup || sequencingGroup.length === 0) return rr
        const sample = sgToSample[sequencingGroup[0]]
        if (!sample) return rr

        const compositeKey = keyOrder.map((key) => row[key]).join('||||')

        const val = rr.get(compositeKey)
        if (val) {
            val.cost += cost
            val.sequencingGroup = val.sequencingGroup.concat(sequencingGroup)
        } else {
            rr.set(compositeKey, {
                ...breakDownCols,
                sample,
                sequencingGroup: sequencingGroup,
                cost,
            })
        }

        return rr
    }, new Map())

    return [...groupedData.entries()].map(([_, val]) => val)
}

type BillingDataResult = {
    loading: boolean
    data?: BillingResultRow[]
    error?: ApolloError
    warnings?: string[]
}

// This is the main hook for fetching data from the billing API
function useBillingCostBySampleData(
    entityType: EntityType,
    ids: string[],
    dateRange: [DateTime, DateTime],
    breakDownBy: BillingColumn[]
): BillingDataResult {
    const [billingResult, setBillingResult] = useState<BillingDataResult>({ loading: false })

    const start = dateRange[0].toISODate()
    const end = dateRange[1].endOf('month').toISODate()

    if (!start || !end) throw new Error('Missing start or end')

    const billingSampleSgMap = useBillingSampleSgMap(entityType, ids)

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

        // Handle errors that come back from getting the sample map,
        // but only if working with sample ids
        if (entityType === 'sample' && billingSampleSgMap.error) {
            setBillingResult({
                loading: false,
                error: billingSampleSgMap.error,
            })
            return
        }

        // Handle loading state of sample map
        if (entityType === 'sample' && (!billingSampleSgMap.data || billingSampleSgMap.loading)) {
            setBillingResult({ loading: true })
            return
        }

        const sampleToSgs = billingSampleSgMap?.data?.sampleToSgs ?? {}
        const sgToSample = billingSampleSgMap?.data?.sgToSample ?? {}
        const missingSamples =
            entityType === 'sample' ? ids.filter((id) => !(id in sampleToSgs)) : []

        if (missingSamples.length > 0) {
            warnings.push(
                `Some samples were not included in results as it wasn't possible to find the sequencing groups those samples belong to. This may be because you don't have access to the projects that these samples belong to. Affected ids: ${missingSamples.join(', ')}`
            )
        }

        const sequencingGroupIds =
            entityType === 'sample' ? ids.flatMap((id) => sampleToSgs[id]).filter(Boolean) : ids

        // Don't run query if there's no ids to filter to
        if (sequencingGroupIds.length === 0) {
            setBillingResult({ loading: false, data: [] })
            return
        }

        const query: BillingTotalCostQueryModel = {
            fields: [...breakDownBy, BillingColumn.SequencingGroup],
            start_date: start,
            end_date: end,
            order_by: { day: false },
            source: BillingSource.Aggregate,
            time_periods: BillingTimePeriods.Month,
            filters: {
                sequencing_group: sequencingGroupIds,
            },
        }

        setBillingResult({
            loading: true,
        })

        billingApi
            .getTotalCost(query)
            .then((result) => {
                // If this has been superseded by another request, then ignore it
                if (ignore) return

                const data = result.data?.map((row) => transformBillingResultRow(row, breakDownBy))

                const groupedData =
                    entityType === 'sample' ? aggregateToSampleLevel(data, sgToSample) : data

                setBillingResult({
                    warnings,
                    loading: false,
                    data: groupedData,
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
    }, [entityType, ids, start, end, breakDownBy, billingSampleSgMap])

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

const sampleCols: GridColDef<BillingResultRow>[] = [
    {
        field: 'sample',
        width: 120,
        headerName: 'Sample',
    },
    {
        field: 'sequencingGroup',
        headerName: 'Sequencing Groups',
        width: 140,
        renderCell: (params) => params.row.sequencingGroup.join(', '),
    },
]

const sgCols: GridColDef<BillingResultRow>[] = [
    {
        field: 'sequencingGroup',
        headerName: 'Sequencing Group',
        renderCell: (params) => params.row.sequencingGroup.join(', '),
    },
]

function BillingCostBySampleTable(props: { entityType: EntityType; data: BillingResultRow[] }) {
    const { data, entityType } = props

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
            ...(entityType === 'sequencing_group' ? sgCols : []),
            ...(entityType === 'sample' ? sampleCols : []),
            ...breakDownColDefs,
            {
                field: 'cost',
                headerName: 'Cost',
                renderCell: (params) => formatMoney(params.row.cost),
            },
        ]
    }, [data, entityType])

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
        DateTime.now().minus({ months: 1 }).startOf('month'),
        DateTime.now().startOf('month'),
    ])
    const [breakDownBy, setBreakDownBy] = useState<BillingColumn[]>([BillingColumn.Topic])
    const viewer = useContext(ViewerContext)

    const idPrefixes: Record<EntityType, string> | undefined = viewer?.metamistSettings
        ? {
              sample: viewer.metamistSettings.samplePrefix,
              sequencing_group: viewer.metamistSettings.sequencingGroupPrefix,
          }
        : undefined

    // Calculate which id type has the most values

    const typeFrequency = idPrefixes
        ? Object.entries(idPrefixes)
              .map(([type, prefix]) => {
                  const entityType = type as EntityType // ts doesn't know how to pull types through Object.entries
                  const count = idList.filter((id) => id.startsWith(prefix)).length
                  return { entityType, prefix, count }
              }, {})
              .filter(({ count }) => count > 0)
              .sort((a, b) => b.count - a.count)
        : undefined

    const mostFrequentPrefix = typeFrequency?.[0]?.prefix
    const mostFrequentType = typeFrequency?.[0]?.entityType

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

    // Filter ids down to the type of the most frequent used id type, memoize this otherwise
    // the hook to get data will update constantly
    const validIdList = useMemo(() => {
        return mostFrequentPrefix && idList
            ? idList.filter((id) => id.startsWith(mostFrequentPrefix))
            : []
    }, [mostFrequentPrefix, idList])

    const entityType = mostFrequentType ?? 'sequencing_group'

    const { loading, data, error, warnings } = useBillingCostBySampleData(
        entityType,
        validIdList,
        dateRange,
        breakDownBy
    )

    return (
        <PaddedPage>
            <Typography variant="h1" fontSize={28} fontWeight={'bold'}>
                Billing cost by sample
            </Typography>

            <Box mt={2}>
                {idPrefixes ? (
                    <IdSelector
                        idPrefixes={Object.values(idPrefixes)}
                        idList={idList}
                        onChange={(value) => setIdList(value)}
                    />
                ) : (
                    <CircularProgress />
                )}
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
                        loaded for one type of id at a time. Only {mostFrequentType} ids will be
                        used.
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
                    <BillingCostBySampleTable entityType={entityType} data={data ?? []} />
                </Box>
            )}
        </PaddedPage>
    )
}

export default BillingCostBySample
