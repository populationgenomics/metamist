import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import {
    Header,
    Button,
    Card,
    Grid,
    Input,
    Message,
    Table as SUITable,
    Select,
} from 'semantic-ui-react'
import SearchIcon from '@mui/icons-material/Search'
import Table from '../../shared/components/Table'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import {
    BillingApi,
    BillingColumn,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
} from '../../sm-api'
import HailBatchGrid from './components/HailBatchGrid'
import { Filter } from '../project/AnalysisRunnerView/Filter'

function currencyFormat(num: number): string {
    if (num === undefined || num === null) {
        return ''
    }

    return `$${num.toFixed(2).replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,')}`
}

const extractHeader = (rec: BillingTotalCostRecord) => {
    if (rec == null) {
        return ''
    }
    if (rec.batch_id != null) {
        return `Batch ID:${rec.batch_id}`
    }
    if (rec.ar_guid != null) {
        return `AR GUID:${rec.ar_guid}`
    }
    if (rec.sequencing_group != null) {
        return `Sequencing Group:${rec.sequencing_group}`
    }
    if (rec.cromwell_workflow_id != null) {
        return `CROMWELL WORKFLOW ID:${rec.cromwell_workflow_id}`
    }
    return ''
}

const BillingCostByAnalysis: React.FunctionComponent = () => {
    const [searchParams] = useSearchParams()

    const now = new Date()

    // Data loading
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()

    const [start, setStart] = React.useState<string>(
        searchParams.get('start') ??
            `${now.getFullYear()}-${now.getMonth().toString().padStart(2, '0')}-01`
    )
    const [end, setEnd] = React.useState<string>(
        searchParams.get('end') ??
            `${now.getFullYear()}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now
                .getDate()
                .toString()
                .padStart(2, '0')}`
    )

    const [data, setData] = React.useState<any>([])

    const [searchTxt, setSearchTxt] = React.useState<string | undefined>(
        searchParams.get('searchTxt') ?? undefined
    )

    const [filters, setFilters] = React.useState<Filter[]>([])
    const [sort, setSort] = React.useState<{ column: string | null; direction: string | null }>({
        column: 'timestamp',
        direction: 'descending',
    })

    enum SearchType {
        Ar_guid,
        Batch_id,
    }
    const searchOptions: string[] = Object.keys(SearchType).filter((item) => isNaN(Number(item)))
    const dropdownOptions = searchOptions.map((item) => ({
        text: item.replaceAll('_', ' '),
        value: item,
    }))

    const [searchByType, setSearchByType] = React.useState<SearchType | undefined>(
        SearchType[searchParams.get('searchType')] ?? undefined
    )

    console.log(
        'searchType from',
        searchParams.get('searchType'),
        SearchType[searchParams.get('searchType')]
    )
    console.log('searchType', searchByType)

    console.log('dropdownOptions', dropdownOptions)

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (sType: SearchType | undefined, sTxt: string | undefined) => {
        let url = `${location.pathname}?`
        const params: string[] = []
        if (sType !== undefined) params.push(`searchType=${SearchType[sType]}`)
        if (sTxt !== undefined) params.push(`searchTxt=${sTxt}`)

        url += params.join('&')
        navigate(url)
    }

    const getData = (sType: SearchType, sTxt: string) => {
        if ((sType === undefined || sTxt === undefined) && sTxt.length < 6) {
            // Seaarch text is not large enough
            setIsLoading(false)
            return
        }
        setIsLoading(true)
        setError(undefined)

        if (sType === SearchType.Ar_guid) {
            console.log('searching by Ar_guid', sType, sTxt)

            new BillingApi()
                .costByArGuid(sTxt)
                .then((response) => {
                    console.log(response)
                    setIsLoading(false)
                    setData(response.data)
                })
                .catch((er) => setError(er.message))
        } else if (sType === SearchType.Batch_id) {
            console.log('searching by Batch_id', sType, sTxt)

            new BillingApi()
                .costByBatchId(sTxt)
                .then((response) => {
                    console.log(response)
                    setIsLoading(false)
                    setData(response.data)
                })
                .catch((er) => setError(er.message))
        } else {
            setIsLoading(false)
            console.log('searching by other', sType, sTxt)
        }

        // new BillingApi()
        //     // .getTotalCost(query)
        //     .costByBatchId(query)
        //     .then((response) => {
        //         setIsLoading(false)
        //         setData(response.data.map((d, idx) => ({ ...d, position: idx })))
        //     })
        //     .catch((er) => setError(er.message))
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

    const updateFilter = (v: string, c: string) => {
        setFilters([
            ...filters.filter(({ category }) => c !== category),
            ...(v ? [{ value: v, category: c }] : []),
        ])
    }

    const handleSearch = () => {
        if (searchByType === undefined || searchTxt === undefined || searchTxt.length < 6) {
            // Seaarch text is not large enough
            setIsLoading(false)
            return
        }
        getData(searchByType, searchTxt)
    }

    const handleSearchChange = (event: any, dt: any) => {
        console.log(dt.value)
        setSearchTxt(dt.value)
        updateNav(searchByType, dt.value)
    }

    const handleSearchTypeChange = (event: any, dt: any) => {
        console.log(typeof dt.value, dt.value)
        setSearchByType(SearchType[dt.value])
        updateNav(SearchType[dt.value], searchTxt)
    }

    React.useEffect(() => {
        handleSearch()
    }, [searchTxt, searchByType])

    const searchCard = () => {
        return (
            <Card fluid style={{ padding: '20px' }} id="billing-container">
                <h1
                    style={{
                        fontSize: 40,
                    }}
                >
                    Billing Cost By Analysis
                </h1>

                <Grid columns={2} stackable>
                    <Grid.Column width={10}>
                        <Input
                            fluid
                            input={{ fluid: true }}
                            placeholder={`Search...`}
                            onChange={handleSearchChange}
                            value={searchTxt}
                            action={{ icon: 'search' }}
                        >
                            <input />
                            <Select
                                compact
                                options={dropdownOptions}
                                defaultValue={dropdownOptions[0].value}
                                onChange={handleSearchTypeChange}
                                value={
                                    searchByType !== undefined
                                        ? dropdownOptions[searchByType].value
                                        : undefined
                                }
                            />
                            <Button type="submit">
                                <SearchIcon />
                            </Button>
                        </Input>
                    </Grid.Column>
                </Grid>

                <br />
                <p>
                    {' '}
                    e.g.
                    <br />
                    ar_guid: f5a065d2-c51f-46b7-a920-a89b639fc4ba
                    <br />
                    batch_id: 430604, 430605
                    <br />
                    url:
                    http://localhost:5173/billing/costByAnalysis?searchType=Ar_guid&searchTxt=f5a065d2-c51f-46b7-a920-a89b639fc4ba
                </p>
            </Card>
        )
    }

    const dataDumpCard = (data: BillingTotalCostRecord[]) => (
        <Card fluid style={{ padding: '20px', overflowX: 'scroll' }} id="billing-container-data">
            <Header as="h3">{extractHeader(data[0])}</Header>
            <Table celled compact sortable selectable>
                <SUITable.Header>
                    <SUITable.Row>
                        <SUITable.HeaderCell
                            style={{
                                borderBottom: 'none',
                            }}
                        >
                            Cost Category
                        </SUITable.HeaderCell>
                        <SUITable.HeaderCell
                            style={{
                                borderBottom: 'none',
                            }}
                        >
                            SKU
                        </SUITable.HeaderCell>
                        <SUITable.HeaderCell
                            style={{
                                borderBottom: 'none',
                            }}
                        >
                            Cost
                        </SUITable.HeaderCell>
                    </SUITable.Row>
                </SUITable.Header>
                <SUITable.Body>
                    {data.map((k, i) => (
                        <SUITable.Row key={i}>
                            <SUITable.Cell>{k.cost_category}</SUITable.Cell>
                            <SUITable.Cell>{k.sku.id}</SUITable.Cell>
                            <SUITable.Cell>
                                <b>{currencyFormat(k.cost)}</b>
                            </SUITable.Cell>
                        </SUITable.Row>
                    ))}
                </SUITable.Body>
            </Table>
        </Card>
    )

    const gridCard = (data: BillingTotalCostRecord[]) => (
        <Card fluid style={{ padding: '20px', overflowX: 'scroll' }} id="billing-container-data">
            <HailBatchGrid
                data={data}
                // idColumn="analysis_id"
                filters={filters}
                updateFilter={updateFilter}
                sort={sort}
                handleSort={handleSort}
            />

            {/* <AnalysisRunnerGrid
                data={(!sort.column
                    ? flatData
                    : _.orderBy(
                        flatData,
                        [sort.column],
                        sort.direction === 'ascending' ? ['asc'] : ['desc']
                    )
                )
                    .filter((log) =>
                        filters.every(({ category, value }) =>
                            _.get(log, category, '').includes(value)
                        )
                    )
                    .slice((pageNumber - 1) * pageLimit, pageNumber * pageLimit)}
                filters={filters}
                updateFilter={updateFilter}
                handleSort={handleSort}
                sort={sort}
            /> */}
        </Card>
    )

    if (error) {
        return (
            <Message negative onDismiss={() => setError(undefined)}>
                {error}
                <br />
                <Button color="red" onClick={() => setStart(start)}>
                    Retry
                </Button>
            </Message>
        )
    }

    if (isLoading) {
        return (
            <div>
                {searchCard()}
                <LoadingDucks />
                <p style={{ textAlign: 'center', marginTop: '5px' }}>
                    <em>This query takes a while...</em>
                </p>
            </div>
        )
    }

    if (data.length === 0) {
        return (
            <div>
                {searchCard()}
                <p style={{ textAlign: 'center', marginTop: '5px' }}>
                    <em>No data found.</em>
                </p>
            </div>
        )
    }

    return (
        <>
            {searchCard()}
            {/* {dataDumpCard(data.costs)} */}
            {gridCard(data.costs)}
        </>
    )
}

export default BillingCostByAnalysis
