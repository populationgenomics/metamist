import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Button, Card, Grid, Input, Message, Select } from 'semantic-ui-react'
import SearchIcon from '@mui/icons-material/Search'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { BillingApi, BillingTotalCostRecord } from '../../sm-api'
import HailBatchGrid from './components/HailBatchGrid'

enum SearchType {
    Ar_guid,
    Batch_id,
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

    const [data, setData] = React.useState<any>([])

    const [searchTxt, setSearchTxt] = React.useState<string | undefined>(
        searchParams.get('searchTxt') ?? undefined
    )

    const searchOptions: string[] = Object.keys(SearchType).filter((item) => isNaN(Number(item)))
    const dropdownOptions = searchOptions.map((item) => ({
        text: item.replaceAll('_', ' '),
        value: item,
    }))

    const [searchByType, setSearchByType] = React.useState<SearchType | undefined>(
        SearchType[searchParams.get('searchType')] ?? undefined
    )

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
            new BillingApi()
                .costByArGuid(sTxt)
                .then((response) => {
                    setIsLoading(false)
                    setData(response.data)
                })
                .catch((er) => setError(er.message))
        } else if (sType === SearchType.Batch_id) {
            new BillingApi()
                .costByBatchId(sTxt)
                .then((response) => {
                    setIsLoading(false)
                    setData(response.data)
                })
                .catch((er) => setError(er.message))
        } else {
            setIsLoading(false)
        }
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
        setSearchTxt(dt.value)
    }

    const handleSearchTypeChange = (event: any, dt: any) => {
        setSearchByType(SearchType[dt.value])
    }

    const handleSearchKeyPress = (event: any) => {
        updateNav(searchByType, searchTxt)
        handleSearch()
    }

    React.useEffect(() => {
        handleSearch()
    }, [])

    const searchCard = () => (
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
                    />
                    <Select
                        compact
                        options={dropdownOptions}
                        onChange={handleSearchTypeChange}
                        defaultValue={
                            searchByType !== undefined
                                ? dropdownOptions[searchByType].value
                                : dropdownOptions[0].value
                        }
                    />
                    <Button type="submit" onClick={handleSearchKeyPress}>
                        <SearchIcon />
                    </Button>
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
            </p>
        </Card>
    )

    const gridCard = (data: BillingTotalCostRecord[]) => (
        <Card fluid style={{ padding: '20px', overflowX: 'scroll' }} id="billing-container-data">
            <HailBatchGrid data={data} />
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
            {gridCard(data.costs)}
        </>
    )
}

export default BillingCostByAnalysis
