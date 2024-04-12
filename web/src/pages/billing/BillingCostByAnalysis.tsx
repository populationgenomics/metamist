import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Button, Card, Grid, Input, Message, Select, Dropdown } from 'semantic-ui-react'
import SearchIcon from '@mui/icons-material/Search'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { BillingApi, AnalysisCostRecord } from '../../sm-api'
import BatchGrid from './components/BatchGrid'

import { getMonthStartDate } from '../../shared/utilities/monthStartEndDate'
import generateUrl from '../../shared/utilities/generateUrl'
import { List } from 'lodash'

enum SearchType {
    Ar_guid,
    Batch_id,
}

const BillingCostByAnalysis: React.FunctionComponent = () => {
    const [searchParams] = useSearchParams()

    // Data loading
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [data, setData] = React.useState<AnalysisCostRecord[] | undefined>()
    const [start, setStart] = React.useState<string>(
        searchParams.get('start') ?? getMonthStartDate()
    )

    const setBillingRecord = (records: AnalysisCostRecord[]) => {
        setIsLoading(false)
        // arData is an array of objects, we use only the first obejct
        // in the future we maye have search by several ar_guids / author etc.
        setData(records)
        if (!records || records.length === 0) {
            // nothing found
            setIsLoading(false)
            setData(null)
            return
        }
        setIsLoading(false)
    }

    const [searchTxt, setSearchTxt] = React.useState<string>(searchParams.get('searchTxt') ?? '')

    const searchOptions: string[] = Object.keys(SearchType).filter((item) => isNaN(Number(item)))
    const dropdownOptions = searchOptions.map((item) => ({
        text: item.replaceAll('_', ' '),
        value: item,
    }))

    const [searchByType, setSearchByType] = React.useState<SearchType>(
        (searchParams.get('searchType') as SearchType) ?? SearchType[0]
    )

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (sType: SearchType, sTxt: string | undefined) => {
        const url = generateUrl(location, {
            searchType: SearchType[sType],
            searchTxt: sTxt,
        })
        navigate(url)
    }

    const getArData = (sType: SearchType | undefined | string, sTxt: string) => {
        if ((sType === undefined || sTxt === undefined) && sTxt.length < 6) {
            // Seaarch text is not large enough
            setIsLoading(false)
            return
        }
        setIsLoading(true)
        setError(undefined)

        // convert sType to enum
        const convertedType: SearchType = SearchType[sType as keyof typeof SearchType]

        if (convertedType === SearchType.Ar_guid) {
            new BillingApi()
                .costByArGuid(sTxt)
                .then((response) => {
                    setBillingRecord(response.data)
                })
                .catch((er) => setError(er.message))
        } else if (convertedType === SearchType.Batch_id) {
            new BillingApi()
                .costByBatchId(sTxt)
                .then((response) => {
                    setBillingRecord(response.data)
                })
                .catch((er) => setError(er.message))
        } else {
            setIsLoading(false)
        }
    }

    const handleSearch = () => {
        if (searchByType === undefined || searchTxt === undefined || searchTxt.length < 6) {
            // Search text is not large enough
            setIsLoading(false)
            return
        }
        getArData(searchByType, searchTxt)
    }

    const handleSearchChange = (event: any, dt: any) => {
        setSearchTxt(dt.value)
    }

    const handleSearchTypeChange = (event: any, dt: any) => {
        setSearchByType(dt.value)
    }

    const handleSearchClick = () => {
        updateNav(searchByType, searchTxt)
        handleSearch()
    }

    const getDefaultSearchType = () => {
        if (searchByType !== undefined) {
            return searchByType
        }
        return dropdownOptions[0].value
    }

    React.useEffect(() => {
        handleSearch()
    }, [])

    const errorComponent = () => {
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

        // if no error return null
        return null
    }

    const loadingComponent = () => {
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

        // otherwise return null
        return null
    }

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
                    {/* There is a Dropdown inside the search Input control to select searchType */}
                    <Input
                        fluid
                        label={
                            <Dropdown
                                selection
                                compact
                                defaultValue={getDefaultSearchType()}
                                options={dropdownOptions}
                                onChange={handleSearchTypeChange}
                            />
                        }
                        labelPosition="right"
                        placeholder="Search..."
                        onChange={handleSearchChange}
                        value={searchTxt}
                    />
                </Grid.Column>
                <Grid.Column width={1}>
                    <Button compact onClick={() => handleSearchClick()} icon={<SearchIcon />} />
                </Grid.Column>
            </Grid>

            <br />
            <p>
                {' '}
                <br />
                Try these examples:
                <br />
                Ar guid: f5a065d2-c51f-46b7-a920-a89b639fc4ba
                <br />
                Batch id: 430604
                <br />
                Hail Batch + DataProc: 433599
                <br />
                Cromwell: ec3f961f-7e16-4fb0-a3e3-9fc93006ab42
                <br />
                Large Hail Batch with 7.5K jobs: a449eea5-7150-441a-9ffe-bd71587c3fe2
            </p>
        </Card>
    )

    // const batchGrid = (gridData: BillingTotalCostRecord) =>

    const dataComponent = () => {
        if ((data?.length || 0) > 0) {
            // only render grid if there are available cost data
            return data!.map((row, idx) => <BatchGrid key={`batch-grid-${idx}`} data={row} />)
        }

        // if valid search text and no data return return No data message
        if (!!data && !!searchByType && searchTxt?.length > 5) {
            return (
                <p style={{ textAlign: 'center', marginTop: '5px' }}>
                    <em>No data found.</em>
                </p>
            )
        }

        // otherwise prompt user to search, if not loading already
        if (!isLoading) {
            return (
                <p style={{ textAlign: 'center', marginTop: '5px' }}>
                    <em>
                        Enter a search term above, select type and press search button to get
                        started.
                    </em>
                </p>
            )
        }

        // otherwise do not render anything
        return null
    }

    return (
        <>
            {searchCard()}
            {errorComponent()}
            {loadingComponent()}
            {dataComponent()}
        </>
    )
}

export default BillingCostByAnalysis
