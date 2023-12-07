import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Header, Button, Card, Grid, Input, Message, Table as SUITable } from 'semantic-ui-react'
import SearchIcon from '@mui/icons-material/Search'
import Table from '../../shared/components/Table'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import {
    BillingApi,
    BillingColumn,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
} from '../../sm-api'

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

const BillingCostByBatch: React.FunctionComponent = () => {
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

    const [data, setData] = React.useState<BillingTotalCostRecord[]>([])

    const [searchTxt, setSearchTxt] = React.useState<string | undefined>(
        searchParams.get('searchBy') ?? undefined
    )

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (searchBy: string | undefined) => {
        let url = `${location.pathname}`
        if (searchBy) {
            url += `?searchBy=${searchBy}`
            navigate(url)
        }
    }

    const getData = (query: BillingTotalCostQueryModel) => {
        setIsLoading(true)
        setError(undefined)
        new BillingApi()
            .getTotalCost(query)
            .then((response) => {
                setIsLoading(false)
                setData(response.data)
            })
            .catch((er) => setError(er.message))
    }

    const handleSearch = () => {
        if (searchTxt === undefined || searchTxt.length < 6) {
            // Seaarch text is not large enough
            setIsLoading(false)
            return
        }
        updateNav(searchTxt)
        getData({
            fields: [
                BillingColumn.CostCategory,
                BillingColumn.Sku,
                BillingColumn.ArGuid,
                BillingColumn.BatchId,
                BillingColumn.CromwellWorkflowId,
                BillingColumn.SequencingGroup,
            ],
            start_date: start,
            end_date: end,
            filters: {
                ar_guid: searchTxt,
                batch_id: searchTxt,
                sequencing_group: searchTxt,
                cromwell_workflow_id: searchTxt,
            },
            filters_op: 'OR',
            order_by: { cost: true },
        })
    }

    const handleSearchChange = (event: any, dt: any) => {
        setSearchTxt(dt.value)
    }

    React.useEffect(() => {}, [])

    React.useEffect(() => {
        handleSearch()
    }, [searchTxt])

    const searchCard = () => {
        return (
            <Card fluid style={{ padding: '20px' }} id="billing-container">
                <h1
                    style={{
                        fontSize: 40,
                    }}
                >
                    Billing Cost By Batch
                </h1>

                <Grid columns={2} stackable>
                    <Grid.Column width={10}>
                        <Input
                            fluid
                            input={{ fluid: true }}
                            placeholder="Search by AR GUID / BATCH ID / SEQ GRP / CROMWELL ID ... min 6 letters"
                            onChange={handleSearchChange}
                            value={searchTxt}
                        />
                    </Grid.Column>
                    <Grid.Column width={2}>
                        <Button icon onClick={handleSearch}>
                            <SearchIcon />
                        </Button>
                    </Grid.Column>
                </Grid>

                <br />
                <p>
                    {' '}
                    E.g.
                    <br />
                    ar_guid: 855a6153-033c-4398-8000-46ed74c02fe8
                    <br />
                    batch_id: 429518
                    <br />
                    sequencing_group: cpg246751
                    <br />
                    cromwell_workflow_id: cromwell-e252f430-4143-47ec-a9c0-5f7face1b296
                    <br />
                </p>
            </Card>
        )
    }

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
            <Card
                fluid
                style={{ padding: '20px', overflowX: 'scroll' }}
                id="billing-container-data"
            >
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
                        {data.map((k) => (
                            <SUITable.Row>
                                <SUITable.Cell>{k.cost_category}</SUITable.Cell>
                                <SUITable.Cell>{k.sku}</SUITable.Cell>
                                <SUITable.Cell>
                                    <b>{currencyFormat(k.cost)}</b>
                                </SUITable.Cell>
                            </SUITable.Row>
                        ))}
                    </SUITable.Body>
                </Table>
            </Card>
        </>
    )
}

export default BillingCostByBatch
