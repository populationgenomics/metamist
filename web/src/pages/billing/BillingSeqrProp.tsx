import * as React from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { Grid, Card, Input } from 'semantic-ui-react'
import SeqrProportionalMapGraph from './components/SeqrProportionalMapGraph'

const BillingSeqrProp: React.FunctionComponent = () => {
    const now = new Date()
    const [start, setStart] = React.useState<string>(`${now.getFullYear()}-01-01`)
    const [end, setEnd] = React.useState<string>(
        `${now.getFullYear()}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now
            .getDate()
            .toString()
            .padStart(2, '0')}`
    )

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (start: string, end: string) => {
        let url = `${location.pathname}`
        if (start || end) url += '?'

        let params: string[] = []
        if (start) params.push(`start=${start}`)
        if (end) params.push(`end=${end}`)

        url += params.join('&')
        navigate(url)
    }

    React.useEffect(() => {
        updateNav(start, end)
    }, [start, end])

    return (
        <Card fluid style={{ padding: '20px' }} id="billing-container">
            <h1
                style={{
                    fontSize: 40,
                }}
            >
                Billing Seqr Proportionate Map over Time
            </h1>
            <Grid columns="equal">
                <Grid.Column stretched>
                    <Input
                        label="Start"
                        type="date"
                        onChange={(e) => setStart(e.target.value)}
                        value={start}
                    />
                </Grid.Column>
                <Grid.Column stretched>
                    <Input
                        label="Finish"
                        type="date"
                        onChange={(e) => setEnd(e.target.value)}
                        value={end}
                    />
                </Grid.Column>
            </Grid>
            <SeqrProportionalMapGraph start={start} end={end} />
        </Card>
    )
}

export default BillingSeqrProp
