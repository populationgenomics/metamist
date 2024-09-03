import * as React from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { Card, Grid, Input } from 'semantic-ui-react'
import generateUrl from '../../shared/utilities/generateUrl'
import { getMonthEndDate } from '../../shared/utilities/monthStartEndDate'
import SeqrProportionalMapGraph from './components/SeqrProportionalMapGraph'

const BillingSeqrProp: React.FunctionComponent = () => {
    const now = new Date()
    const [start, setStart] = React.useState<string>(`${now.getFullYear()}-01-01`)
    const [end, setEnd] = React.useState<string>(getMonthEndDate())

    // use navigate and update url params
    const location = useLocation()
    const navigate = useNavigate()

    const updateNav = (st: string, ed: string) => {
        const url = generateUrl(location, {
            start: st,
            end: ed,
        })
        navigate(url)
    }

    /* eslint-disable react-hooks/exhaustive-deps */
    React.useEffect(() => {
        updateNav(start, end)
    }, [start, end])
    /* eslint-enable react-hooks/exhaustive-deps */

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
