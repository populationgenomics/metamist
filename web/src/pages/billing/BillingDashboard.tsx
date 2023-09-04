import * as React from 'react'
import { Card } from 'semantic-ui-react'
import SeqrProportionalMapGraph from './SeqrProportionalMapGraph'

const BillingDashboard: React.FunctionComponent = () => {
    const [start, setStart] = React.useState<string>('2021-01-01')
    const [end, setEnd] = React.useState<string>('2021-12-31')

    return (
        <Card fluid>
            <h3>Billing dashboard</h3>
            <SeqrProportionalMapGraph start={start} end={end} />
        </Card>
    )
}

export default BillingDashboard
