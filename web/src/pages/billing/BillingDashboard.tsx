import * as React from 'react'
import { Card } from 'semantic-ui-react'
import SeqrProportionalMapGraph from './SeqrProportionalMapGraph'

const BillingDashboard: React.FunctionComponent = () => {
    const [start, setStart] = React.useState<string>('2021-01-01')
    const [end, setEnd] = React.useState<string>('2022-10-31')

    return (
        <Card fluid style={{ padding: "20px" }}>
            <div
                style={{
                    marginTop: 20,
                    paddingTop: 20,
                    marginBottom: 20,
                }}
            >
                <div
                    style={{
                        fontSize: 50,
                        marginLeft: 20,
                    }}
                >
                    Billing Dashboard
                </div>
                <br />
            </div>
            <SeqrProportionalMapGraph start={start} end={end} />
        </Card>
    )
}

export default BillingDashboard
