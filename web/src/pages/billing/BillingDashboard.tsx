import * as React from 'react'
import { Card, Input } from 'semantic-ui-react'
import SeqrProportionalMapGraph from './SeqrProportionalMapGraph'

const BillingDashboard: React.FunctionComponent = () => {
    const now = new Date()
    const [start, setStart] = React.useState<string>(`${now.getFullYear()}-01-01`)
    const [end, setEnd] = React.useState<string>(
        `${now.getFullYear()}-${now.getMonth() + 1}-${now.getDate()}`
    )

    return (
        <Card fluid style={{ padding: '20px' }} id="billing-container">
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
            <form style={{ maxWidth: '100px' }}>
                Start
                <Input type="date" onChange={(e) => setStart(e.target.value)} value={start} />
                Finish
                <Input type="date" onChange={(e) => setEnd(e.target.value)} value={end} />
            </form>
            <SeqrProportionalMapGraph start={start} end={end} />
        </Card>
    )
}

export default BillingDashboard
