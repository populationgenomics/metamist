import * as React from 'react'
import { Card } from 'semantic-ui-react'
import SeqrProportionalMapGraph from './SeqrProportionalMapGraph'

const BillingDashboard: React.FunctionComponent = () => {
    const [start, setStart] = React.useState<string>('2021-01-01')
    const [end, setEnd] = React.useState<string>('2022-10-31')

    return (
        <Card fluid style={{ backgroundColor: '#EFECEA' }}>
            <div
                style={{
                    backgroundColor: '#F5F3F2',
                    marginTop: 20,
                    paddingTop: 20,
                    marginBottom: 20,
                }}
            >
                <div
                    style={{
                        fontSize: 50,
                        marginLeft: 20,
                        color: '#635F5D',
                    }}
                >
                    Billing Dashboard
                </div>
                <br />
                <div
                    style={{
                        fontSize: 24,
                        marginBottom: 10,
                        marginLeft: 20,
                        marginTop: 5,
                        color: '#8E8883',
                        fontStyle: 'italic',
                    }}
                >
                    Breakdown of proportional allocation of costs per project over time
                </div>
            </div>
            <SeqrProportionalMapGraph start={start} end={end} />
        </Card>
    )
}

export default BillingDashboard
