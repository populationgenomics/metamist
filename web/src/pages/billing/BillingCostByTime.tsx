import * as React from 'react'
import { useSearchParams } from 'react-router-dom'
import { Card, Input } from 'semantic-ui-react'
import CostByTimeChart from './CostByTimeChart'
import FieldSelector from './FieldSelector'

const BillingCostByTime: React.FunctionComponent = () => {
    const now = new Date()
    const [start, setStart] = React.useState<string>(`${now.getFullYear()}-03-01`)
    const [end, setEnd] = React.useState<string>(`${now.getFullYear()}-03-05`)
    // TODO once we have more data change to the current month
    // (
    //     `${now.getFullYear()}-${now.getMonth() + 1}-${now.getDate()}`
    // )

    const [searchParams] = useSearchParams()

    const inGroupBy = searchParams.get('groupBy')
    const inSelectedGroup = searchParams.get('selectedGroup')

    const [groupBy, setGroupBy] = React.useState<string | null>(inGroupBy)

    const [selectedGroup, setSelectedGroup] = React.useState<string | null>(inSelectedGroup)

    const onGroupBySelect = (event: any, data: any) => {
        setGroupBy(data.value)
        setSelectedGroup(null)
    }

    const onSelect = (event: any, data: any) => {
        setSelectedGroup(data.value)
    }

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
                    Billing Cost By Time
                </div>
                <br />
            </div>
            <form style={{ maxWidth: '100%' }}>
                <FieldSelector
                    label="Group By"
                    fieldName="Group"
                    onClickFunction={onGroupBySelect}
                    selected={groupBy}
                />

                <FieldSelector
                    label={groupBy}
                    fieldName={groupBy}
                    onClickFunction={onSelect}
                    selected={selectedGroup}
                    includeAll={true}
                />

                <table>
                    <tr>
                        <td className="field-selector-label">
                            <h3>Start</h3>
                        </td>
                        <td>
                            <Input
                                type="date"
                                onChange={(e) => setStart(e.target.value)}
                                value={start}
                            />
                        </td>
                        <td className="field-selector-label"></td>
                        <td className="field-selector-label">
                            <h3>Finish</h3>
                        </td>
                        <td>
                            <Input
                                type="date"
                                onChange={(e) => setEnd(e.target.value)}
                                value={end}
                            />
                        </td>
                    </tr>
                </table>
            </form>
            <CostByTimeChart
                start={start}
                end={end}
                groupBy={groupBy}
                selectedGroup={selectedGroup}
            />
        </Card>
    )
}

export default BillingCostByTime
