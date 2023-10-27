import * as React from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { Card, Grid, Input } from 'semantic-ui-react'
import CostByTimeChart from './CostByTimeChart'
import FieldSelector from './FieldSelector'
import { BillingColumn } from '../../sm-api'

import { convertFieldName } from '../../shared/utilities/fieldName'

const BillingCostByTime: React.FunctionComponent = () => {
    const now = new Date()
    const [start, setStart] = React.useState<string>(`${now.getFullYear()}-03-01`)
    const [end, setEnd] = React.useState<string>(`${now.getFullYear()}-03-05`)
    // TODO once we have more data change to the current month
    // (
    //     `${now.getFullYear()}-${now.getMonth() + 1}-${now.getDate()}`
    // )

    const [searchParams] = useSearchParams()

    const inputGroupBy: string | undefined = searchParams.get('groupBy') ?? undefined
    const fixedGroupBy: BillingColumn = inputGroupBy ? inputGroupBy as BillingColumn : BillingColumn.GcpProject
    const inputSelectedData: string | undefined = searchParams.get('selectedData') ?? undefined

    const [groupBy, setGroupBy] = React.useState<BillingColumn>(fixedGroupBy ?? BillingColumn.GcpProject)
    const [selectedData, setSelectedData] = React.useState<string | undefined>(inputSelectedData)

    // use navigate and update url params
    const location = useLocation();
    const navigate = useNavigate();

    const updateNav = (grp: string | undefined, data: string | undefined) => {
        let url = `${location.pathname}`
        if (grp || data) url += '?'

        let params: string[] = []
        if (grp) params.push(`groupBy=${grp}`)
        if (data) params.push(`selectedData=${data}`)

        url += params.join('&')
        navigate(url)
    }

    const onGroupBySelect = (event: any, data: any) => {
        setGroupBy(data.value)
        setSelectedData(undefined)
        updateNav(data.value, undefined)
    }

    const onSelect = (event: any, data: any) => {
        setSelectedData(data.value)
        updateNav(groupBy, data.value)
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
            <div>
                <Grid columns='equal'>
                    <Grid.Column>
                        <FieldSelector
                            label="Group By"
                            fieldName="Group"
                            onClickFunction={onGroupBySelect}
                            selected={groupBy}
                        />
                    </Grid.Column>

                    <Grid.Column>
                        <FieldSelector
                            label={convertFieldName(groupBy)}
                            fieldName={groupBy}
                            onClickFunction={onSelect}
                            selected={selectedData}
                            includeAll={true}
                        />
                    </Grid.Column>
                </Grid>

                <Grid columns='equal'>
                    <Grid.Column className="field-selector-label">
                        <Input
                            label="Start"
                            fluid
                            type="date"
                            onChange={(e) => setStart(e.target.value)}
                            value={start}
                        />
                    </Grid.Column>

                    <Grid.Column className="field-selector-label">
                        <Input
                            label="Finish"
                            fluid
                            type="date"
                            onChange={(e) => setEnd(e.target.value)}
                            value={end}
                        />
                    </Grid.Column>
                </Grid>
            </div>
            <CostByTimeChart
                start={start}
                end={end}
                groupBy={groupBy}
                selectedGroup={selectedData}
            />
        </Card >
    )
}

export default BillingCostByTime
