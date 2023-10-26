import * as React from 'react'
import _ from 'lodash'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import {
    BillingApi,
    BillingColumn,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
} from '../../sm-api'
import { Message } from 'semantic-ui-react'
import {
    IStackedAreaByDateChartData,
    StackedAreaByDateChart,
} from '../../shared/components/Graphs/StackedAreaByDateChart'

interface ICostByTimeChartProps {
    start: string
    end: string
    groupBy: string
    selectedGroup: string
}

const CostByTimeChart: React.FunctionComponent<ICostByTimeChartProps> = ({
    start,
    end,
    groupBy,
    selectedGroup,
}) => {
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()

    const [data, setData] = React.useState<IStackedAreaByDateChartData[]>([])
    const [groups, setGroups] = React.useState<string[]>([])

    const getData = (query: BillingTotalCostQueryModel) => {
        setIsLoading(true)
        setError(undefined)
        new BillingApi()
            .getTotalCost(query)
            .then((response) => {
                setIsLoading(false)
                const rec_grps = _.uniq(
                    response.data.map((item: BillingTotalCostRecord) => item.cost_category)
                )
                const records = response.data.reduce(
                    (
                        acc: { [key: string]: { [key: string]: number } },
                        item: BillingTotalCostRecord
                    ) => {
                        const { day, cost_category, cost } = item
                        if (day !== undefined) {
                            if (!acc[day]) {
                                acc[day] = {}
                                rec_grps.forEach((k) => {
                                    acc[day][k] = 0
                                })
                            }
                            acc[day][cost_category] = cost
                        }
                        return acc
                    },
                    {}
                )
                setGroups(rec_grps)
                setData(
                    Object.keys(records).map((key) => ({
                        date: new Date(key),
                        values: records[key],
                    }))
                )
            })
            .catch((er) => setError(er.message))
    }

    // on first load
    React.useEffect(() => {
        if (selectedGroup !== undefined && selectedGroup !== '' && selectedGroup !== null) {
            if (selectedGroup.startsWith('All ')) {
                getData({
                    fields: [BillingColumn.Day, BillingColumn.CostCategory],
                    start_date: start,
                    end_date: end,
                    order_by: { day: false },
                })
            } else {
                getData({
                    fields: [BillingColumn.Day, BillingColumn.CostCategory],
                    start_date: start,
                    end_date: end,
                    filters: { [groupBy.replace('-', '_').toLowerCase()]: selectedGroup },
                    order_by: { day: false },
                })
            }
        }
    }, [start, end, groupBy, selectedGroup])

    if (isLoading)
        return (
            <div>
                <LoadingDucks />
                <p style={{ textAlign: 'center', marginTop: '5px' }}>
                    <em>This query takes a while...</em>
                </p>
            </div>
        )

    return (
        <>
            <StackedAreaByDateChart
                keys={groups}
                data={data}
                start={new Date(start)}
                end={new Date(end)}
                isPercentage={false}
                yLabel="Cost (AUD)"
                seriesLabel="Service"
            />
        </>
    )
}

export default CostByTimeChart
