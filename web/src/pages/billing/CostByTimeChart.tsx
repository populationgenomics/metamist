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
    groups: string[]
    isLoading: boolean
    data: IStackedAreaByDateChartData[]
}

const CostByTimeChart: React.FunctionComponent<ICostByTimeChartProps> = ({
    start,
    end,
    groups,
    isLoading,
    data,
}) => {

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
                xLabel=""
                yLabel="Cost (AUD)"
                seriesLabel="Service"
                extended={false}
                showDate={true}
            />
        </>
    )
}

export default CostByTimeChart
