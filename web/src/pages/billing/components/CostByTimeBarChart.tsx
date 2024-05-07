import * as React from 'react'
import _ from 'lodash'

import LoadingDucks from '../../../shared/components/LoadingDucks/LoadingDucks'
import { StackedBarChart } from '../../../shared/components/Graphs/StackedBarChart'
import { BillingCostBudgetRecord } from '../../../sm-api'

interface ICostByTimeBarChartProps {
    accumulate: boolean
    isLoading: boolean
    data: BillingCostBudgetRecord[]
}

const CostByTimeBarChart: React.FunctionComponent<ICostByTimeBarChartProps> = ({
    accumulate,
    isLoading,
    data,
}) => {
    if (isLoading) {
        return (
            <div>
                <LoadingDucks />
                <p style={{ textAlign: 'center', marginTop: '5px' }}>
                    <em>This query takes a while...</em>
                </p>
            </div>
        )
    }

    return (
        <>
            <StackedBarChart data={data} accumulate={accumulate} isLoading={isLoading} />
        </>
    )
}

export default CostByTimeBarChart
