import * as React from 'react'
import LoadingDucks from '../../../shared/components/LoadingDucks/LoadingDucks'
import {
    StackedBarChart,
    IStackedBarChartData,
} from '../../../shared/components/Graphs/StackedBarChart'

interface ICostByTimeBarChartProps {
    accumulate: boolean
    isLoading: boolean
    data: IStackedBarChartData[]
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
            <StackedBarChart data={data} accumulate={accumulate} />
        </>
    )
}

export default CostByTimeBarChart
