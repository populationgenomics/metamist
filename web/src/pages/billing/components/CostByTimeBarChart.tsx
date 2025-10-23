import * as React from 'react'
import {
    IStackedBarChartData,
    StackedBarChart,
} from '../../../shared/components/Graphs/StackedBarChart'
import LoadingDucks from '../../../shared/components/LoadingDucks/LoadingDucks'

interface ICostByTimeBarChartProps {
    accumulate: boolean
    isLoading: boolean
    data: IStackedBarChartData[]
    extrapolate?: boolean
}

const CostByTimeBarChart: React.FunctionComponent<ICostByTimeBarChartProps> = ({
    accumulate,
    isLoading,
    data,
    extrapolate=true,
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

    console.log(data)
    return (
        <>
            <StackedBarChart data={data} accumulate={accumulate} extrapolate={extrapolate} />
        </>
    )
}

export default CostByTimeBarChart
