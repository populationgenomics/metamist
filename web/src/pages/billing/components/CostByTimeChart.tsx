import * as React from 'react'

import {
    IStackedAreaByDateChartData,
    StackedAreaByDateChart,
} from '../../../shared/components/Graphs/StackedAreaByDateChart'
import LoadingDucks from '../../../shared/components/LoadingDucks/LoadingDucks'

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
    if (isLoading) {
        return (
            <div>
                <LoadingDucks />
            </div>
        )
    }

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

export default React.memo(CostByTimeChart)
