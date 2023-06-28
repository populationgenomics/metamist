import * as React from 'react'
import { ProjectSummaryResponse } from '../../sm-api/api'

interface TotalStatsProps {
    summary: Partial<ProjectSummaryResponse>
}

const TotalsStats: React.FunctionComponent<TotalStatsProps> = ({ summary }) => {
    const _summary = Object.entries(summary)
    if (!_summary.length) return <></>

    return (
        <>
            <h2>Project Summary Stats</h2>
            <b>Total Participants: </b>
            {summary?.total_participants}
            <br />
            <b>Total Samples: </b>
            {'    '}
            {summary?.total_samples}
            <br />
            <b>Total Sequences: </b>
            {'    '}
            {summary?.total_sequences}
            <br />
        </>
    )
}

export default TotalsStats
