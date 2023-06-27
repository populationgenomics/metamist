import * as React from 'react'
import { ProjectSummary } from '../../sm-api/api'

interface TotalStatsProps {
    summary: Partial<ProjectSummary>
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
            <b>Total Sequencing Groups: </b>
            {'    '}
            {summary?.total_sequencing_groups}
            <br />
        </>
    )
}

export default TotalsStats
