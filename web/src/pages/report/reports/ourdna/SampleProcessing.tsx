import * as Plot from '@observablehq/plot'
import Report from '../../components/Report'
import { ReportItemPlot, ReportItemTable } from '../../components/ReportItem'
import ReportRow from '../../components/ReportRow'

const ROW_HEIGHT = 450
const PROJECT = 'ourdna'

const PROCESS_DURATION_QUERY = `
    select
        s.sample_id,
        cast(s.participant_id as string) as participant_id,
        coalesce(s."meta_processing-site", 'Unknown') as processing_site,

        try_strptime(nullif("meta_collection-time", ' '), '%Y-%m-%d %H:%M:%S') as collection_time,
        try_strptime(nullif("meta_process-end-time", ' '), '%Y-%m-%d %H:%M:%S') as process_end_time,
        date_diff(
            'hour',
            try_strptime(nullif("meta_collection-time", ' '), '%Y-%m-%d %H:%M:%S'),
            try_strptime(nullif("meta_process-end-time", ' '), '%Y-%m-%d %H:%M:%S')
        ) as duration
    from sample s
    join participant p
    on p.participant_id = s.participant_id
    where s.type = 'blood'
`

export default function ProcessingTimes() {
    return (
        <Report>
            <ReportRow>
                <ReportItemPlot
                    height={ROW_HEIGHT}
                    flexGrow={1}
                    title="Processing times for all samples"
                    description="This excludes any samples with > 100 hour processing times, and any that have missing or malformed collection or processing times"
                    project={PROJECT}
                    query={[
                        {
                            name: 'durations',
                            query: PROCESS_DURATION_QUERY,
                        },
                        {
                            name: 'result',
                            query: `
                                select * from durations where duration < 100 and duration > 0
                            `,
                        },
                    ]}
                    plot={(data) => ({
                        y: { grid: true, padding: 5, label: 'duration(hrs)' },
                        x: { grid: true, padding: 5 },
                        marginTop: 20,
                        marginRight: 20,
                        marginBottom: 30,

                        marginLeft: 40,
                        marks: [
                            Plot.ruleY([24, 48, 72], { stroke: '#ff725c' }),
                            Plot.dot(data, {
                                x: 'collection_time',
                                y: 'duration',
                                stroke: null,
                                fill: '#4269d0',
                                fillOpacity: 0.5,
                                channels: {
                                    process_end: 'process_end_time',
                                    sample_id: 'sample_id',
                                    participant_id: 'participant_id',
                                },
                                tip: true,
                            }),
                        ],
                    })}
                />

                <ReportItemPlot
                    height={ROW_HEIGHT}
                    flexGrow={1}
                    title="Processing time buckets"
                    description="Count of participants with their samples processed in each bucket"
                    project={PROJECT}
                    query={[
                        { name: 'durations', query: PROCESS_DURATION_QUERY },
                        {
                            name: 'result',
                            query: `
                            select
                                count(distinct participant_id) as count,
                                CASE
                                    WHEN duration < 24 THEN '0-24 hours'
                                    WHEN duration >= 24 AND duration < 36 THEN '24-36 hours'
                                    WHEN duration >= 36 AND duration < 48 THEN '36-48 hours'
                                    WHEN duration >= 48 AND duration < 72 THEN '48-72 hours'
                                    ELSE '72+ hours'
                                END AS duration
                            from durations group by 2 order by 2
                        `,
                        },
                    ]}
                    plot={(data) => ({
                        marginLeft: 100,
                        inset: 10,
                        color: {
                            scheme: 'RdYlGn',
                            reverse: true,
                        },
                        marks: [
                            Plot.barX(data, {
                                y: 'duration',
                                x: 'count',
                                tip: true,
                                fill: 'duration',
                            }),
                        ],
                    })}
                />
            </ReportRow>
            <ReportRow>
                <ReportItemPlot
                    height={ROW_HEIGHT}
                    flexGrow={1}
                    flexBasis={400}
                    title="Processing time by site"
                    description="Count of participants processed in each time bucket by processing site"
                    project={PROJECT}
                    query={[
                        { name: 'durations', query: PROCESS_DURATION_QUERY },
                        {
                            name: 'result',
                            query: `
                            select
                                count(distinct participant_id) as count,
                                processing_site,
                                CASE
                                    WHEN duration < 24 THEN '0-24 hours'
                                    WHEN duration >= 24 AND duration < 36 THEN '24-36 hours'
                                    WHEN duration >= 36 AND duration < 48 THEN '36-48 hours'
                                    WHEN duration >= 48 AND duration < 72 THEN '48-72 hours'
                                    ELSE '72+ hours'
                                END AS duration
                            from durations group by 2,3 order by 2,3
                        `,
                        },
                    ]}
                    plot={(data) => ({
                        marginLeft: 100,
                        marginRight: 100,
                        inset: 10,
                        color: {
                            scheme: 'RdYlGn',
                            reverse: true,
                        },
                        marks: [
                            Plot.barX(data, {
                                y: 'processing_site',
                                fy: 'duration',
                                x: 'count',
                                tip: true,
                                fill: 'duration',
                            }),
                        ],
                    })}
                />

                <ReportItemTable
                    height={ROW_HEIGHT}
                    flexGrow={1}
                    flexBasis={300}
                    title="All sample processing times"
                    project={PROJECT}
                    showToolbar={true}
                    query={[
                        { name: 'durations', query: PROCESS_DURATION_QUERY },
                        {
                            name: 'result',
                            query: `
                                select
                                    sample_id,
                                    participant_id,
                                    processing_site,
                                    strftime(collection_time, '%Y-%m-%d %H:%M:%S') as collection_time,
                                    strftime(process_end_time, '%Y-%m-%d %H:%M:%S') as process_end_time,
                                    duration
                                from durations
                                order by duration desc nulls last
                            `,
                        },
                    ]}
                />
            </ReportRow>
            <ReportRow>
                <ReportItemPlot
                    height={ROW_HEIGHT}
                    flexGrow={1}
                    flexBasis={400}
                    title="Sample types"
                    description="Count of participants for each sample type"
                    project={PROJECT}
                    query={[
                        {
                            name: 'result',
                            query: `
                                select
                                    count(distinct participant_id) as count,
                                    type
                                from sample s
                                group by 2
                            `,
                        },
                    ]}
                    plot={(data) => ({
                        marginLeft: 100,
                        marks: [
                            Plot.barX(data, {
                                y: 'type',
                                x: 'count',
                                fill: 'type',
                                tip: true,
                            }),
                        ],
                    })}
                />
            </ReportRow>
        </Report>
    )
}
