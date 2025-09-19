import * as Plot from '@observablehq/plot'
import Report from '../../components/Report'
import { ReportItemPlot, ReportItemTable } from '../../components/ReportItem'
import ReportRow from '../../components/ReportRow'

const ROW_HEIGHT = 450

const PROCESS_DURATION_QUERY = `
    with times as (
        select
            s.sample_id,
            cast(s.participant_id as string) as participant_id,
            p.external_id as participant_portal_id,
            coalesce(s.meta_processing_site, 'unknown') as processing_site,
            coalesce(s.meta_collection_event_type, 'unknown') as event_type,
            try_strptime(meta_collection_datetime, '%Y-%m-%dT%H:%M:%S') as collection_time,
            dayname(try_strptime(meta_collection_datetime, '%Y-%m-%dT%H:%M:%S')) as day,
            meta_ancestry_participant_ancestry as ancestry,
            s.external_id as sample_agd_id,
            -- The most important sample derivative to track the processing time of is PBMC
            try_strptime((
                select
                    meta_processing_end_datetime
                from sample ss
                where ss.sample_root_id = s.sample_id
                and type = 'pbmc'
                limit 1
            ), '%Y-%m-%dT%H:%M:%S') as process_end_time
        from sample s
        join participant p
        on p.participant_id = s.participant_id
        where s.type = 'blood'
    ) select
        *,
        date_diff(
            'hour',
            collection_time,
            process_end_time
        ) as duration
    from times
`

function ProcessingTimesByCollectionDay(props: {
    eventType: string
    eventTypeTitle: string
    project: string
}) {
    return (
        <ReportItemPlot
            height={ROW_HEIGHT}
            flexBasis={480}
            flexGrow={1}
            title={`${props.eventTypeTitle} processing times by collection day`}
            description="Processing time by the day the sample was collected"
            project={props.project}
            query={[
                {
                    name: 'durations',
                    query: PROCESS_DURATION_QUERY,
                },
                {
                    name: 'result',
                    query: `
                        select
                            count(distinct participant_id) as count,
                            day,
                            CASE
                                WHEN duration < 24 THEN '0-24 hours'
                                WHEN duration >= 24 AND duration < 36 THEN '24-36 hours'
                                WHEN duration >= 36 AND duration < 48 THEN '36-48 hours'
                                WHEN duration >= 48 AND duration < 72 THEN '48-72 hours'
                                ELSE '72+ hours'
                            END AS duration,
                            event_type
                        from durations
                        where event_type = '${props.eventType}'
                        and duration is not null
                        group by 2, 3, 4 order by 3, 2, 4
                    `,
                },
            ]}
            plot={(data) => ({
                marginTop: 20,
                marginRight: 0,
                marginBottom: 40,
                marginLeft: 0,
                color: {
                    scheme: 'RdYlGn',
                    legend: true,
                    reverse: true,
                },
                x: {
                    axis: null,
                },
                y: {
                    axis: null,
                },
                fx: {
                    domain: [
                        'Monday',
                        'Tuesday',
                        'Wednesday',
                        'Thursday',
                        'Friday',
                        'Saturday',
                        'Sunday',
                    ],
                },
                marks: [
                    Plot.frame({ stroke: '#ccc' }),
                    Plot.barY(data, {
                        x: 'duration',
                        y: 'count',
                        fill: 'duration',
                        tip: true,
                        fx: 'day',
                    }),
                ],
            })}
        />
    )
}

function ProcessingTimesByAncestry(props: { project: string }) {
    return (
        <ReportItemPlot
            height={ROW_HEIGHT}
            flexBasis={480}
            flexGrow={1}
            title={`Processing times by ancestry`}
            description="Processing time by the ancestry of the participant"
            project={props.project}
            query={[
                {
                    name: 'durations',
                    query: PROCESS_DURATION_QUERY,
                },
                {
                    name: 'result',
                    query: `
                        select
                            count(distinct participant_id) as count,
                            CASE
                                WHEN 'Vietnamese' IN durations.ancestry THEN 'Vietnamese'
                                WHEN 'Filipino' IN durations.ancestry THEN 'Filipino'
                                WHEN 'Lebanese' IN durations.ancestry THEN 'Lebanese'
                                ELSE 'Other'
                            END AS ancestry,
                            CASE
                                WHEN duration < 24 THEN '0-24 hours'
                                WHEN duration >= 24 AND duration < 36 THEN '24-36 hours'
                                WHEN duration >= 36 AND duration < 48 THEN '36-48 hours'
                                WHEN duration >= 48 AND duration < 72 THEN '48-72 hours'
                                ELSE '72+ hours'
                            END AS duration,
                        from durations
                        where duration is not null
                        group by 2, 3 order by 2, 3
                    `,
                },
            ]}
            plot={(data) => ({
                marginTop: 20,
                marginRight: 100,
                marginBottom: 40,
                marginLeft: 100,
                color: {
                    scheme: 'RdYlGn',
                    legend: true,
                    reverse: true,
                },
                x: {
                    axis: null,
                },
                y: {
                    axis: null,
                },
                fx: {
                    domain: ['Vietnamese', 'Filipino', 'Lebanese', 'Other'],
                },
                marks: [
                    Plot.frame({ stroke: '#ccc' }),
                    Plot.barY(data, {
                        x: 'duration',
                        y: 'count',
                        fill: 'duration',
                        tip: true,
                        fx: 'ancestry',
                    }),
                ],
            })}
        />
    )
}

export default function ProcessingTimes({ project }: { project: string }) {
    return (
        <Report>
            <ReportRow>
                <ReportItemPlot
                    height={ROW_HEIGHT + 100}
                    flexGrow={1}
                    title="Processing times for all samples by processing site"
                    description="Time from collection to completion of PBMC processing. This excludes any samples with > 100 hour processing times, and any that have missing or malformed collection or processing times"
                    project={project}
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
                        marginBottom: 40,
                        color: { legend: true },

                        marginLeft: 40,
                        marks: [
                            Plot.ruleY([24, 48, 72], { stroke: '#ff725c' }),
                            Plot.dot(data, {
                                x: 'collection_time',
                                y: 'duration',
                                stroke: null,
                                fill: 'processing_site',
                                fillOpacity: 0.5,
                                channels: {
                                    process_end: 'process_end_time',
                                    sample_id: 'sample_id',
                                    sample_agd_id: 'sample_agd_id',
                                    participant_id: 'participant_id',
                                    participant_portal_id: 'participant_portal_id',
                                },
                                tip: true,
                            }),
                        ],
                    })}
                />
            </ReportRow>
            <ReportRow>
                <ReportItemPlot
                    height={ROW_HEIGHT + 100}
                    flexGrow={1}
                    title="Processing times for all samples by event type"
                    description="Time from collection to completion of PBMC processing. This excludes any samples with > 100 hour processing times, and any that have missing or malformed collection or processing times"
                    project={project}
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
                        marginBottom: 40,
                        color: { legend: true },

                        marginLeft: 40,
                        marks: [
                            Plot.ruleY([24, 48, 72], { stroke: '#ff725c' }),
                            Plot.dot(data, {
                                x: 'collection_time',
                                y: 'duration',
                                stroke: null,
                                fill: 'event_type',
                                fillOpacity: 0.5,
                                channels: {
                                    process_end: 'process_end_time',
                                    sample_id: 'sample_id',
                                    sample_agd_id: 'sample_agd_id',
                                    participant_id: 'participant_id',
                                    participant_portal_id: 'participant_portal_id',
                                },
                                tip: true,
                            }),
                        ],
                    })}
                />
            </ReportRow>

            <ReportRow>
                <ProcessingTimesByCollectionDay
                    eventType="walk-in"
                    eventTypeTitle="Walk in"
                    project={project}
                />
                <ProcessingTimesByCollectionDay
                    eventType="one-stop-shop"
                    eventTypeTitle="One stop shop"
                    project={project}
                />
            </ReportRow>
            <ReportRow>
                <ReportItemPlot
                    height={ROW_HEIGHT}
                    flexGrow={1}
                    title="Processing time buckets"
                    description="Count of participants with their samples processed in each bucket"
                    project={project}
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
                                    WHEN duration is null THEN 'unknown'
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
                <ReportItemPlot
                    height={ROW_HEIGHT}
                    flexGrow={1}
                    title="Processing time by site"
                    description="Count of participants processed in each time bucket by processing site"
                    project={project}
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
                                    WHEN duration is null then 'unknown'
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
            </ReportRow>
            <ReportRow>
                <ProcessingTimesByAncestry project={project} />
            </ReportRow>
            <ReportRow>
                <ReportItemTable
                    height={ROW_HEIGHT + 200}
                    flexGrow={1}
                    flexBasis={300}
                    title="All sample processing times"
                    project={project}
                    showToolbar={true}
                    query={[
                        { name: 'durations', query: PROCESS_DURATION_QUERY },
                        {
                            name: 'result',
                            query: `
                                select
                                    sample_id,
                                    sample_agd_id,
                                    participant_id,
                                    participant_portal_id,
                                    processing_site,
                                    event_type,
                                    day as collection_day,
                                    strftime(collection_time, '%Y-%m-%d %H:%M:%S') as collection_time,
                                    strftime(process_end_time, '%Y-%m-%d %H:%M:%S') as process_end_time,
                                    duration,
                                    array_to_string(ancestry, ', ') as ancestry
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
                    project={project}
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
