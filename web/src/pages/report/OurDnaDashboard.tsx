import { Box, Typography } from '@mui/material'
import * as Plot from '@observablehq/plot'
import { MetricFromQueryCard } from './chart/MetricFromQuery'
import { PlotFromQueryCard } from './chart/PlotFromQuery'
import { TableFromQueryCard } from './chart/TableFromQuery'

/*

other charts:

- chart of samples where we don't have consent / registration information for a participant.
    - could be a few reasons, but want to capture badly entered participant ids


    sections:
    - recruitment
    - processing times

    - demographics

    - data quality

*/

// Common queries:
// queries that are used in multiple charts

// Query for processing times, this can be used for most visualistions
// of processing time
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

export default function OurDnaDashboard() {
    return (
        <Box>
            <Box m={2}>
                <Typography fontWeight={'bold'} fontSize={26}>
                    Recruitment
                </Typography>
            </Box>
            <Box display={'flex'} gap={2} m={2}>
                <Box width={'50%'}>
                    <PlotFromQueryCard
                        title="Recruitment funnel"
                        description="The number of participants at each stage of recruitment"
                        project="ourdna"
                        query={`
                            with funnel as (
                                select
                                    count(distinct p.participant_id) as "All Participants",
                                    count(distinct p.participant_id) filter (
                                        p."meta_blood-consent" = 'yes' and p."meta_informed-consent" = 'yes'
                                    ) as "Participants consented",
                                    count(distinct p.participant_id) filter (s.sample_id is not null) as "Participants with samples"
                                from participant p
                                left join sample s
                                on s.participant_id = p.participant_id
                            ) unpivot funnel
                            on columns (*)
                            into name stage value count
                        `}
                        plot={(data, { width }) =>
                            Plot.plot({
                                marginLeft: 160,
                                inset: 10,
                                height: 170,
                                width,
                                marks: [
                                    Plot.barX(data, {
                                        y: 'stage',
                                        x: 'count',
                                        tip: true,
                                        fill: 'stage',
                                    }),
                                ],
                            })
                        }
                    />
                </Box>
                <Box width={'50%'}>
                    <PlotFromQueryCard
                        title="Recruitment types"
                        description="How the participant was recruited"
                        project="ourdna"
                        query={`
                            select
                                count(distinct participant_id) as count,
                                CASE coalesce("meta_collection-event-type", '__NULL__')
                                    WHEN 'POE'
                                        THEN 'POE?'

                                    WHEN 'Walk-in'
                                        THEN 'Walk in'
                                    WHEN 'Walk In'
                                        THEN 'Walk in'

                                    WHEN 'OSS'
                                        THEN 'Once Stop Shop'
                                    
                                    WHEN '__NULL__'
                                        THEN 'Unknown'

                                    ELSE "meta_collection-event-type"

                                END as collection_type
                            from sample s
                            group by 2
                        `}
                        plot={(data, { width }) =>
                            Plot.plot({
                                width,
                                height: 150,
                                color: { legend: true },
                                marks: [
                                    Plot.barX(
                                        data,
                                        Plot.stackX({
                                            x: 'count',
                                            fill: 'collection_type',
                                            inset: 0.5,
                                            tip: true,
                                        })
                                    ),
                                ],
                            })
                        }
                    />
                </Box>
            </Box>

            <Box m={2}>
                <Typography fontWeight={'bold'} fontSize={26}>
                    Processing times
                </Typography>
            </Box>
            <Box display={'flex'} gap={2} m={2}>
                <Box width={'60%'}>
                    <PlotFromQueryCard
                        title="Processing times for all samples"
                        description="This excludes any samples with > 100 hour processing times, and any that have missing or malformed collection or processing times"
                        project="ourdna"
                        queries={[
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
                        plot={(data, { width }) =>
                            Plot.plot({
                                y: { grid: true, padding: 5, label: 'duration(hrs)' },
                                x: { grid: true, padding: 5 },
                                width: width,
                                height: 400,
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
                            })
                        }
                    />
                </Box>
                <Box width={'40%'}>
                    <PlotFromQueryCard
                        title="Processing time buckets"
                        description="Count of participants with their samples processed in each bucket"
                        project="ourdna"
                        queries={[
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
                        plot={(data, { width }) =>
                            Plot.plot({
                                marginLeft: 100,
                                inset: 10,
                                height: 400,
                                color: {
                                    scheme: 'RdYlGn',
                                    reverse: true,
                                },
                                width,
                                marks: [
                                    Plot.barX(data, {
                                        y: 'duration',
                                        x: 'count',
                                        tip: true,
                                        fill: 'duration',
                                    }),
                                ],
                            })
                        }
                    />
                </Box>
            </Box>
            <Box display={'flex'} gap={2} m={2}>
                <Box width={'50%'}>
                    <PlotFromQueryCard
                        title="Processing time by site"
                        description="Count of participants processed in each time bucket by processing site"
                        project="ourdna"
                        queries={[
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
                        plot={(data, { width }) =>
                            Plot.plot({
                                marginLeft: 100,
                                marginRight: 100,
                                inset: 10,
                                height: 450,
                                color: {
                                    scheme: 'RdYlGn',
                                    reverse: true,
                                },
                                width,
                                marks: [
                                    Plot.barX(data, {
                                        y: 'processing_site',
                                        fy: 'duration',
                                        x: 'count',
                                        tip: true,
                                        fill: 'duration',
                                    }),
                                ],
                            })
                        }
                    />
                </Box>

                <Box width={'50%'}>
                    <TableFromQueryCard
                        showToolbar={true}
                        project={'ourdna'}
                        title="All sample processing times"
                        queries={[
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
                        height={450}
                    />
                </Box>
            </Box>

            <Box m={2}>
                <Typography fontWeight={'bold'} fontSize={26}>
                    Demographics
                </Typography>
            </Box>

            <Box display={'flex'} gap={2} m={2}>
                <Box width={'25%'}>
                    <MetricFromQueryCard
                        title="Ratio of reported sex"
                        subtitle="For participants who have reported"
                        description="This chart shows the ratio of participants that have responded by reporting their sex"
                        project="ourdna"
                        query={`
                            select
                                count(*) filter (reported_sex = 1) as male,
                                count(*) filter (reported_sex = 2) as female
                            from participant
                        `}
                    />
                </Box>
                <Box width={'75%'}>
                    <PlotFromQueryCard
                        project="ourdna"
                        title={'Participant count by ancestry over time'}
                        queries={[
                            {
                                name: 'all_ancestries',
                                query: `
                                    select
                                        p.participant_id,
                                        "meta_collection-time" as collection_time,
                                        unnest(p."meta_ancestry-participant-ancestry") as ancestry
                                    from participant p
                                    join sample s
                                    on s.participant_id = p.participant_id
                                    where collection_time is not null
                                    and collection_time != ' '
                                `,
                            },
                            {
                                name: 'counts_by_week',
                                query: `
                                    select
                                        coalesce(count(distinct participant_id), 0) as participants,
                                        date_trunc(
                                            'week',
                                            try_strptime(
                                                nullif(
                                                    collection_time,
                                                    ' '
                                                ),
                                                '%Y-%m-%d %H:%M:%S'
                                            )
                                        ) as week,
                                        ancestry
                                    from all_ancestries p
                                    group by week, ancestry
                                `,
                            },
                            {
                                name: 'weeks',
                                query: `
                                    select
                                        unnest(generate_series(min(week), max(week), interval '1 week')) as week
                                    from
                                    counts_by_week
                                `,
                            },
                            {
                                name: 'ancestries',
                                query: `
                                    select distinct ancestry from counts_by_week
                                `,
                            },
                            {
                                name: 'filled',
                                query: `
                                    select
                                        coalesce(r.participants, 0) as participants,
                                        strftime(w.week, '%Y-%m-%d') as week,
                                        a.ancestry,
                                        
                                    from weeks w
                                    join ancestries a on true
                                    left join counts_by_week r
                                    on r.week = w.week
                                    and r.ancestry = a.ancestry
                                    order by 2, 3
                                `,
                            },
                            {
                                name: 'result',
                                query: `
                                    select
                                        (SUM(participants) over (PARTITION BY ancestry  ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as count,
                                        ancestry,
                                        week
                                    from filled
                                `,
                            },
                        ]}
                        plot={(data, { width }) =>
                            Plot.plot({
                                marginLeft: 40,
                                inset: 10,
                                width: width,
                                height: 400,
                                y: {
                                    grid: true,
                                    label: 'Participants',
                                },
                                x: {
                                    type: 'time',
                                },
                                marks: [
                                    Plot.areaY(data, {
                                        x: 'week',
                                        y: 'count',
                                        fill: 'ancestry',
                                        tip: true,
                                        order: 'sum',
                                    }),
                                    Plot.ruleY([0]),
                                ],
                            })
                        }
                    />
                </Box>
            </Box>
            <Box display={'flex'} gap={2} m={2}>
                <Box width={'50%'}>
                    <PlotFromQueryCard
                        title="Participant age and reported sex"
                        description="Age buckets for participants. (note, the ages are based off the birth year so may be +- 1 year depending on DOB"
                        project="ourdna"
                        query={`
                            select
                                participant_id,
                                CASE reported_sex WHEN 1 THEN 'male' WHEN 2 THEN 'female' ELSE 'other/unknown' END as reported_sex,
                                date_part('year', current_date()) - try_cast("meta_birth-year" as int) as age
                            from participant
                            where date_part('year', current_date()) - try_cast("meta_birth-year" as int) is not null
                        `}
                        plot={(data, { width }) =>
                            Plot.plot({
                                width,
                                height: 450,
                                color: { legend: true },
                                marks: [
                                    Plot.rectY(
                                        data,
                                        Plot.binX<Plot.RectYOptions>(
                                            { y: 'count' },
                                            { x: 'age', fill: 'reported_sex', tip: true }
                                        )
                                    ),
                                ],
                            })
                        }
                    />
                </Box>
                <Box width={'50%'}>
                    <PlotFromQueryCard
                        title="Ancestry totals"
                        description="Count of participant by ancestry, each participant may have multiple ancestries"
                        project="ourdna"
                        query={`
                            with all_ancestries as (
                                select
                                    p.participant_id,
                                    unnest(p."meta_ancestry-participant-ancestry") as ancestry
                                from participant p
                            )
                            select
                                count(distinct participant_id) as count,
                                ancestry
                            from all_ancestries
                            group by 2
                            order by 1 desc
                        `}
                        plot={(data, { width }) =>
                            Plot.plot({
                                marginLeft: 160,
                                inset: 10,
                                height: 450,
                                color: {
                                    scheme: 'Observable10',
                                    reverse: true,
                                },
                                width,
                                marks: [
                                    Plot.barX(data, {
                                        y: 'ancestry',
                                        fill: 'ancestry',
                                        x: 'count',
                                        tip: true,
                                        sort: {
                                            y: 'x',
                                            reverse: true,
                                        },
                                    }),
                                ],
                            })
                        }
                    />
                </Box>
            </Box>

            <Box m={2}>
                <Typography fontWeight={'bold'} fontSize={26}>
                    Choices
                </Typography>
            </Box>
        </Box>
    )
}
