import * as Plot from '@observablehq/plot'
import Report from '../../components/Report'
import { ReportItemMetric, ReportItemPlot } from '../../components/ReportItem'
import ReportRow from '../../components/ReportRow'

const ROW_HEIGHT = 450
const PROJECT = 'ourdna'

export default function Demographics() {
    return (
        <Report>
            <ReportRow>
                <ReportItemMetric
                    project={PROJECT}
                    height={ROW_HEIGHT}
                    flexBasis={200}
                    flexGrow={1}
                    title="Ratio of reported sex"
                    subtitle="For participants who have reported"
                    description="This chart shows the ratio of participants that have responded by reporting their sex"
                    query={`
                        select
                            count(*) filter (reported_sex = 1) as male,
                            count(*) filter (reported_sex = 2) as female
                        from participant
                    `}
                />
                <ReportItemPlot
                    project={PROJECT}
                    height={ROW_HEIGHT}
                    flexGrow={3}
                    flexBasis={720}
                    title={'Participant count by ancestry over time'}
                    query={[
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
                    plot={(data) => ({
                        marginLeft: 40,
                        inset: 10,
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
                    })}
                />
            </ReportRow>
            <ReportRow>
                <ReportItemPlot
                    height={ROW_HEIGHT}
                    project={PROJECT}
                    flexGrow={1}
                    flexBasis={400}
                    title="Participant age and reported sex"
                    description="Age buckets for participants. (note, the ages are based off the birth year so may be +- 1 year depending on DOB"
                    query={`
                        select
                            participant_id,
                            CASE reported_sex WHEN 1 THEN 'male' WHEN 2 THEN 'female' ELSE 'other/unknown' END as reported_sex,
                            date_part('year', current_date()) - try_cast("meta_birth-year" as int) as age
                        from participant
                        where date_part('year', current_date()) - try_cast("meta_birth-year" as int) is not null
                    `}
                    plot={(data) => ({
                        color: { legend: true },
                        y: { label: 'count' },
                        marks: [
                            Plot.rectY(
                                data,
                                Plot.binX<Plot.RectYOptions>(
                                    { y: 'count' },
                                    { x: 'age', fill: 'reported_sex', tip: true }
                                )
                            ),
                        ],
                    })}
                />
                <ReportItemPlot
                    height={ROW_HEIGHT}
                    project={PROJECT}
                    flexGrow={1}
                    flexBasis={400}
                    title="Ancestry totals"
                    description="Count of participant by ancestry, each participant may have multiple ancestries"
                    query={`
                        with all_ancestries as (
                            select
                                p.participant_id,
                                unnest(p."meta_ancestry-participant-ancestry") as ancestry
                            from participant p
                            join sample s on s.participant_id = p.participant_id
                            where s.type = 'blood'
                        )
                        select
                            count(distinct participant_id) as count,
                            ancestry
                        from all_ancestries
                        group by 2
                        order by 1 desc
                    `}
                    plot={(data) => ({
                        marginLeft: 160,
                        inset: 10,
                        color: {
                            scheme: 'Observable10',
                            reverse: true,
                        },
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
                    })}
                />
            </ReportRow>
            <ReportRow>
                <ReportItemPlot
                    height={800}
                    project={PROJECT}
                    flexGrow={1}
                    flexBasis={400}
                    title="Ancestry by processing site"
                    description="Count of participant by ancestry, broken down by processing site. Each participant may have multiple ancestries"
                    query={`
                        with all_ancestries as (
                            select
                                p.participant_id,
                                unnest(p."meta_ancestry-participant-ancestry") as ancestry,
                                coalesce(s."meta_processing-site", 'Unknown') as processing_site
                            from participant p
                            join sample s on s.participant_id = p.participant_id
                            where s.type = 'blood'
                        )
                        select
                            count(distinct participant_id) as count,
                            ancestry,
                            processing_site
                        from all_ancestries
                        group by 2, 3
                        order by 1 desc
                    `}
                    plot={(data) => ({
                        marginLeft: 100,
                        marginRight: 100,
                        inset: 10,
                        y: {
                            inset: 1,
                        },
                        color: {
                            scheme: 'Observable10',
                            reverse: true,
                        },
                        marks: [
                            Plot.barX(data, {
                                y: 'processing_site',
                                fy: 'ancestry',
                                fill: 'ancestry',
                                x: 'count',
                                tip: true,
                                sort: {
                                    fy: 'x',
                                    reverse: true,
                                },
                            }),
                        ],
                    })}
                />
            </ReportRow>
        </Report>
    )
}
