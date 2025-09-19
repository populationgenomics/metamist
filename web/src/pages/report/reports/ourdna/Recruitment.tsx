import * as Plot from '@observablehq/plot'
import Report from '../../components/Report'
import { ReportItemPlot } from '../../components/ReportItem'
import ReportRow from '../../components/ReportRow'

const ROW_HEIGHT = 400

export default function Recruitment({ project }: { project: string }) {
    return (
        <Report>
            <ReportRow>
                <ReportItemPlot
                    height={ROW_HEIGHT}
                    flexGrow={1}
                    title="Recruitment funnel"
                    description="The number of participants at each stage of recruitment"
                    project={project}
                    query={`
                        with funnel as (
                            select
                                count(distinct p.participant_id) as "All Participants",
                                count(distinct p.participant_id) filter (
                                    p."meta_consent_blood_consent" = 'yes' and p."meta_consent_informed_consent" = 'yes'
                                ) as "Participants consented",
                                count(distinct p.participant_id) filter (s.sample_id is not null) as "Participants with samples"
                            from participant p
                            left join sample s
                            on s.participant_id = p.participant_id
                        ) unpivot funnel
                        on columns (*)
                        into name stage value count
                    `}
                    plot={(data) => ({
                        marginLeft: 160,
                        inset: 10,
                        marks: [
                            Plot.barX(data, {
                                y: 'stage',
                                x: 'count',
                                tip: true,
                                fill: 'stage',
                            }),
                        ],
                    })}
                />

                <ReportItemPlot
                    height={ROW_HEIGHT}
                    flexGrow={1}
                    title="Recruitment types"
                    description="How the participant was recruited"
                    project={project}
                    query={`
                        select
                            count(distinct participant_id) as count,
                            coalesce("meta_collection_event_type", 'unknown') as collection_type
                        from sample s
                        where type = 'blood'
                        group by 2
                    `}
                    plot={(data) => ({
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
                    })}
                />
            </ReportRow>
            <ReportRow>
                <ReportItemPlot
                    height={ROW_HEIGHT}
                    flexGrow={1}
                    title="Recruitment type by processing site"
                    description="How the participant was recruited, broken down by processing site"
                    project={project}
                    query={`
                        select
                            count(distinct participant_id) as count,
                            coalesce(meta_collection_event_type, 'unknown') as collection_type,
                            coalesce(meta_processing_site, 'unknown') as processing_site,
                        from sample s
                        where type = 'blood'
                        group by 2, 3
                    `}
                    plot={(data) => ({
                        color: { legend: true },
                        marginLeft: 100,
                        marks: [
                            Plot.barX(
                                data,
                                Plot.stackX({
                                    x: 'count',
                                    y: 'processing_site',
                                    fill: 'collection_type',
                                    inset: 0.5,
                                    tip: true,
                                    order: '-sum',
                                })
                            ),
                        ],
                    })}
                />
            </ReportRow>
        </Report>
    )
}
