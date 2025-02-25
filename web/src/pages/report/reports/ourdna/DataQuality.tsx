import Report from '../../components/Report'
import { ReportItemMetric, ReportItemTable } from '../../components/ReportItem'
import ReportRow from '../../components/ReportRow'

const ROW_HEIGHT = 220
const PROJECT = 'ourdna'

const AGE_QUERY = `
    select
        participant_id,
        "meta_birth-year" as birth_year,
        date_part('year', current_date()) - try_cast("meta_birth-year" as int) as age
    from participant
    where date_part('year', current_date()) - try_cast("meta_birth-year" as int) is not null
`

export default function DataQuality() {
    return (
        <Report>
            <ReportRow>
                <ReportItemMetric
                    project={PROJECT}
                    height={ROW_HEIGHT}
                    flexBasis={300}
                    flexGrow={1}
                    title="Participants without a blood sample"
                    description="Participants who don't have a blood sample"
                    query={`
                        select
                            count(distinct participant_id) as count
                        from participant p
                        where not exists (
                            select 1 from sample s
                            where s.participant_id = p.participant_id
                            and s.type = 'blood'
                        )
                    `}
                />
                <ReportItemMetric
                    project={PROJECT}
                    height={ROW_HEIGHT}
                    flexBasis={300}
                    flexGrow={1}
                    title="Samples without a parent blood sample"
                    description="Samples that aren't correctly nested under a blood sample"
                    query={`
                        select
                            count(distinct sample_id) as count
                        from sample s
                        where not exists (
                            select 1 from sample ss
                            where ss.sample_id = s.sample_root_id
                            and ss.type = 'blood'
                        )
                    `}
                />
                <ReportItemMetric
                    project={PROJECT}
                    height={ROW_HEIGHT}
                    flexBasis={300}
                    flexGrow={1}
                    title="Participants without collection times"
                    description="Participants who do have blood samples, but who don't have a collection time for their blood sample"
                    query={`
                        select
                            count(distinct participant_id) as count
                        from sample
                        where type = 'blood'
                        and "meta_collection-time" is null
                    `}
                />
                <ReportItemMetric
                    project={PROJECT}
                    height={ROW_HEIGHT}
                    flexBasis={300}
                    flexGrow={1}
                    title="Participants with invalid collection times"
                    description="Participants who do have blood sample but whose sample collection times are malformed"
                    query={`
                        select
                            count(distinct participant_id) as count
                        from sample
                        where type = 'blood'
                        and "meta_collection-time" is not null
                        and try_strptime(nullif("meta_collection-time", ' '), '%Y-%m-%d %H:%M:%S') is null
                    `}
                />
            </ReportRow>
            <ReportRow>
                <ReportItemMetric
                    project={PROJECT}
                    height={ROW_HEIGHT}
                    flexBasis={300}
                    flexGrow={1}
                    title="Participants without a process end time"
                    description="Participants who do have blood sample but who are missing a process end time"
                    query={`
                        select
                            count(distinct participant_id) as count
                        from sample
                        where type = 'blood'
                        and "meta_process-end-time" is null
                    `}
                />
                <ReportItemMetric
                    project={PROJECT}
                    height={ROW_HEIGHT}
                    flexBasis={300}
                    flexGrow={1}
                    title="Participants with a invalid process end time"
                    description="Participants who do have blood sample and a process end time, but whose process end times are malformed"
                    query={`
                        select
                            count(distinct participant_id) as count
                        from sample
                        where type = 'blood'
                        and "meta_process-end-time" is not null
                        and try_strptime(nullif("meta_process-end-time", ' '), '%Y-%m-%d %H:%M:%S') is null
                    `}
                />
                <ReportItemMetric
                    project={PROJECT}
                    height={ROW_HEIGHT}
                    flexBasis={300}
                    flexGrow={1}
                    title="Participants with unreasonable ages"
                    description="Count of participants whose ages are less than 16 or more than 120"
                    query={[
                        {
                            name: 'ages',
                            query: AGE_QUERY,
                        },
                        {
                            name: 'counts',
                            query: `
                                select count(*) as count from ages
                                where age is not null and (age < 16 or age > 120)
                            `,
                        },
                    ]}
                />
            </ReportRow>
            <ReportRow>
                <ReportItemTable
                    height={600}
                    flexGrow={1}
                    flexBasis={300}
                    title="Participants with unreasonable ages"
                    project={PROJECT}
                    showToolbar={true}
                    query={[
                        {
                            name: 'ages',
                            query: AGE_QUERY,
                        },
                        {
                            name: 'result',
                            query: `
                                select * from ages
                                where age is not null and (age < 16 or age > 120)
                            `,
                        },
                    ]}
                />
            </ReportRow>
        </Report>
    )
}
