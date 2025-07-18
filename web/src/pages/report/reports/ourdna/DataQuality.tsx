import Report from '../../components/Report'
import { ReportItemMetric, ReportItemTable } from '../../components/ReportItem'
import ReportRow from '../../components/ReportRow'

const ROW_HEIGHT = 220
const PROJECT = 'ourdna'

const AGE_QUERY = `
    select distinct
        p.participant_id,
        s.sample_id,
        s.external_id as sample_agd_id,
        try_cast(p.meta_birth_year as int) as birth_year,
        date_part('year', try_strptime(
            s.meta_collection_datetime,
            '%Y-%m-%dT%H:%M:%S'
        )) as collection_year,
        collection_year - birth_year as age
    from participant p
    join sample s
    on s.participant_id = p.participant_id
    where age is not null
    and s.type = 'blood'
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
                    title="Participants without a parent blood sample"
                    description="Participants whose samples aren't correctly nested under a blood sample"
                    query={`
                        select
                            count(distinct participant_id) as count
                        from sample s
                        where not exists (
                            select 1 from sample ss
                            where ss.sample_id = s.sample_root_id
                            and ss.type = 'blood'
                        ) and type != 'blood'
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
                        and meta_collection_datetime is null
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
                        and meta_collection_datetime is not null
                        and try_strptime(meta_collection_datetime, '%Y-%m-%dT%H:%M:%S') is null
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
                        where type != 'blood'
                        and meta_processing_end_datetime is null
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
                        and meta_processing_end_datetime is not null
                        and try_strptime(meta_processing_end_datetime, '%Y-%m-%dT%H:%M:%S') is null
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
            <ReportRow>
                <ReportItemTable
                    height={600}
                    flexGrow={1}
                    flexBasis={300}
                    title="Mismatched or invalid Sonic Ids"
                    description={`Sonic ids that are either not numeric, or don't match the sonic id recorded in the event portal`}
                    project={PROJECT}
                    showToolbar={true}
                    query={`
                        select
                            coalesce(p.participant_id, s.participant_id) as participant_id,
                            s.sample_id,
                            s.external_id as sample_agd_id,
                            s.meta_processing_site,
                            p.meta_event_recorded_sonic_id as event_portal_sonic_id,
                            s.external_id_sonic as processing_site_sonic_id
                        from participant p
                        full outer join sample s on p.participant_id = s.participant_id
                        where (
                            p.meta_event_recorded_sonic_id is not null
                            and s.external_id_sonic is not null
                            and p.meta_event_recorded_sonic_id != s.external_id_sonic
                        )
                        or (
                            s.external_id_sonic is not null
                            and not regexp_full_match(s.external_id_sonic, '\\d+')
                        )
                        or (
                            p.meta_event_recorded_sonic_id is not null
                            and not regexp_full_match(p.meta_event_recorded_sonic_id, '\\d+')
                        )
                    `}
                />
            </ReportRow>
        </Report>
    )
}
