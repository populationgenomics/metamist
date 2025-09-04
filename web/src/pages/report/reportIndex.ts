import OurDnaConsentAndChoices from './reports/ourdna/ConsentAndChoices'
import OurDnaDataQuality from './reports/ourdna/DataQuality'
import OurDnaDemographics from './reports/ourdna/Demographics'
import OurDnaRecruitment from './reports/ourdna/Recruitment'
import OurDnaProcessingTimes from './reports/ourdna/SampleProcessing'

type ReportTab = {
    title: string
    id: string
    content: React.ComponentType
}

type ReportDefinition = {
    title: string
    tabs: ReportTab[]
}

export const reports: Record<string, Record<string, ReportDefinition>> = {
    ourdna: {
        dashboard: {
            title: 'OurDNA Dashboard',
            tabs: [
                {
                    title: 'Recruitment',
                    id: 'recruitment',
                    content: OurDnaRecruitment,
                },
                {
                    title: 'Consent & Choices',
                    id: 'consent_and_choices',
                    content: OurDnaConsentAndChoices,
                },
                {
                    title: 'Sample Processing',
                    id: 'sample_processing',
                    content: OurDnaProcessingTimes,
                },
                {
                    title: 'Demographics',
                    id: 'demographics',
                    content: OurDnaDemographics,
                },
                {
                    title: 'Data Quality',
                    id: 'data_quality',
                    content: OurDnaDataQuality,
                },
            ],
        },
    },
}
