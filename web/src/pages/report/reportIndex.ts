import OurDnaDemographics from './reports/ourdna/Demographics'
import OurDnaProcessingTimes from './reports/ourdna/ProcessingTimes'
import OurDnaRecruitment from './reports/ourdna/Recruitment'

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
                    title: 'Processing Times',
                    id: 'processing_times',
                    content: OurDnaProcessingTimes,
                },
                {
                    title: 'Demographics',
                    id: 'demographics',
                    content: OurDnaDemographics,
                },
            ],
        },
    },
}
