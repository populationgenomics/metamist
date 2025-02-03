import * as React from 'react'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import MuckError from '../../shared/components/MuckError'
import { ProjectSummary, WebApi } from '../../sm-api'

import { Box, Button } from '@mui/material'
import { Link } from 'react-router-dom'
import BatchStatistics from './BatchStatistics'
import MultiQCReports from './MultiQCReports'
import SeqrLinks from './SeqrLinks'
import SeqrSync from './SeqrSync'
import SummaryStatistics from './SummaryStatistics'
import TotalsStats from './TotalsStats'

import { reports } from '../report/reportIndex'

interface ProjectSummaryProps {
    projectName: string
}

export const ProjectSummaryView: React.FunctionComponent<ProjectSummaryProps> = ({
    projectName,
}) => {
    const [summary, setSummary] = React.useState<ProjectSummary | undefined>()

    const [isLoading, setIsLoading] = React.useState<boolean>(false)
    const [error, setError] = React.useState<string | undefined>()

    React.useEffect(() => {
        if (!projectName) {
            setSummary(undefined)
            return
        }
        setError(undefined)
        setIsLoading(true)
        new WebApi()
            .getProjectSummary(projectName)
            .then((resp) => {
                setSummary(resp.data)
                setIsLoading(false)
            })
            .catch((er: Error) => {
                setError(er.message)
            })
    }, [projectName])

    if (isLoading) {
        return <LoadingDucks />
    }

    if (error || !summary) {
        return (
            <MuckError
                message={`Ah Muck, An error occurred when fetching project summary: ${error}`}
            />
        )
    }

    const customReports = reports[projectName]

    return (
        <Box>
            <Box display="flex">
                <Box flexGrow={1}>
                    <TotalsStats summary={summary ?? {}} />
                </Box>
                <Box flexGrow={1}>
                    <Box display={'flex'}>
                        <Box flexGrow={1}>{customReports && <h2>Custom Reports</h2>}</Box>
                        <Box>
                            <Button
                                component={Link}
                                variant="outlined"
                                to={`/project/${projectName}/query`}
                            >
                                SQL Explore
                            </Button>
                        </Box>
                    </Box>
                    <ul>
                        {customReports &&
                            Object.entries(customReports).map(([key, report]) => (
                                <li key={key}>
                                    <Link to={`/project/${projectName}/report/${key}`}>
                                        {report.title}
                                    </Link>
                                </li>
                            ))}
                    </ul>
                </Box>
            </Box>
            <SummaryStatistics
                projectName={projectName}
                cramSeqrStats={summary?.cram_seqr_stats ?? {}}
            />
            <BatchStatistics
                projectName={projectName}
                cramSeqrStats={summary?.cram_seqr_stats ?? {}}
                batchSequenceStats={summary?.batch_sequencing_group_stats ?? {}}
            />

            <hr />

            <MultiQCReports projectName={projectName} />
            <SeqrLinks seqrLinks={summary?.seqr_links ?? {}} />
            <SeqrSync syncTypes={summary?.seqr_sync_types} project={projectName} />
        </Box>
    )
}
