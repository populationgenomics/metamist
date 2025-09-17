import { Alert, Box, Tab, Tabs, Typography } from '@mui/material'
import { useContext } from 'react'
import { Link, useParams } from 'react-router-dom'
import { ProjectMemberRole } from '../../__generated__/graphql'
import MuckError from '../../shared/components/MuckError'
import { ViewerContext } from '../../viewer'
import { reports } from './reportIndex'

const NotFound = () => <MuckError message="Report not found" />

export default function ProjectReport() {
    const { projectName, reportName, tabName } = useParams()
    const viewer = useContext(ViewerContext)
    
    if (!projectName || !reportName || !reports[projectName] || !reports[projectName][reportName]) {
        return <NotFound />
    }

    const canAccess = viewer?.checkProjectAccessByName(projectName, [
        ProjectMemberRole.Contributor,
        ProjectMemberRole.Reader,
        ProjectMemberRole.Writer,
    ])
    if (!canAccess) {
        return (
            <Box p={5}>
                <Alert severity="error">
                    You do not have appropriate permissions to view the project "{projectName}".
                </Alert>
            </Box>
        )
    }

    const reportTabs = reports[projectName][reportName].tabs
    const urlActiveTabIndex = reportTabs.findIndex((tab) => tab.id === tabName)
    const activeTabIndex = urlActiveTabIndex > -1 ? urlActiveTabIndex : 0
    const activeTab = reportTabs[activeTabIndex]

    const Report = activeTab.content

    return (
        <Box>
            <Box>
                <Tabs value={activeTabIndex}>
                    {reportTabs.map((tab) => (
                        <Tab
                            component={Link}
                            to={`/project/${projectName}/report/${reportName}/${tab.id}`}
                            label={tab.title}
                            key={tab.id}
                        />
                    ))}
                </Tabs>
            </Box>
            <Box p={2}>
                <Typography fontWeight={'bold'} fontSize={26} mb={2}>
                    {activeTab.title}
                </Typography>
                <Box>
                    <Report project={projectName} />
                </Box>
            </Box>
        </Box>
    )
}
