import { Box, Tab, Tabs, Typography } from '@mui/material'
import { Link, useParams } from 'react-router-dom'
import MuckError from '../../shared/components/MuckError'
import { reports } from './reportIndex'

const NotFound = () => <MuckError message="Report not found" />

export default function ProjectReport() {
    const { projectName, reportName, tabName } = useParams()

    if (!projectName || !reportName || !reports[projectName] || !reports[projectName][reportName]) {
        return <NotFound />
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
                    <Report />
                </Box>
            </Box>
        </Box>
    )
}
