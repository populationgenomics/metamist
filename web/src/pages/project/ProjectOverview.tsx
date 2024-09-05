import * as React from 'react'
import { useNavigate, useParams } from 'react-router-dom'

import ProjectSelector, { IMetamistProject } from './ProjectSelector'

import { Box } from '@mui/material'
import { SplitPage } from '../../shared/components/Layout/SplitPage'
import { ProjectCommentsView } from '../comments/ProjectCommentsView'
import { ProjectGridContainer } from './ProjectGridContainer'
import { ProjectSummaryView } from './ProjectSummary'

const ProjectOverview: React.FunctionComponent = () => {
    const navigate = useNavigate()

    const { projectName } = useParams()

    // retrigger if project changes, or pageLimit changes
    const onProjectSelect = (_project: IMetamistProject) => {
        navigate(`/project/${_project.name}`)
    }

    let body = (
        <p>
            <em>Please select a project</em>
        </p>
    )
    if (projectName) {
        body = (
            <>
                <ProjectSummaryView projectName={projectName} />
                <ProjectGridContainer projectName={projectName} />
            </>
        )
    }

    return (
        <SplitPage
            collapsedWidth={60}
            collapsed={true}
            main={() => (
                <Box p={10} pt={5}>
                    <ProjectSelector onProjectSelect={onProjectSelect} />
                    <hr />
                    {body}
                </Box>
            )}
            side={({ collapsed, onToggleCollapsed }) =>
                projectName ? (
                    <ProjectCommentsView
                        projectName={projectName}
                        collapsed={collapsed}
                        onToggleCollapsed={onToggleCollapsed}
                    />
                ) : null
            }
        />
    )
}

export default ProjectOverview
