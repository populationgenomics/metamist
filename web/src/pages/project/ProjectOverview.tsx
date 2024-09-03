import * as React from 'react'
import { useNavigate, useParams } from 'react-router-dom'

import ProjectSelector, { IMetamistProject } from './ProjectSelector'

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
        <>
            <ProjectSelector onProjectSelect={onProjectSelect} />
            <hr />
            {body}
        </>
    )
}

export default ProjectOverview
