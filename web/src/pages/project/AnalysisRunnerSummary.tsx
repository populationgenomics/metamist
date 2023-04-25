import * as React from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import ProjectSelector from './ProjectSelector'
import AnalysisRunnerGrid from './AnalysisRunnerGrid'

const AnalysisRunnerSummary: React.FunctionComponent = () => {
    const navigate = useNavigate()
    const { projectName } = useParams()
    const project_name = projectName || ''

    const projectSelectorOnClick = React.useCallback(
        (_, { value }) => {
            navigate(`/analysis-runner/${value}`)
        },
        [navigate]
    )

    return (
        <>
            <ProjectSelector onClickFunction={projectSelectorOnClick} />
            {project_name && <AnalysisRunnerGrid projectName={project_name} />}
        </>
    )
}

export default AnalysisRunnerSummary
