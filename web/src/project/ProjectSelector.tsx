import * as React from 'react'

import { useSearchParams } from 'react-router-dom'

import { Input } from 'reactstrap'

import { AppContext } from '../GlobalState'
import { ProjectApi } from '../sm-api/api'

interface ProjectSelectorProps {
    onChange?: (project: string) => void
}

export const ProjectSelector: React.FunctionComponent<ProjectSelectorProps> = ({ onChange }) => {

    // store project in global settings
    const globalContext = React.useContext(AppContext)

    let [searchParams, setSearchParams] = useSearchParams()

    const projectFromParams = searchParams.get('project')
    if (!!projectFromParams && projectFromParams !== globalContext.project) {
        globalContext.setProject?.(projectFromParams)
    }

    const [projects, setProjects] = React.useState<string[] | undefined>()
    const [error, setError] = React.useState<string | undefined>()


    React.useEffect(() => {
        new ProjectApi().getMyProjects()
            .then(projects => setProjects(projects.data))
            .catch(er => setError(er.message))
    }, [])

    if (error) {
        return <p>An error occurred while getting projects: {error}</p>
    }

    if (projects === undefined) {
        return <p>Loading projects...</p>
    }

    const projectChanged: React.ChangeEventHandler<HTMLInputElement> = (e) => {
        const project = e.currentTarget.value
        globalContext.setProject?.(project)
        searchParams.set('project', project)
        setSearchParams(searchParams)
        onChange?.(project)

    }

    return <div>
        <h4>Select a project</h4>
        <Input type="select" onChange={projectChanged} value={globalContext.project || ''}>
            {(!globalContext.project) && <option value=''>Select a project</option>}
            {projects.map(p => <option key={p} value={p}>{p}</option>)}
        </Input>
    </div>
}
