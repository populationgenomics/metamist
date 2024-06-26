import * as _ from 'lodash'
import * as React from 'react'

import { useParams } from 'react-router-dom'

import { Dropdown, Message } from 'semantic-ui-react'

import { useQuery } from '@apollo/client'

import { gql } from '../../__generated__/gql'

const GET_PROJECTS = gql(`
    query getProjects {
        myProjects {
            id
            name
            meta
        }
    }
`)

export interface IMetamistProject {
    id: number
    name: string
    meta: object
}

interface ProjectSelectorProps {
    onProjectSelect: (project: IMetamistProject) => void
}

const ProjectSelector: React.FunctionComponent<ProjectSelectorProps> = ({ onProjectSelect }) => {
    const { loading, error, data } = useQuery(GET_PROJECTS)
    const { projectName } = useParams()

    if (error) {
        return (
            <Message negative>
                <h4>An error occurred while getting projects</h4>
                <p>{JSON.stringify(error?.networkError || error)}</p>
            </Message>
        )
    }

    if (loading) {
        return <p>Loading projects...</p>
    }

    const projectsByName = _.keyBy(data?.myProjects, 'name')

    return (
        <div>
            <h2>Select a project</h2>
            <Dropdown
                search
                selection
                fluid
                onChange={(_, data) =>
                    onProjectSelect(projectsByName[data?.value] as IMetamistProject)
                }
                placeholder="Select a project"
                value={projectName}
                options={
                    data &&
                    _.sortBy(data.myProjects, (p) => p.name).map((p) => ({
                        key: p.name,
                        text: p.name,
                        value: p.name,
                    }))
                }
            />
        </div>
    )
}

export default ProjectSelector
