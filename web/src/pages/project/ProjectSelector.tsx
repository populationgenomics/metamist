import * as React from 'react'

import { useParams } from 'react-router-dom'

import { Dropdown } from 'semantic-ui-react'

import { useQuery } from '@apollo/client'

import { gql } from '../../__generated__/gql'

// import { SearchItem, MetaSearchEntityPrefix } from '../../sm-api/api'

const GET_PROJECTS = gql(`
    query getProjects {
        myProjects {
            name
        }
    }
`)

interface ProjectSelectorProps {
    onClickFunction: (_: any, { value }: any) => void
}

const ProjectSelector: React.FunctionComponent<ProjectSelectorProps> = ({ onClickFunction }) => {
    const { loading, error, data } = useQuery(GET_PROJECTS)
    const { projectName } = useParams()

    if (error) {
        return <p>An error occurred while getting projects: {error}</p>
    }

    if (loading) {
        return <p>Loading projects...</p>
    }

    return (
        <div>
            <h2>Select a project</h2>
            <Dropdown
                search
                selection
                fluid
                onChange={onClickFunction}
                placeholder="Select a project"
                value={projectName ?? ''}
                options={
                    data &&
                    data.myProjects.map((p) => ({
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
