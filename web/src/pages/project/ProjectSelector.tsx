import * as React from 'react'

import { useParams, useNavigate } from 'react-router-dom'

import { Dropdown } from 'semantic-ui-react'

import { useQuery } from '@apollo/client'

import { gql } from '../../__generated__/gql'

const GET_PROJECTS = gql(`
    query getProjects {
        myProjects {
            name
        }
    }
`)

interface ProjectSelectorProps {
    setPageNumber: React.Dispatch<React.SetStateAction<number>>
    setPageLimit: React.Dispatch<React.SetStateAction<number>>
    setFilterValues: React.Dispatch<
        React.SetStateAction<Record<string, Record<string, Record<string, string>[]>>>
    >
    setGridFilterValues: React.Dispatch<React.SetStateAction<Record<string, string>>>
    pageLimit: number
}

const ProjectSelector: React.FunctionComponent<ProjectSelectorProps> = ({
    setPageNumber,
    setPageLimit,
    setFilterValues,
    setGridFilterValues,
    pageLimit,
}) => {
    const { loading, error, data } = useQuery(GET_PROJECTS)
    const { projectName } = useParams()
    const navigate = useNavigate()
    const handleOnClick = React.useCallback(
        (_, { value }) => {
            navigate(`/project/${value}`)
            setPageNumber(1)
            setPageLimit(pageLimit)
            setFilterValues({})
            setGridFilterValues({})
        },
        [navigate, setPageLimit, setPageNumber, pageLimit, setFilterValues, setGridFilterValues]
    )

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
                onChange={handleOnClick}
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
