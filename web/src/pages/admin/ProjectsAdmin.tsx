/* eslint-disable */
// Since this is for admin only

import { useQuery } from '@apollo/client'
import * as React from 'react'
import { Button, Checkbox, Input, InputProps, Message } from 'semantic-ui-react'
import { gql } from '../../__generated__/gql'
import Table from '../../shared/components/Table'
import { Project, ProjectApi } from '../../sm-api'

const GET_SEQUENCING_TYPES = gql(`
query GetSequencingTypes {
    enum {
        sequencingType
    }
}`)

interface ControlledInputProps extends InputProps {
    project: Project
    metaKey: string
}

const ProjectsAdmin = () => {
    const [projects, setProjects] = React.useState<Project[]>([])
    const [error, setError] = React.useState<string | undefined>()
    const seqTypeData = useQuery(GET_SEQUENCING_TYPES)

    const getProjects = () => {
        setError(undefined)
        new ProjectApi()
            .getAllProjects()
            .then((response) => {
                setProjects(response.data)
            })
            .catch((er) => setError(er.message))
    }

    React.useEffect(() => {
        getProjects()
    }, [])

    const headers = ['Id', 'Name', 'Dataset', 'Seqr', 'Seqr GUID']

    if (error)
        return (
            <Message negative>
                {error}
                <br />
                <Button negative onClick={() => getProjects()}>
                    Retry
                </Button>
            </Message>
        )

    if (!projects) return <div>Loading...</div>

    const updateMetaValue = (projectName: string, metaKey: string, metaValue: any) => {
        new ProjectApi()
            .updateProject(projectName, { meta: { [metaKey]: metaValue } })
            .then(() => getProjects())
    }

    const ControlledInput: React.FunctionComponent<ControlledInputProps> = ({
        project,
        metaKey,
        ...props
    }) => {
        // const projStateMeta: any = projectStateValue[project.name!]?.meta || {}
        const projectMeta: any = project?.meta || {}
        return (
            <Input
                fluid
                key={`input-${project.name}-${metaKey}`}
                // label={metaKey}
                defaultValue={projectMeta[metaKey]}
                // onChange={(e) => setProjectMetaState({ [project.name!]: { meta: { ...projStateMeta, [metaKey]: e.target.value } } })}
                onBlur={(e: React.FocusEvent<HTMLInputElement>) => {
                    const newValue = e.currentTarget.value
                    if (newValue === projectMeta[metaKey]) {
                        console.log(`Skip update to meta.${metaKey} as the value did not change`)
                        return
                    }
                    console.log(`Updating ${project.name}: meta.${metaKey} to ${newValue}`)
                    updateMetaValue(project.name!, metaKey, newValue)
                }}
                {...props}
            />
        )
    }

    const seqTypes: (string | null)[] = seqTypeData?.data?.enum?.sequencingType || [null]

    return (
        <>
            <h1>Projects admin</h1>
            <Table className="table table-bordered">
                <thead>
                    <tr>
                        {headers.map((k) => (
                            <th key={k}>{k}</th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {projects
                        .map((p) => {
                            const meta: { [key: string]: any } = p?.meta || {}

                            const isSeqr = meta?.is_seqr || false
                            const types = isSeqr ? seqTypes : [null]
                            const rowSpan = isSeqr ? seqTypes.length : undefined

                            return types.map((seqType, idx) => (
                                <tr key={`${p.id}-${seqType}`}>
                                    {idx === 0 && (
                                        <>
                                            <td rowSpan={rowSpan}>{p.id}</td>
                                            <td rowSpan={rowSpan}>{p.name}</td>
                                            <td rowSpan={rowSpan}>{p.dataset}</td>
                                            <td rowSpan={rowSpan}>
                                                <Checkbox
                                                    checked={meta?.is_seqr}
                                                    onChange={(e, data) =>
                                                        updateMetaValue(
                                                            p.name!,
                                                            'is_seqr',
                                                            data.checked
                                                        )
                                                    }
                                                />
                                            </td>
                                        </>
                                    )}
                                    {!seqType && <td />}
                                    {!!seqType && (
                                        <td>
                                            <ControlledInput
                                                key={`controlled-${p.name!}-${seqType}-seqr-guid}`}
                                                project={p}
                                                metaKey={`seqr-project-${seqType}`}
                                                placeholder={`Seqr ${seqType} project GUID`}
                                                label={seqType}
                                            />
                                        </td>
                                    )}
                                </tr>
                            ))
                        })
                        .flat()}
                </tbody>
            </Table>
        </>
    )
}

export default ProjectsAdmin
