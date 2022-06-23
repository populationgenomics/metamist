import * as React from 'react';
import { ProjectApi, ProjectRow } from '../sm-api';

import { Alert, Button, Input, InputProps } from 'reactstrap';

const ProjectsAdmin = (props: any) => {

    const [projects, setProjects] = React.useState<ProjectRow[]>([]);
    const [error, setError] = React.useState<string | undefined>();

    const getProjects = () => {
        setError(undefined)
        new ProjectApi().getAllProjects().then(response => {
            setProjects(response.data);
        }).catch(er => setError(er.message))
    }

    React.useEffect(() => {
        getProjects()
    }, [])

    const headers = ['Id', 'Name', 'Dataset', 'Seqr', 'Seqr GUID']

    if (!!error) return (<Alert color="danger">
        {error}<br />
        <Button color="danger" onClick={() => getProjects()}>Retry</Button>
    </Alert>)

    if (!projects) return <div>Loading...</div>

    const updateMetaValue = (projectName: string, metaKey: string, metaValue: any) => {
        new ProjectApi().updateProject(projectName, { meta: { [metaKey]: metaValue } }).then(() => getProjects())
    }

    const ControlledInput: React.FunctionComponent<{ project: ProjectRow, metaKey: string } & InputProps> = ({ project, metaKey, ...props }) => {
        // const projStateMeta: any = projectStateValue[project.name!]?.meta || {}
        const projectMeta: any = project?.meta || {}
        return (<Input
            key={`input-${project.name}-${metaKey}`}
            type="text"
            label={metaKey}
            defaultValue={projectMeta[metaKey]}
            // onChange={(e) => setProjectMetaState({ [project.name!]: { meta: { ...projStateMeta, [metaKey]: e.target.value } } })}
            onBlur={(e) => {
                const newValue = e.currentTarget.value
                if (newValue === projectMeta[metaKey]) {
                    console.log(`Skip update to meta.${metaKey} as the value did not change`)
                    return
                }
                console.log(`Updating ${project.name}: meta.${metaKey} to ${newValue}`)
                updateMetaValue(project.name!, metaKey, newValue)
            }}
            {...props}
        />)
    }

    return <>
        <h1>Projects admin</h1>
        <table className='table table-bordered'>
            <thead>
                <tr>
                    {headers.map(k => <th key={k}>{k}</th>)}
                </tr>
            </thead>
            <tbody>
                {projects.map(p => {
                    // @ts-ignore
                    const meta: { [key: string]: any } = p?.meta || {}
                    return (<tr key={p.id}>
                        <td>{p.id}</td>
                        <td>{p.name}</td>
                        <td>{p.dataset}</td>
                        <td>
                            <Input
                                type="checkbox"
                                checked={meta?.is_seqr}
                                onChange={(e) => updateMetaValue(p.name!, 'is_seqr', e.target.checked)}
                            />
                        </td>
                        <td>
                            <ControlledInput
                                key={`controlled-${p.name!}-seqr-guid}`}
                                project={p}
                                metaKey="seqr_guid"
                                disabled={!p.meta?.is_seqr}
                                placeholder='Seqr GUID'
                            />
                        </td>
                    </tr>)
                })}
            </tbody>
        </table>
    </>
}

export default ProjectsAdmin;
