import * as React from 'react';
import { ProjectApi, ProjectRow } from '../sm-api';

import { Alert, Button, Input } from 'reactstrap';

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

    const headers = ['Id', 'Name', 'Dataset', 'Seqr']

    if (!!error) return (<Alert color="danger">
        {error}<br />
        <Button color="danger" onClick={() => getProjects()}>Retry</Button>
    </Alert>)

    if (!projects) return <div>Loading...</div>

    return <>
        <h1>Projects admin</h1>
        <table className='table table-bordered'>
            <thead>
                <tr>
                    {headers.map(k => <th key={k}>{k}</th>)}
                </tr>
            </thead>
            <tbody>
                {projects.map(p => <tr key={p.id}>
                    <td>{p.id}</td>
                    <td>{p.name}</td>
                    <td>{p.dataset}</td>
                    <td>
                        <Input type="checkbox" checked={p?.meta?.is_seqr} onChange={(e) => {
                            new ProjectApi().updateProject(p.name!, { meta: { is_seqr: e.target.checked } }).then(() => getProjects())
                        }
                        } />
                    </td>
                </tr>)}
            </tbody>
        </table>
    </>
}

export default ProjectsAdmin;
