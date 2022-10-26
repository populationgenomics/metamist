import * as React from "react";
import { ProjectApi, ProjectRow } from "../sm-api";

import { Message, Button, Checkbox, Input } from "semantic-ui-react";

const ProjectsAdmin = (props: any) => {
    const [projects, setProjects] = React.useState<ProjectRow[]>([]);
    const [error, setError] = React.useState<string | undefined>();

    const getProjects = () => {
        setError(undefined);
        new ProjectApi()
            .getAllProjects()
            .then((response) => {
                setProjects(response.data);
            })
            .catch((er) => setError(er.message));
    };

    React.useEffect(() => {
        getProjects();
    }, []);

    const headers = ["Id", "Name", "Dataset", "Seqr", "Seqr GUID"];

    if (!!error)
        return (
            <Message negative>
                {error}
                <br />
                <Button color="red" onClick={() => getProjects()}>
                    Retry
                </Button>
            </Message>
        );

    if (!projects) return <div>Loading...</div>;

    const updateMetaValue = (
        projectName: string,
        metaKey: string,
        metaValue: any
    ) => {
        new ProjectApi()
            .updateProject(projectName, { meta: { [metaKey]: metaValue } })
            .then(() => getProjects());
    };

    const ControlledInput: React.FunctionComponent<{
        project: ProjectRow;
        metaKey: string;
    }> = ({ project, metaKey, ...props }) => {
        // const projStateMeta: any = projectStateValue[project.name!]?.meta || {}
        const projectMeta: any = project?.meta || {};
        return (
            <Input
                fluid
                key={`input-${project.name}-${metaKey}`}
                // label={metaKey}
                defaultValue={projectMeta[metaKey]}
                // onChange={(e) => setProjectMetaState({ [project.name!]: { meta: { ...projStateMeta, [metaKey]: e.target.value } } })}
                onBlur={(e) => {
                    const newValue = e.currentTarget.value;
                    if (newValue === projectMeta[metaKey]) {
                        console.log(
                            `Skip update to meta.${metaKey} as the value did not change`
                        );
                        return;
                    }
                    console.log(
                        `Updating ${project.name}: meta.${metaKey} to ${newValue}`
                    );
                    updateMetaValue(project.name!, metaKey, newValue);
                }}
                {...props}
            />
        );
    };

    return (
        <>
            <h1>Projects admin</h1>
            <table className="table table-bordered">
                <thead>
                    <tr>
                        {headers.map((k) => (
                            <th key={k}>{k}</th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {projects.map((p) => {
                        // @ts-ignore
                        const meta: { [key: string]: any } = p?.meta || {};
                        return (
                            <tr key={p.id}>
                                <td>{p.id}</td>
                                <td>{p.name}</td>
                                <td>{p.dataset}</td>
                                <td>
                                    <Checkbox
                                        checked={meta?.is_seqr}
                                        onChange={(e, data) =>
                                            updateMetaValue(
                                                p.name!,
                                                "is_seqr",
                                                data.checked
                                            )
                                        }
                                    />
                                </td>
                                <td>
                                    <ControlledInput
                                        key={`controlled-${p.name!}-seqr-guid}`}
                                        project={p}
                                        metaKey="seqr_guid"
                                        disabled={!p.meta?.is_seqr}
                                        placeholder="Seqr GUID"
                                    />
                                </td>
                            </tr>
                        );
                    })}
                </tbody>
            </table>
        </>
    );
};

export default ProjectsAdmin;
