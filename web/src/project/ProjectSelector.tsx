import * as React from "react";

import { useParams, useNavigate } from "react-router-dom";

import { Input } from "reactstrap";

import { ProjectApi } from "../sm-api/api";

interface ProjectSelectorProps {
    onChange?: (project: string) => void;
}

export const ProjectSelector: React.FunctionComponent<ProjectSelectorProps> = ({
    onChange,
}) => {
    const { projectName } = useParams();
    const navigate = useNavigate();
    const handleOnClick = React.useCallback(
        (p) => navigate(`/project/${p}`),
        [navigate]
    );

    const [projects, setProjects] = React.useState<string[] | undefined>();
    const [error, setError] = React.useState<string | undefined>();

    React.useEffect(() => {
        new ProjectApi()
            .getMyProjects()
            .then((projects) => setProjects(projects.data))
            .catch((er) => setError(er.message));

        // empty array means only do the on first load,
        //no props change should cause this to retrigger
    }, []);

    if (error) {
        return <p>An error occurred while getting projects: {error}</p>;
    }

    if (projects === undefined) {
        return <p>Loading projects...</p>;
    }

    return (
        <div>
            <h4>Select a project</h4>
            <Input
                type="select"
                onChange={(event) => {
                    handleOnClick(event.target.value);
                }}
                value={projectName || ""}
            >
                {!projectName && <option value="">Select a project</option>}
                {projects.map((p) => (
                    <option key={p} value={p}>
                        {p}
                    </option>
                ))}
            </Input>
        </div>
    );
};
