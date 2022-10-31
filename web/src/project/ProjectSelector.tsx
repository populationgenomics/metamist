import * as React from "react";

import { useParams, useNavigate } from "react-router-dom";

import { Dropdown } from "semantic-ui-react";

import { ProjectApi } from "../sm-api/api";

interface ProjectSelectorProps {
    // onChange?: (project: string) => void;
    setPageNumber: React.Dispatch<React.SetStateAction<number>>;
    setPageLimit: React.Dispatch<React.SetStateAction<number>>;
    pageLimit: number;
}

export const ProjectSelector: React.FunctionComponent<ProjectSelectorProps> = ({
    setPageNumber,
    setPageLimit,
    pageLimit,
}) => {
    const { projectName } = useParams();
    const navigate = useNavigate();
    const handleOnClick = React.useCallback(
        (_, { value }) => {
            navigate(`/project/${value}`);
            setPageNumber(1);
            setPageLimit(pageLimit);
        },
        [navigate, setPageLimit, setPageNumber, pageLimit]
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
            <h2>Select a project</h2>
            <Dropdown
                search
                selection
                fluid
                onChange={handleOnClick}
                placeholder="Select a project"
                value={projectName}
                options={projects.map((p) => ({ key: p, text: p, value: p }))}
            />
        </div>
    );
};
