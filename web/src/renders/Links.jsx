import * as React from "react";

import { Link } from "react-router-dom";

export const SampleLink = ({ id, projectName, ...props }) => (
    <Link to={`/project/${projectName}/sample/${id}`}>
        {props.children || id}
    </Link>
);

export const FamilyLink = ({ id, projectName, ...props }) => (
    <Link to={`/project/${projectName}/family/${id}`}>
        {props.children || id}
    </Link>
);

export const ParticipantLink = ({ id, projectName, ...props }) => (
    <Link to={`/project/${projectName}/participant/${id}`}>
        {props.children || id}
    </Link>
);
