import * as React from 'react'
import { Link } from 'react-router-dom'

interface LinkProps {
    id: string
    projectName: string
    children: React.ReactNode
}

export const SampleLink: React.FunctionComponent<LinkProps> = ({ id, projectName, children }) => (
    <Link to={`/project/${projectName}/sample/${id}`}>{children || id}</Link>
)

export const FamilyLink: React.FunctionComponent<LinkProps> = ({ id, projectName, children }) => (
    <Link to={`/project/${projectName}/family/${id}`}>{children || id}</Link>
)

export const ParticipantLink: React.FunctionComponent<LinkProps> = ({
    id,
    projectName,
    children,
}) => <Link to={`/project/${projectName}/participant/${id}`}>{children || id}</Link>
