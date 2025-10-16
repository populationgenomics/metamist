import * as React from 'react'
import { Link } from 'react-router-dom'

export interface ProjectLinkProps {
    name: string
    children?: React.ReactNode
    onClick?: React.MouseEventHandler<HTMLAnchorElement> | undefined
}

const ProjectLink: React.FunctionComponent<ProjectLinkProps> = ({ name, children, ...props }) => (
    <Link to={`/project/${name}`} {...props}>
        {children || name}
    </Link>
)

export default ProjectLink
