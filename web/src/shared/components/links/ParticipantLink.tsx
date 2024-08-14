import * as React from 'react'
import { Link } from 'react-router-dom'
import { LinkProps } from './LinkProps'

export function getParticipantLink(id: string | number, projectName: string) {
    return `/project/${projectName}/participant/${id}`
}

const ParticipantLink: React.FunctionComponent<LinkProps> = ({ id, projectName, children }) => (
    <Link to={getParticipantLink(projectName, id)}>{children || id}</Link>
)

export default ParticipantLink
