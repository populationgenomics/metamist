import * as React from 'react'
import { Link } from 'react-router-dom'
import { LinkProps } from './LinkProps'

const ParticipantLink: React.FunctionComponent<LinkProps> = ({ id, projectName, children }) => (
    <Link to={`/project/${projectName}/participant/${id}`}>{children || id}</Link>
)

export default ParticipantLink
