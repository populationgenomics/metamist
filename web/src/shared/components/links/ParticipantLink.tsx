import * as React from 'react'
import { Link } from 'react-router-dom'

interface ParticipantLinkProps {
    id: string | number
    children?: React.ReactNode
    onClick?: React.MouseEventHandler<HTMLAnchorElement> | undefined
}

export function getParticipantLink(id: string | number) {
    return `/participant/${id}`
}

const ParticipantLink: React.FunctionComponent<ParticipantLinkProps> = ({
    id,
    children,
    ...props
}) => (
    <Link to={getParticipantLink(id)} {...props}>
        {children || id}
    </Link>
)

export default ParticipantLink
