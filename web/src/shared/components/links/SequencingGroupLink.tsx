import * as React from 'react'
import { Link } from 'react-router-dom'

export interface SequencingGroupLinkProps {
    id: string
    sampleId: string
    children?: React.ReactNode
    onClick?: React.MouseEventHandler<HTMLAnchorElement> | undefined
}
const SequencingGroupLink: React.FunctionComponent<SequencingGroupLinkProps> = ({
    id,
    sampleId,
    children,
}) => <Link to={`/sample/${sampleId}/${id}`}>{children || id}</Link>

export default SequencingGroupLink
