import * as React from 'react'
import { Link } from 'react-router-dom'
import { LinkProps } from './LinkProps'

const SequencingGroupLink: React.FunctionComponent<LinkProps> = ({ id, sg_id, children }) => (
    <Link to={`/sample/${id}/${sg_id}`}>{children || id}</Link>
)

export default SequencingGroupLink
