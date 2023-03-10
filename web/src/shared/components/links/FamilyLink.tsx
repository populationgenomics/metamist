import * as React from 'react'
import { Link } from 'react-router-dom'
import { LinkProps } from './LinkProps'

const FamilyLink: React.FunctionComponent<LinkProps> = ({ id, children }) => (
    <Link to={`/family/${id}`}>{children || id}</Link>
)

export default FamilyLink
