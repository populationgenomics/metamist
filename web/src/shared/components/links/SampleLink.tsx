import * as React from 'react'
import { Link } from 'react-router-dom'
import { LinkProps } from './LinkProps'

const SampleLink: React.FunctionComponent<LinkProps> = ({ id, children }) => (
    <Link to={`/sample/${id}`}>{children || id}</Link>
)

export default SampleLink
