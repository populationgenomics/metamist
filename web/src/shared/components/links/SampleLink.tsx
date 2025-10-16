import * as React from 'react'
import { Link } from 'react-router-dom'

const SampleLink = ({
    id,
    children,
}: {
    id: number | string
    children?: React.ReactNode
    onClick?: React.MouseEventHandler<HTMLAnchorElement> | undefined
}) => <Link to={`/sample/${id}`}>{children || id}</Link>

export default SampleLink
