import * as React from 'react'
import { Link } from 'react-router-dom'

const FamilyLink = ({
    id,
    children,
    ...props
}: {
    id: number | string
    children?: React.ReactNode
    onClick?: React.MouseEventHandler<HTMLAnchorElement> | undefined
}) => (
    <Link to={`/family/${id}`} {...props}>
        {children || id}
    </Link>
)

export default FamilyLink
