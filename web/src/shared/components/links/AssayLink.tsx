import * as React from 'react'
import { Link } from 'react-router-dom'

const AssayLink = ({
    id,
    children,
    ...props
}: {
    id: number | string
    project: string
    children?: React.ReactNode
    onClick?: React.MouseEventHandler<HTMLAnchorElement> | undefined
}) => {
    const filter = encodeURIComponent(JSON.stringify({ assay: { id: { eq: id } } }))

    return (
        <Link to={`/project/${props.project}?filter=${filter}`} {...props}>
            {children || id}
        </Link>
    )
}

export default AssayLink
