import * as React from 'react'
import { Link } from 'react-router-dom'

export interface AnalysisLinkProps {
    id?: number | string
    children?: React.ReactNode
    onClick?: React.MouseEventHandler<HTMLAnchorElement> | undefined
}

const AnalysisLink: React.FunctionComponent<AnalysisLinkProps> = ({ id, children, ...props }) => (
    <Link to={`/analysis/${id}`} {...props}>
        {children || id}
    </Link>
)

export default AnalysisLink
