import * as React from 'react'

export interface LinkProps {
    id: string
    projectName: string
    sg_id?: string
    children?: React.ReactNode
    onClick?: React.MouseEventHandler<HTMLAnchorElement> | undefined
}
