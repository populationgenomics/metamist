import { ApolloError, useQuery } from '@apollo/client'
import { createContext } from 'react'
import { gql } from './__generated__/gql'
import { ProjectMemberRole, ViewerQueryQuery } from './__generated__/graphql'

const VIEWER_QUERY = gql(`
    query ViewerQuery {
        viewer {
            username
            projects {
                id
                name
                roles
            }
        }
    }
`)

type ViewerData = ViewerQueryQuery['viewer']
type ProjectIdRoleMap = Map<number, Set<ProjectMemberRole>>
type ProjectNameRoleMap = Map<string, Set<ProjectMemberRole>>

class Viewer {
    private data: ViewerData
    private projectIdRoleMap: ProjectIdRoleMap
    private projectNameRoleMap: ProjectNameRoleMap

    constructor(viewerData: ViewerData) {
        this.data = viewerData

        this.projectIdRoleMap = this.data.projects.reduce((mm: ProjectIdRoleMap, pp) => {
            return mm.set(pp.id, new Set(pp.roles))
        }, new Map())

        this.projectNameRoleMap = this.data.projects.reduce((mm: ProjectNameRoleMap, pp) => {
            return mm.set(pp.name, new Set(pp.roles))
        }, new Map())
    }

    get username() {
        return this.data.username
    }

    get projectRoles() {
        return this.data.projects.map((pp) => ({
            projectName: pp.name,
            projectId: pp.id,
            roles: pp.roles,
        }))
    }

    checkProjectAccessByName(projectName: string, allowedRoles: ProjectMemberRole[]) {
        const projectRoles = this.projectNameRoleMap.get(projectName)
        if (!projectRoles) return false
        return allowedRoles.some((role) => projectRoles.has(role))
    }

    checkProjectAccessById(projectId: number, allowedRoles: ProjectMemberRole[]) {
        const projectRoles = this.projectIdRoleMap.get(projectId)
        if (!projectRoles) return false
        return allowedRoles.some((role) => projectRoles.has(role))
    }
}

export const ViewerContext = createContext<Viewer | null>(null)

export function useViewer(): {
    viewer: Viewer | null
    loading: boolean
    error: ApolloError | undefined
} {
    const { data, loading, error } = useQuery(VIEWER_QUERY)

    return {
        loading,
        error,
        viewer: data ? new Viewer(data.viewer) : null,
    }
}
