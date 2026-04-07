import { useCallback, useEffect, useRef, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'

const PROJECTS_PARAM = 'projects'
const SEQ_TYPES_PARAM = 'seqTypes'

function parseCommaSeparated(param: string | null): string[] {
    if (!param) return []
    return param.split(',').filter(Boolean)
}

function encodeCommaSeparated(values: string[]): string {
    return values.join(',')
}

interface UseInsightsUrlStateResult {
    selectedProjects: string[]
    setSelectedProjects: (projects: string[]) => void
    selectedSeqTypes: string[]
    setSelectedSeqTypes: (seqTypes: string[]) => void
}

export function useInsightsUrlState(
    availableProjectNames: string[],
    availableSeqTypes: string[]
): UseInsightsUrlStateResult {
    const location = useLocation()
    const navigate = useNavigate()
    const isInitialized = useRef(false)

    const [selectedProjects, setSelectedProjectsState] = useState<string[]>([])
    const [selectedSeqTypes, setSelectedSeqTypesState] = useState<string[]>([])

    // Initialize from URL once available options are loaded
    useEffect(() => {
        if (
            isInitialized.current ||
            availableProjectNames.length === 0 ||
            availableSeqTypes.length === 0
        ) {
            return
        }

        const searchParams = new URLSearchParams(location.search)
        const urlProjects = parseCommaSeparated(searchParams.get(PROJECTS_PARAM))
        const urlSeqTypes = parseCommaSeparated(searchParams.get(SEQ_TYPES_PARAM))

        // Filter to only valid values the user has access to
        const validProjects = urlProjects.filter((p) => availableProjectNames.includes(p))
        const validSeqTypes = urlSeqTypes.filter((st) => availableSeqTypes.includes(st))

        if (validProjects.length > 0) {
            setSelectedProjectsState(validProjects)
        }
        if (validSeqTypes.length > 0) {
            setSelectedSeqTypesState(validSeqTypes)
        }

        isInitialized.current = true
    }, [availableProjectNames, availableSeqTypes, location.search])

    // Write selection to URL
    const updateUrl = useCallback(
        (projects: string[], seqTypes: string[]) => {
            const searchParams = new URLSearchParams(location.search)

            if (projects.length > 0) {
                searchParams.set(PROJECTS_PARAM, encodeCommaSeparated(projects))
            } else {
                searchParams.delete(PROJECTS_PARAM)
            }

            if (seqTypes.length > 0) {
                searchParams.set(SEQ_TYPES_PARAM, encodeCommaSeparated(seqTypes))
            } else {
                searchParams.delete(SEQ_TYPES_PARAM)
            }

            const paramString = searchParams.toString()
            const newUrl = paramString
                ? `${location.pathname}?${paramString}`
                : location.pathname
            navigate(newUrl, { replace: true })
        },
        [location.search, location.pathname, navigate]
    )

    const setSelectedProjects = useCallback(
        (projects: string[]) => {
            setSelectedProjectsState(projects)
            updateUrl(projects, selectedSeqTypes)
        },
        [updateUrl, selectedSeqTypes]
    )

    const setSelectedSeqTypes = useCallback(
        (seqTypes: string[]) => {
            setSelectedSeqTypesState(seqTypes)
            updateUrl(selectedProjects, seqTypes)
        },
        [updateUrl, selectedProjects]
    )

    return {
        selectedProjects,
        setSelectedProjects,
        selectedSeqTypes,
        setSelectedSeqTypes,
    }
}
