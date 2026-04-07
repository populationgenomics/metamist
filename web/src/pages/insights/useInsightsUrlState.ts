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

    // Refs to track latest values, avoiding stale closures in callbacks
    const selectedProjectsRef = useRef(selectedProjects)
    const selectedSeqTypesRef = useRef(selectedSeqTypes)
    selectedProjectsRef.current = selectedProjects
    selectedSeqTypesRef.current = selectedSeqTypes

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

        const validProjects = urlProjects.filter((p) => availableProjectNames.includes(p))
        const validSeqTypes = urlSeqTypes.filter((st) => availableSeqTypes.includes(st))

        if (validProjects.length > 0) {
            setSelectedProjectsState(validProjects)
        }
        if (validSeqTypes.length > 0) {
            setSelectedSeqTypesState(validSeqTypes)
        }

        isInitialized.current = true
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [availableProjectNames, availableSeqTypes])

    const updateUrl = useCallback(
        (projects: string[], seqTypes: string[]) => {
            const searchParams = new URLSearchParams()

            if (projects.length > 0) {
                searchParams.set(PROJECTS_PARAM, encodeCommaSeparated(projects))
            }

            if (seqTypes.length > 0) {
                searchParams.set(SEQ_TYPES_PARAM, encodeCommaSeparated(seqTypes))
            }

            const paramString = searchParams.toString()
            const newUrl = paramString
                ? `${location.pathname}?${paramString}`
                : location.pathname
            navigate(newUrl, { replace: true })
        },
        [location.pathname, navigate]
    )

    const setSelectedProjects = useCallback(
        (projects: string[]) => {
            setSelectedProjectsState(projects)
            updateUrl(projects, selectedSeqTypesRef.current)
        },
        [updateUrl]
    )

    const setSelectedSeqTypes = useCallback(
        (seqTypes: string[]) => {
            setSelectedSeqTypesState(seqTypes)
            updateUrl(selectedProjectsRef.current, seqTypes)
        },
        [updateUrl]
    )

    return {
        selectedProjects,
        setSelectedProjects,
        selectedSeqTypes,
        setSelectedSeqTypes,
    }
}
