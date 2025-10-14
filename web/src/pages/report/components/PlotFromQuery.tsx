import { Alert, Box } from '@mui/material'
import * as Plot from '@observablehq/plot'
import { Table, TypeMap } from 'apache-arrow'
import { useEffect, useRef } from 'react'
import { useMeasure } from 'react-use'
import { formatQuery, UnformattedQuery } from '../data/formatQuery'
import { useProjectDbQuery } from '../data/projectDatabase'
import ReportItemLoader from './ReportItemLoader'

type PlotInputFunc = (data: Table<TypeMap>) => Omit<Plot.PlotOptions, 'width' | 'height'>

export type PlotProps = {
    project: string
    query: UnformattedQuery
    plot: PlotInputFunc
}

export function PlotFromQuery(props: PlotProps) {
    const containerRef = useRef<HTMLDivElement>(null)

    const { project, plot: getPlotOptions } = props
    const query = formatQuery(props.query)
    const result = useProjectDbQuery(project, query)

    const [measureRef, { width, height }] = useMeasure<HTMLDivElement>()

    const data = result && result.status === 'success' ? result.data : undefined

    useEffect(() => {
        if (!data) return
        const plotOptions = getPlotOptions(data)
        const plot = Plot.plot({
            ...plotOptions,
            width,
            height,
        })
        containerRef.current?.append(plot)
        return () => plot.remove()
    }, [data, width, height, getPlotOptions])

    return (
        <Box>
            <div
                ref={measureRef}
                style={{
                    width: '100%',
                    height: '100%',
                    position: 'absolute',
                    zIndex: -1,
                    top: 0,
                    left: 0,
                }}
            />
            {!result || result.status === 'loading' ? <ReportItemLoader /> : null}
            <div ref={containerRef} />
            {result && result.status === 'error' && (
                <Alert severity="error">{result.errorMessage}</Alert>
            )}
        </Box>
    )
}
