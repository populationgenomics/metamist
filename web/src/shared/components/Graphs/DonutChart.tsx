import { arc, DefaultArcObject, interpolateRainbow, pie, select } from 'd3'
import React from 'react'
import formatMoney from '../../utilities/formatMoney'
import LoadingDucks from '../LoadingDucks/LoadingDucks'

export interface IDonutChartData {
    label: string
    value: number
}

export interface IDonutChartProps {
    id?: string
    data?: IDonutChartData[]
    maxSlices: number
    colors?: (t: number) => string
    isLoading: boolean
    legendSize?: number
    showLegend?: boolean
    maxWidth?: string
}

interface IDonutChartPreparadData {
    index: number
    startAngle: number
    endAngle: number
    data: IDonutChartData
}
interface DataItem {
    label: string
    value: number
    type?: string // Assuming 'type' is optional and a string
}

function calcTranslate(data: IDonutChartPreparadData, move = 4) {
    const moveAngle = data.startAngle + (data.endAngle - data.startAngle) / 2
    return `translate(${-2 * move * Math.cos(moveAngle + Math.PI / 2)}, ${
        -2 * move * Math.sin(moveAngle + Math.PI / 2)
    })`
}

function createViewBox(legSize: number | undefined, w: number) {
    // calculate the viewbox of Legend
    const minX = 0
    const minY = 0
    let width = 200
    let height = 200
    if (legSize) {
        width = legSize * w
        height = legSize * w
    }

    return `${minX} ${minY} ${width} ${height}`
}

export const DonutChart: React.FC<IDonutChartProps> = ({
    id,
    data,
    maxSlices,
    colors,
    isLoading,
    legendSize,
    showLegend,
    maxWidth,
}) => {
    const colorFunc: (t: number) => string | undefined = colors ?? interpolateRainbow
    const duration = 250
    const containerDivRef = React.useRef<HTMLDivElement | null>(null)
    const [graphWidth, setGraphWidth] = React.useState<number>(768)
    // to distinquished between charts on the same page we need an id
    const chartId = id ?? 'donutChart'

    React.useEffect(() => {
        function updateWindowWidth() {
            setGraphWidth(containerDivRef.current?.clientWidth || 768)
        }
        if (containerDivRef.current) {
            updateWindowWidth()
        }
        window.addEventListener('resize', updateWindowWidth)

        return () => {
            window.removeEventListener('resize', updateWindowWidth)
        }
    }, [])

    const onHoverOver = React.useCallback(
        (tg: HTMLElement, v: IDonutChartPreparadData) => {
            select(`#${chartId}-lbl${v.index}`).select('tspan').attr('font-weight', 'bold')
            select(`#${chartId}-legend${v.index}`).attr('font-weight', 'bold')
            select(`#${chartId}-lgd${v.index}`).attr('font-weight', 'bold')
            select(`#${chartId}-lgd${v.index}`).attr('style', 'font-weight: bold')
            select(tg).transition().duration(duration).attr('transform', calcTranslate(v, 6))
            select(tg)
                .select('path')
                .transition()
                .duration(duration)
                .attr('stroke', 'rgba(100, 100, 100, 0.2)')
                .attr('stroke-width', 4)
            select(tg)
        },
        [chartId, duration]
    )

    const onHoverOut = React.useCallback(
        (tg: HTMLElement, v: IDonutChartPreparadData) => {
            select(`#${chartId}-lbl${v.index}`).select('tspan').attr('font-weight', 'normal')
            select(`#${chartId}-legend${v.index}`).attr('font-weight', 'normal')
            select(`#${chartId}-lgd${v.index}`).attr('font-weight', 'normal')
            select(`#${chartId}-lgd${v.index}`).attr('style', 'font-weight: normal')
            select(tg).transition().duration(duration).attr('transform', 'translate(0, 0)')
            select(tg)
                .select('path')
                .transition()
                .duration(duration)
                .attr('stroke', 'white')
                .attr('stroke-width', 1)
        },
        [chartId, duration]
    )

    React.useEffect(() => {
        if (isLoading || !data || data.length === 0 || graphWidth === 0) {
            return
        }

        const contDiv = containerDivRef.current
        if (!contDiv) {
            return
        }

        const width = graphWidth
        const height = width
        const margin = 15
        const radius = Math.min(width, height) / 2 - margin

        // keep order of the slices, declare custom sort function to keep order of slices as passed in
        // by default pie function starts from index 1 and sorts by value
        const pieFnc = pie<DataItem>()
            .value((d) => d.value)
            .sort((a) => {
                if (typeof a === 'object' && a.type === 'inc') {
                    return 1
                }
                return 0 // works both on Safari and Firefox, any other value will break one of them
            })
        const data_ready = pieFnc(data)
        const innerRadius = radius / 1.75 // inner radius of pie, in pixels (non-zero for donut)
        const outerRadius = radius // outer radius of pie, in pixels
        const labelRadius = outerRadius * 0.8 // center radius of labels
        const arcData = arc().innerRadius(innerRadius).outerRadius(outerRadius)
        const arcLabel = arc().innerRadius(labelRadius).outerRadius(labelRadius)

        // reset svg
        contDiv.innerHTML = ''

        // construct svg
        const svg = select(contDiv)
            .append('svg')
            .attr('width', '55%')
            .attr('height', '100%')
            .attr('viewBox', `0 0 ${width} ${width}`)
            .append('g')
            .attr(
                'transform',
                `translate(${Math.min(width, height) / 2}, ${Math.min(width, height) / 2})`
            )

        // Donut partitions
        svg.selectAll('whatever')
            .data(data_ready)
            .enter()
            .append('path')
            .attr('d', (d) => arcData(d as unknown as DefaultArcObject) as string)
            .attr('fill', (d) => colorFunc(d.index / maxSlices) ?? 'black')
            .attr('stroke', '#fff')
            .style('stroke-width', '2')
            .style('opacity', '0.8')
            .style('cursor', 'pointer')
            .attr('id', (d) => `${chartId}-path${d.index}`)
            .on('mouseover', (event, v) => {
                onHoverOver(event.currentTarget, v)
            })
            .on('mouseout', (event, v) => {
                onHoverOut(event.currentTarget, v)
            })
            .append('title')
            .text((d) => `${d.data.label} ${d.data.value}`)
            .style('text-anchor', 'middle')
            .style('font-size', 17)

        // labels
        svg.append('g')
            .attr('font-family', 'sans-serif')
            .attr('font-size', '1.5em')
            .attr('text-anchor', 'middle')
            .selectAll('text')
            .data(data_ready)
            .join('text')
            .attr(
                'transform',
                (d) => `translate(${arcLabel.centroid(d as unknown as DefaultArcObject)})`
            )
            .attr('id', (d) => `${chartId}-lbl${d.index}`)
            .selectAll('tspan')
            .data((d) => {
                const lines = `${formatMoney(d.data.value)}`.split(/\n/)
                return d.endAngle - d.startAngle > 0.25 ? lines : lines.slice(0, 1)
            })
            .join('tspan')
            .attr('x', 0)
            .attr('y', (_, i) => `${i * 2.1}em`)
            .attr('font-weight', (_, i) => (i ? null : 'normal'))
            .text((d) => d)

        // add legend
        if (showLegend === true) {
            const svgLegend = select(contDiv)
                .append('svg')
                .attr('width', '45%')
                .attr('viewBox', createViewBox(legendSize, width))
                .attr('vertical-align', 'top')

            svgLegend
                .selectAll('g.legend')
                .data(data_ready)
                .enter()
                .append('g')
                .attr('transform', (d) => `translate(${margin},${margin + d.index * 20})`)
                .each(function (d) {
                    select(this)
                        .append('circle')
                        .attr('r', 8)
                        .attr('fill', colorFunc(d.index / maxSlices) ?? 'black')
                    select(this)
                        .append('text')
                        .attr('text-anchor', 'start')
                        .attr('x', 20)
                        .attr('y', 0)
                        .attr('dy', '0.35em')
                        .attr('id', `${chartId}-legend${d.index}`)
                        .text(d.data.label)
                        .attr('font-size', '0.9em')
                    select(this)
                        .on('mouseover', () => {
                            const element = select(`#${chartId}-path${d.index}`)
                            onHoverOver(element.node() as HTMLElement, d)
                        })
                        .on('mouseout', () => {
                            const element = select(`#${chartId}-path${d.index}`)
                            onHoverOut(element.node() as HTMLHtmlElement, d)
                        })
                })
        }
    }, [
        data,
        graphWidth,
        isLoading,
        maxSlices,
        colorFunc,
        chartId,
        onHoverOver,
        onHoverOut,
        showLegend,
        legendSize,
    ])

    if (isLoading) {
        return (
            <div>
                <LoadingDucks />
            </div>
        )
    }

    if (!data || data.length === 0) {
        return <>No Data</>
    }

    return <div ref={containerDivRef} style={{ maxWidth: maxWidth }}></div>
}
