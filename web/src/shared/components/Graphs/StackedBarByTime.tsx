import * as d3 from 'd3'
import React from 'react'

export interface IStackedBarByTimeData {
    date: Date
    values: { [key: string]: number }
}

interface IStackedBarByTimeProps {
    data?: IStackedBarByTimeData[]
    unit?: string
}

function getSeries(data: IStackedBarByTimeData[]) {
    if (!data || data.length === 0) {
        return []
    }

    const newGroups = new Set<string>()
    data.forEach((dateRecord) => {
        Object.keys(dateRecord.values).forEach((typeKey) => newGroups.add(typeKey))
    })
    return Array.from<string>(newGroups)
}

const colorFunc: (t: number) => string | undefined = d3.interpolateRainbow
const margin = { top: 100, right: 10, bottom: 100, left: 100 }
const height = 800 - margin.top - margin.bottom

export const StackedBarByTime: React.FC<IStackedBarByTimeProps> = ({ data = [], unit = '' }) => {
    const svgRef = React.useRef(null)
    const legendRef = React.useRef(null)

    const containerDivRef = React.useRef<HTMLDivElement | null>(null)
    const tooltipDivRef = React.useRef<HTMLDivElement | null>(null)

    const marginLegend = 10
    const minWidth = 1900

    const [width, setWidth] = React.useState(minWidth)
    const series = React.useMemo(() => getSeries(data), [data])
    const orderedData = React.useMemo(() => {
        return data.toSorted((a, b) => {
            if (a.date < b.date) {
                return -1
            } else if (b.date < a.date) {
                return 1
            }
            return 0
        })
    }, [data])

    React.useEffect(() => {
        if (orderedData.length === 0) {
            return
        }

        // X - values
        const x_vals = orderedData.map((d) => d.date.toISOString().substring(0, 10))

        // prepare stacked data
        let stackedData
        stackedData = d3
            .stack()
            .offset(d3.stackOffsetNone)
            // @ts-ignore
            .keys(series)(orderedData.map((d) => ({ date: d.date, ...d.values })))
            .map((ser, i) => ser.map((d) => ({ ...d, key: series[i] })))

        // find max values for the X axes
        const y1Max = d3.max(stackedData, (y) => d3.max(y, (d) => d[1]))

        // tooltip events
        const tooltip = d3.select(tooltipDivRef.current)

        const mouseover = () => {
            tooltip.style('opacity', 0.8)
        }
        const mousemove = (
            event: MouseEvent,
            d: {
                key: string
                0: number
                1: number
            }
        ) => {
            const formatter = d3.format('')
            tooltip
                .html(d.key + ': ' + formatter(d[1] - d[0]) + ' ' + unit)
                .style('top', event.layerY - 30 + 'px')
                .style('left', event.layerX - 30 + 'px')
        }
        const mouseleave = () => {
            tooltip.style('opacity', 0)
        }

        const x = d3
            .scaleBand()
            // @ts-ignore
            .domain(d3.range(x_vals.length))
            .rangeRound([margin.left, minWidth - margin.right])
            .padding(0.08)

        // Calculate opacity
        const calcOpacity = (d: { key: string; 0: number; 1: number; data: { date: Date } }) => {
            const idx = series.indexOf(d.key)

            // @ts-ignore
            return d3.color(colorFunc(idx / series.length))
        }

        // get SVG reference
        const svg = d3.select(svgRef.current)

        // remove prevously rendered data
        svg.selectAll('g').remove()
        svg.selectAll('rect').remove()

        // generate bars
        const g = svg
            .selectAll('g')
            .data(stackedData)
            .enter()
            .append('g')
            // @ts-ignore
            .attr('fill', (d, i) => colorFunc(i / series.length))
            .attr('id', (d, i) => `path${i}`)

        const rect = g
            .selectAll('rect')
            .data((d) => d)
            .enter()
            .append('rect')
            // @ts-ignore
            .attr('x', (d, i) => x(i))
            .attr('y', height - margin.bottom)
            .attr('width', x.bandwidth())
            .attr('height', 0)
            // @ts-ignore
            .attr('fill', (d) => calcOpacity(d))
            .on('mouseover', mouseover)
            .on('mousemove', mousemove)
            .on('mouseleave', mouseleave)

        // x-axis & labels
        const formatX = (val: number): string => x_vals[val]

        let x_labels: d3.Selection<SVGGElement, unknown, null, undefined> =
            svg.select<SVGGElement>('.x-axis')

        if (x_labels.empty()) {
            x_labels = svg
                .append('g')
                .attr('class', 'x-axis')
                .attr('transform', `translate(0,${height - margin.bottom})`)
                // @ts-ignore
                .call(d3.axisBottom(x).tickSizeOuter(0).tickFormat(formatX))
        } else {
            // @ts-ignore
            x_labels.call(d3.axisBottom(x).tickSizeOuter(0).tickFormat(formatX))
        }

        // rotate x labels, if too many
        if (x_vals.length > 10) {
            x_labels
                .selectAll('text')
                .attr('transform', 'rotate(-90)')
                .attr('text-anchor', 'end')
                .attr('dy', '-0.55em')
                .attr('dx', '-1em')
        } else {
            x_labels
                .selectAll('text')
                .attr('transform', 'rotate(0)')
                .attr('text-anchor', 'middle')
                .attr('dy', '0.55em')
                .attr('dx', '0em')
        }

        // y-axis & labels
        const y = d3
            .scaleLinear()
            // @ts-ignore
            .domain([0, y1Max])
            .range([height - margin.bottom, margin.top])

        let y_labels: d3.Selection<SVGGElement, unknown, null, undefined> =
            svg.select<SVGGElement>('.y-axis')

        if (y_labels.empty()) {
            y_labels = svg
                .append('g')
                .attr('class', 'y-axis')
                .attr('transform', `translate(${margin.left},0)`)
                .call(d3.axisLeft(y))
        } else {
            y_labels.call(d3.axisLeft(y))
        }

        // animate bars
        rect.transition()
            .duration(200)
            .delay((d, i) => i * 5)
            .attr('y', (d) => y(d[1]) || 0)
            .attr('height', (d) => y(d[0]) - y(d[1]))
            .transition()
            // @ts-ignore
            .attr('x', (d, i) => x(i) || 0)
            .attr('width', x.bandwidth())

        // on Hover
        const onHoverOver = (tg: HTMLElement, v: number) => {
            d3.selectAll(`#path${v}`).style('fill-opacity', 0.5)
            d3.select(tg).selectAll('circle').style('fill-opacity', 0.5)
            d3.select(tg).selectAll('text').attr('font-weight', 'bold')
        }

        const onHoverOut = (tg: HTMLElement, v: number) => {
            d3.selectAll(`#path${v}`).style('fill-opacity', 1)
            d3.select(tg).selectAll('circle').style('fill-opacity', 1)
            d3.select(tg).selectAll('text').attr('font-weight', 'normal')
        }

        const svgLegend = d3.select(legendRef.current)

        svgLegend
            .selectAll('g.legend')
            .data(series)
            .join('g')
            .attr('class', 'legend')
            .attr('transform', `translate(0, ${margin.top})`)
            .attr('id', (d, i) => `legend${i}`)
            .attr('transform', (d, i) => `translate(${marginLegend},${marginLegend + i * 20})`)
            .each(function (d, i) {
                d3.select(this)
                    .selectAll('circle') // Replace append with selectAll
                    .data([d]) // Use data to bind a single data element
                    .join('circle') // Use join to handle enter/update/exit selections
                    .attr('r', 8)
                    // @ts-ignore
                    .attr('fill', () => colorFunc(i / series.length))
                d3.select(this)
                    .selectAll('text') // Replace append with selectAll
                    .data([d]) // Use data to bind a single data element
                    .join('text') // Use join to handle enter/update/exit selections
                    .attr('text-anchor', 'start')
                    .attr('x', 10)
                    .attr('y', 0)
                    .attr('dy', '0.5em')
                    .text(d)
                    .attr('font-size', '0.8em')
                d3.select(this)
                    .on('mouseover', () => {
                        const element = d3.select(`#legend${i}`)
                        // @ts-ignore
                        onHoverOver(element.node(), i)
                    })
                    .on('mouseout', () => {
                        const element = d3.select(`#legend${i}`)
                        // @ts-ignore
                        onHoverOut(element.node(), i)
                    })
            })

        // set all text to 2.5em
        svg.selectAll('text').style('font-size', '2.5em')

        function updateWindowWidth() {
            setWidth(containerDivRef.current?.clientWidth || 768)
        }
        if (containerDivRef.current) {
            updateWindowWidth()
        }
        window.addEventListener('resize', updateWindowWidth)
    }, [data, series, series.length])

    if (!data || data.length === 0) {
        return <>No Data</>
    }

    return (
        <>
            <div ref={containerDivRef}>
                <svg
                    ref={svgRef}
                    style={{ maxWidth: '100%', height: 'auto', verticalAlign: 'top' }}
                    height="100%" // {height}
                    viewBox={`0 0 ${minWidth} ${height}`}
                    preserveAspectRatio="xMinYMin"
                    width={width < 1000 ? '100%' : width - 210}
                ></svg>
                <svg
                    ref={legendRef}
                    viewBox={
                        width < 1000
                            ? `-50 0 250 ${50 + 20 * series.length}`
                            : `0 0 200 ${20 * series.length}`
                    }
                    height="100%"
                    width="200px"
                ></svg>
            </div>
            <div id="chart" className="tooltip" ref={tooltipDivRef} />
        </>
    )
}
