import * as d3 from 'd3'
import React from 'react'

export interface IStackedBarChartData {
    date: Date
    values: { [key: string]: number }
}

interface IStackedBarChartProps {
    data?: IStackedBarChartData[]
    accumulate: boolean
    extrapolate: boolean
}

function getSeries(data: IStackedBarChartData[] | undefined) {
    if (!data || data.length === 0) {
        return []
    }

    return Object.keys(data[0].values)
}

function alignToStartOfMonth(date: Date): Date {
    const year = date.getFullYear()
    const month = date.getMonth()
    return new Date(Date.UTC(year, month, 1))
}

/**
 * Creates an array of three new dates, each incremented by a specified number of days from the given last date.
 * If the difference in days is greater than 28, the dates are aligned to the start of their respective months.
 *
 * @param lastDate - The last date from which the new dates will be calculated.
 * @param differenceInDays - The number of days to increment for each new date.
 * @returns An array of three new Date objects.
 */
function createNewDates(lastDate: Date, differenceInDays: number): Date[] {
    const newDates: Date[] = []
    for (let i = 1; i <= 3; i++) {
        const newDate = new Date(lastDate.getTime() + i * differenceInDays * 24 * 60 * 60 * 1000)
        if (differenceInDays > 28) {
            const alignedDate = alignToStartOfMonth(newDate)
            newDates.push(alignedDate)
        } else {
            newDates.push(newDate)
        }
    }
    return newDates
}

function getNewDates(data: IStackedBarChartData[]) {
    // need at least 2 days to extrapolate
    if (!data || data.length < 2) {
        return []
    }

    // Get the last date in the data array
    const lastDate = data[data.length - 1].date
    const prevDate = data[data.length - 2].date

    const timeDifference = Math.abs(lastDate.getTime() - prevDate.getTime())
    const differenceInDays = Math.ceil(timeDifference / (1000 * 3600 * 24))

    // for monthly add 3 extra days so we get the next month
    return createNewDates(lastDate, differenceInDays > 28 ? differenceInDays + 3 : differenceInDays)
}

function prepareData(
    series: string[],
    data: IStackedBarChartData[],
    accumulate: boolean,
    newDates: Date[]
) {
    if (!data || data.length === 0) {
        return []
    }

    const predictedRatio = newDates.length / data.length
    const firstDateData = data[0]
    const lastDateData = data[data.length - 1]

    // Interpolate the values for the new dates
    const newValues = newDates.map((date: Date, i: number) => {
        return {
            date,
            values: series.reduce((acc: Record<string, number>, key: string) => {
                const values = { ...acc }
                const interpolator = d3.interpolate(
                    firstDateData.values[key],
                    lastDateData.values[key]
                )
                const predX = 1 + (i + 1) * predictedRatio
                const predictedValue = interpolator(predX)
                values[key] = predictedValue < 0 ? lastDateData.values[key] : predictedValue
                return values
            }, {}),
        }
    })

    // Add the new values to the data array
    let extData = data.concat(newValues)
    extData = extData.filter((item) => item !== undefined)

    return extData
}

const colorFunc: (t: number) => string | undefined = d3.interpolateRainbow
const margin = { top: 0, right: 10, bottom: 200, left: 100 }
const height = 800 - margin.top - margin.bottom

export const StackedBarChart: React.FC<IStackedBarChartProps> = ({ data, accumulate, extrapolate }) => {
    const svgRef = React.useRef(null)
    const legendRef = React.useRef(null)

    const containerDivRef = React.useRef<HTMLDivElement | null>(null)
    const tooltipDivRef = React.useRef<HTMLDivElement | null>(null)

    const marginLegend = 10
    const minWidth = 1900

    const [width, setWidth] = React.useState(minWidth)
    const series = getSeries(data)
    const seriesCount = series.length

    React.useEffect(() => {
        if (!data || data.length === 0) {
            return
        }

        // Prepare all data structures and predicted data
        const newDates = extrapolate ? getNewDates(data) : []
        const combinedData = prepareData(series, data, accumulate, newDates)

        // X - values
        const x_vals = combinedData.map((d) => d.date.toISOString().substring(0, 10))

        // prepare stacked data
        let stackedData
        if (accumulate) {
            const accumulatedData = combinedData.reduce(
                (acc: { date: Date; values: Record<string, number> }[], curr) => {
                    const last = acc[acc.length - 1]
                    const accumulated = {
                        date: curr.date,
                        values: Object.keys(curr.values).reduce(
                            (accValues: Record<string, number>, key) => {
                                return {
                                    ...accValues,
                                    [key]: (last ? last.values[key] : 0) + curr.values[key],
                                }
                            },
                            {}
                        ),
                    }
                    return [...acc, accumulated]
                },
                []
            )

            stackedData = d3
                .stack()
                .offset(d3.stackOffsetNone)
                // @ts-ignore
                .keys(series)(accumulatedData.map((d) => ({ date: d.date, ...d.values })))
                .map((ser, i) => ser.map((d) => ({ ...d, key: series[i] })))
        } else {
            stackedData = d3
                .stack()
                .offset(d3.stackOffsetNone)
                // @ts-ignore
                .keys(series)(combinedData.map((d) => ({ date: d.date, ...d.values })))
                .map((ser, i) => ser.map((d) => ({ ...d, key: series[i] })))
        }

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
            const formatter = d3.format(',.2f')
            tooltip
                .html(d.key + ' ' + formatter(d[1] - d[0]) + ' AUD')
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

        // calculate opacity (for new dates)
        const opacity = 0.3
        const calcOpacity = (d: { key: string; 0: number; 1: number; data: { date: Date } }) => {
            const idx = series.indexOf(d.key)
            // @ts-ignore
            const color = d3.color(colorFunc(idx / seriesCount))
            if (newDates.includes(d.data.date)) {
                // @ts-ignore
                return d3.rgb(color.r, color.g, color.b, opacity)
            }

            return color
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
            .attr('fill', (d, i) => colorFunc(i / seriesCount))
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
                    .attr('fill', () => colorFunc(i / seriesCount))
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
    }, [data, accumulate, series, seriesCount])

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
                    width={width < 1000 ? '100%' : width - 410}
                ></svg>
                <svg
                    ref={legendRef}
                    viewBox={
                        width < 1000
                            ? `-50 0 450 ${50 + 20 * seriesCount}`
                            : `0 0 400 ${20 * seriesCount}`
                    }
                    height="100%"
                    width="400px"
                ></svg>
            </div>
            <div id="chart" className="tooltip" ref={tooltipDivRef} />
        </>
    )
}
