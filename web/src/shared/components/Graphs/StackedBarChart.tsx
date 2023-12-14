import * as d3 from 'd3'
import _ from 'lodash'
import React from 'react'

export interface IStackedBarChartData {
    date: Date
    values: { [key: string]: number }
}

interface IStackedBarChartProps {
    data?: IStackedBarChartData[]
    accumulate: boolean
}

export const StackedBarChart: React.FC<IStackedBarChartProps> = ({ data, accumulate }) => {
    const colorFunc: (t: number) => string | undefined = d3.interpolateRainbow
    const margin = { top: 50, right: 50, bottom: 100, left: 100 }
    const height = 800 - margin.top - margin.bottom
    const marginLegend = 10

    const containerDivRef = React.useRef<HTMLDivElement>()
    const [width, setWidth] = React.useState(768)

    React.useEffect(() => {
        function updateWindowWidth() {
            setWidth(containerDivRef.current?.clientWidth || 768)
        }
        if (containerDivRef.current) {
            updateWindowWidth()
        }
        window.addEventListener('resize', updateWindowWidth)

        return () => {
            window.removeEventListener('resize', updateWindowWidth)
        }
    }, [])

    if (!data || data.length === 0) {
        return <React.Fragment>No Data</React.Fragment>
    }

    const contDiv = containerDivRef.current
    if (contDiv) {
        // reset svg
        contDiv.innerHTML = ''
    }

    const series = Object.keys(data[0].values)
    const seriesLength = series.length

    // Get the last date in the data array
    const lastDate = data[data.length - 1].date

    // Create 3 new dates
    // TODO make it as custom props
    const newDates = d3
        .range(1, 4)
        .map((day) => new Date(lastDate.getTime() + day * 24 * 60 * 60 * 1000))

    // Interpolate the values for the new dates
    const newValues = newDates.map((date, i) => {
        if (i < data.length) {
            const prevData = data[data.length - 1 - i]
            const nextData = data[data.length - 1 - i]
            return {
                date,
                values: series.reduce((values, key) => {
                    // TODO revisit how we extrapolate new data
                    const interpolator = d3.interpolate(prevData.values[key], nextData.values[key])
                    values[key] = interpolator((i + 1) / 6)
                    return values
                }, {}),
            }
        }
    })

    // Add the new values to the data array
    let extData = data.concat(newValues)
    extData = extData.filter((item) => item !== undefined)

    // X - values
    const x_vals = extData.map((d) => d.date.toISOString().substring(0, 10))

    // prepare stacked data
    let stackedData
    if (accumulate) {
        const accumulatedData = extData.reduce((acc: any[], curr) => {
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
        }, [])

        stackedData = d3
            .stack()
            .offset(d3.stackOffsetNone)
            .keys(series)(accumulatedData.map((d) => ({ date: d.date, ...d.values })))
            .map((ser, i) => ser.map((d) => ({ ...d, key: series[i] })))
    } else {
        stackedData = d3
            .stack()
            .offset(d3.stackOffsetNone)
            .keys(series)(extData.map((d) => ({ date: d.date, ...d.values })))
            .map((ser, i) => ser.map((d) => ({ ...d, key: series[i] })))
    }

    // find max values for the X axes
    const y1Max = d3.max(stackedData, (y) => d3.max(y, (d) => d[1]))

    // tooltip events
    const tooltip = d3.select('body').append('div').attr('id', 'chart').attr('class', 'tooltip')

    const mouseover = (d) => {
        tooltip.style('opacity', 0.8)
        d3.select(this).style('opacity', 0.5)
    }
    const mousemove = (event, d) => {
        const formater = d3.format(',.2f')
        tooltip
            .html(d.key + ' ' + formater(d[1] - d[0]) + ' AUD')
            .style('top', event.pageY - 10 + 'px')
            .style('left', event.pageX + 10 + 'px')
    }
    const mouseleave = (d) => {
        tooltip.style('opacity', 0)
        d3.select(this).style('opacity', 1)
    }

    const x = d3
        .scaleBand()
        .domain(d3.range(x_vals.length))
        .rangeRound([margin.left, width - margin.right])
        .padding(0.08)

    // create root svg element
    const svg = d3
        .select(contDiv)
        .append('svg')
        .attr('viewBox', [0, 0, width, height])
        .attr('height', height)
        .attr('style', 'max-width: 100%; height: auto;')

    // calculate opacity (for new dates)
    const opacity = 0.3
    const calcOpacity = (d) => {
        const idx = series.indexOf(d.key)
        const color = d3.color(colorFunc(idx / seriesLength))
        if (newDates.includes(d.data.date)) {
            return d3.rgb(color.r, color.g, color.b, opacity)
        }

        return color
    }

    // bars
    const rect = svg
        .selectAll('g')
        .data(stackedData)
        .join('g')
        .attr('fill', (d, i) => colorFunc(i / seriesLength))
        .attr('id', (d, i) => `path${i}`)
        .selectAll('rect')
        .data((d) => d)
        .join('rect')
        .attr('x', (d, i) => x(i))
        .attr('y', height - margin.bottom)
        .attr('width', x.bandwidth())
        .attr('height', 0)
        .attr('fill', (d) => calcOpacity(d))
        .on('mouseover', mouseover)
        .on('mousemove', mousemove)
        .on('mouseleave', mouseleave)

    // x-axis & labels
    const formatX = (val: number): string => x_vals[val]

    const x_labels = svg
        .append('g')
        .attr('transform', `translate(0,${height - margin.bottom})`)
        .call(d3.axisBottom(x).tickSizeOuter(0).tickFormat(formatX))

    if (x_vals.length > 10) {
        // rotate x labels, if too many
        x_labels
            .selectAll('text')
            .attr('transform', 'rotate(-90)')
            .attr('text-anchor', 'end')
            .attr('dy', '-0.55em')
            .attr('dx', '-1em')
    }

    // y-axis & labels
    const y = d3
        .scaleLinear()
        .domain([0, y1Max])
        .range([height - margin.bottom, margin.top])

    const y_axis = d3.axisLeft().scale(y).ticks(10, '$.0f')
    svg.append('g').attr('transform', `translate(${margin.left}, 0)`).call(y_axis)

    // animate bars
    rect.transition()
        .duration(200)
        .delay((d, i) => i * 5)
        .attr('y', (d) => y(d[1]))
        .attr('height', (d) => y(d[0]) - y(d[1]))
        .transition()
        .attr('x', (d, i) => x(i))
        .attr('width', x.bandwidth())

    // on Hover
    const onHoverOver = (tg: HTMLElement, v) => {
        d3.selectAll(`#path${v}`).style('fill-opacity', 0.5)
        d3.select(tg).selectAll('circle').style('fill-opacity', 0.5)
        d3.select(tg).selectAll('text').attr('font-weight', 'bold')
    }

    const onHoverOut = (tg: HTMLElement, v) => {
        d3.selectAll(`#path${v}`).style('fill-opacity', 1)
        d3.select(tg).selectAll('circle').style('fill-opacity', 1)
        d3.select(tg).selectAll('text').attr('font-weight', 'normal')
    }

    // add legend
    const svgLegend = d3
        .select(contDiv)
        .append('svg')
        .attr('height', height)
        .attr('viewBox', `0 0 450 ${height}`)

    svgLegend
        .selectAll('g.legend')
        .attr('transform', `translate(0, ${margin.top})`)
        .data(series)
        .enter()
        .append('g')
        .attr('id', (d, i) => `legend${i}`)
        .attr('transform', (d, i) => `translate(${marginLegend},${marginLegend + i * 20})`)
        .each(function (d, i) {
            d3.select(this)
                .append('circle')
                .attr('r', 8)
                .attr('fill', (d) => colorFunc(i / seriesLength))
            d3.select(this)
                .append('text')
                .attr('text-anchor', 'start')
                .attr('x', 10)
                .attr('y', 0)
                .attr('dy', '0.5em')
                .text(d)
                .attr('font-size', '0.8em')
            d3.select(this)
                .on('mouseover', (event, v) => {
                    const element = d3.select(`#legend${i}`)
                    onHoverOver(element.node(), i)
                })
                .on('mouseout', (event, v) => {
                    const element = d3.select(`#legend${i}`)
                    onHoverOut(element.node(), i)
                })
        })

    // set all text to 15px
    svg.selectAll('text').style('font-size', '20px')

    // Simple responsive, move legend to bottom if mobile
    if (width < 1000) {
        // if mobile / tablet size
        svgLegend.attr('width', '100%')
        svg.attr('width', '100%')
    } else {
        svgLegend.attr('width', '30%')
        svg.attr('width', '70%')
    }

    return <div ref={containerDivRef}></div>
}
