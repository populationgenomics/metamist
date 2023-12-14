import React from 'react'
import * as d3 from 'd3'
import LoadingDucks from '../LoadingDucks/LoadingDucks'
import { BillingCostBudgetRecord } from '../../../sm-api'
import { grey } from '@mui/material/colors'

interface HorizontalStackedBarChartProps {
    data: BillingCostBudgetRecord[]
    title: string
    series: string[]
    labels: string[]
    total_series: string
    threshold_values: number[]
    threshold_series: string
    sorted_by: string
    colors: (t: number) => string | undefined
    isLoading: boolean
    showLegend: boolean
}

const HorizontalStackedBarChart: React.FC<HorizontalStackedBarChartProps> = ({
    data,
    title,
    series,
    labels,
    total_series,
    threshold_values,
    threshold_series,
    sorted_by,
    colors,
    isLoading,
    showLegend,
}) => {
    if (!data || data.length === 0) {
        return <div>No data available</div>
    }

    const colorFunc: (t: number) => string | undefined = colors ?? d3.interpolateRainbow

    // set the dimensions and margins of the graph
    const margin = { top: 80, right: 20, bottom: 50, left: 250 }
    const width = 650 - margin.left - margin.right
    const outsideHeight = 2850
    const height = outsideHeight - margin.top - margin.bottom

    const containerDivRef = React.useRef<HTMLDivElement>()

    const [clientWidth, setClientWidth] = React.useState(650)

    React.useEffect(() => {
        function updateWindowWidth() {
            setClientWidth(containerDivRef.current?.clientWidth ?? 650)
        }
        if (containerDivRef.current) {
            updateWindowWidth()
        }
        window.addEventListener('resize', updateWindowWidth)

        return () => {
            window.removeEventListener('resize', updateWindowWidth)
        }
    }, [])

    const contDiv = containerDivRef.current
    if (contDiv) {
        // reset svg
        contDiv.innerHTML = ''

        if (isLoading) {
            return (
                <div>
                    <LoadingDucks />
                    <p style={{ textAlign: 'center', marginTop: '5px' }}>
                        <em>This query takes a while...</em>
                    </p>
                </div>
            )
        }

        // prepare data
        let maxTotalSeries = Math.max(...data.map((item) => item[total_series]))
        const typeKeys = data.map((d) => d.field)
        data.sort((a, b) => b[sorted_by] - a[sorted_by])

        // stack the data
        const stack_fnc = d3
            .stack()
            .keys(series)
            .order(d3.stackOrderNone)
            .offset(d3.stackOffsetNone)

        const stackedData = stack_fnc(data)
        const indexedData = stackedData.map((innerArray, outerIdx) =>
            innerArray.map((d, innerIdx) => ({ outerIdx, innerIdx, data: d }))
        )
        const budgetData = data.reduce((acc, d) => {
            acc[d.field] = d.budget
            return acc
        }, {})

        const maxBudget = Math.max(...data.map((item) => item.budget))

        if (showLegend) {
            if (maxBudget > maxTotalSeries) {
                maxTotalSeries = maxBudget * 1.01
            }
        }

        // construct svg
        const svg = d3
            .select(contDiv)
            .append('svg')
            .attr('width', '100%')
            .attr('height', '100%')
            .attr('viewBox', `0 0 650 ${outsideHeight}`)
            .attr('preserveAspectRatio', 'xMinYMin')
            .append('g')
            .attr('transform', `translate(${margin.left}, ${margin.top})`)

        svg.append('defs')
            .append('pattern')
            .attr('id', 'pattern0')
            .attr('patternUnits', 'userSpaceOnUse')
            .attr('width', 4)
            .attr('height', 4)
            .append('path')
            .attr('stroke', '#000000')
            .attr('stroke-width', 1)

        svg.append('defs')
            .append('pattern')
            .attr('id', 'pattern1')
            .attr('patternUnits', 'userSpaceOnUse')
            .attr('width', 4)
            .attr('height', 4)
            .append('path')
            .attr('d', 'M-1,1 l2,-2 M0,4 l4,-4 M3,5 l2,-2')
            .attr('stroke', '#000000')
            .attr('stroke-width', 1)

        svg.append('defs')
            .append('pattern')
            .attr('id', 'pattern2')
            .attr('patternUnits', 'userSpaceOnUse')
            .attr('width', 4)
            .attr('height', 4)
            .append('path')
            .attr('d', 'M 2 0 L 2 4')
            .attr('stroke', '#000000')
            .attr('stroke-width', 1)

        // X scale and Axis
        const formater = d3.format('.1s')
        const xScale = d3.scaleSqrt().domain([0, maxTotalSeries]).range([0, width])

        svg.append('g')
            .attr('transform', `translate(0, ${height})`)
            .call(d3.axisBottom(xScale).ticks(7).tickSize(0).tickPadding(6).tickFormat(formater))
            .call((d) => d.select('.domain').remove())

        // Y scale and Axis
        const yScale = d3
            .scaleBand()
            .domain(data.map((d) => d.field))
            .range([0, height])
            .padding(0.2)

        svg.append('g')
            .style('font-size', '18px') // make the axis labels bigger
            .call(d3.axisLeft(yScale).tickSize(0).tickPadding(5))

        // color palette
        const color = d3.scaleOrdinal().domain(typeKeys).range(['url(#pattern0)', 'url(#pattern1)'])

        const color_fnc = (d) => {
            if (threshold_series === undefined) {
                // if not defiend trhesholds then use the color function
                return colorFunc(d.innerIdx / typeKeys.length)
            }
            if (d.data.data[threshold_series] == null) {
                // no threshold value defined for bar
                return 'grey'
            }
            if (d.data.data[threshold_series] >= threshold_values[0]) {
                return 'red'
            }
            if (d.data.data[threshold_series] >= threshold_values[1]) {
                return 'orange'
            }
            return 'green'
        }

        // set vertical grid line
        const GridLine = () => d3.axisBottom().scale(xScale)

        svg.append('g')
            .attr('class', 'hb-chart-grid')
            .call(GridLine().tickSize(height, 0, 0).tickFormat('').ticks(8))
            .selectAll('line')
            .style('stroke-dasharray', '5,5')

        // create a tooltip
        const tooltip = d3.select('body').append('div').attr('id', 'chart').attr('class', 'tooltip')

        // tooltip events
        const mouseover = (d) => {
            tooltip.style('opacity', 0.8)
            d3.select(this).style('opacity', 0.5)
        }
        const mousemove = (event, d) => {
            const formater = d3.format(',.2f')
            tooltip
                .html(formater(d.data[1] - d.data[0]) + ' AUD')
                .style('top', event.pageY - 10 + 'px')
                .style('left', event.pageX + 10 + 'px')
        }
        const mouseleave = (d) => {
            tooltip.style('opacity', 0)
            d3.select(this).style('opacity', 1)
        }

        // create bars
        svg.append('g')
            .selectAll('g')
            .data(indexedData)
            .join('g')
            .selectAll('rect')
            .data((d) => d)
            .join('rect')
            .attr('x', (d) => xScale(d.data[0]))
            .attr('y', (d) => yScale(d.data.data.field))
            .attr('width', (d) => xScale(d.data[1]) - xScale(d.data[0]))
            .attr('height', yScale.bandwidth())
            .attr('fill', (d) => color_fnc(d))

        svg.append('g')
            .selectAll('g')
            .data(indexedData)
            .join('g')
            .attr('fill', (d) => color(d))
            .selectAll('rect')
            .data((d) => d)
            .join('rect')
            .attr('x', (d) => xScale(d.data[0]))
            .attr('y', (d) => yScale(d.data.data.field))
            .attr('width', (d) => xScale(d.data[1]) - xScale(d.data[0]))
            .attr('height', yScale.bandwidth())
            .on('mouseover', mouseover)
            .on('mousemove', mousemove)
            .on('mouseleave', mouseleave)

        // create bidgetn line
        const budgetFnc = (d) => {
            if (showLegend) {
                return xScale(budgetData[d.data.data.field])
            }
            return 0
        }

        const budgetColor = (d) => {
            const budgetVal = budgetData[d.data.data.field]
            if (showLegend && budgetVal !== null && budgetVal !== undefined) {
                return 'darkcyan'
            }
            return 'rgba(0, 0, 0, 0)'
        }

        svg.append('g')
            .selectAll('g')
            .data(indexedData)
            .join('g')
            .selectAll('rect')
            .data((d) => d)
            .join('rect')
            .attr('x', (d) => budgetFnc(d))
            .attr('y', (d) => yScale(d.data.data.field) - 5)
            .attr('width', (d) => 3)
            .attr('height', yScale.bandwidth() + 10)
            .attr('fill', (d) => budgetColor(d))

        // set title
        svg.append('text')
            .attr('class', 'chart-title')
            .style('font-size', '18px')
            .attr('x', 0)
            .attr('y', -margin.top / 1.7)
            .attr('text-anchor', 'start')
            .text(title)

        // set Y axis label
        svg.append('text')
            .attr('class', 'chart-label')
            .style('font-size', '18px')
            .attr('x', width / 2)
            .attr('y', height + margin.bottom)
            .attr('text-anchor', 'middle')
            .text('AUD')

        if (showLegend) {
            // Legend
            for (let i = 0; i < labels.length; i++) {
                svg.append('rect')
                    .attr('x', 0 + i * 150)
                    .attr('y', -(margin.top / 2.5))
                    .attr('width', 15)
                    .attr('height', 15)
                    .style('fill', `url(#pattern${i})`)

                if (i === 0) {
                    // add background
                    svg.append('rect')
                        .attr('x', 0 + i * 150)
                        .attr('y', -(margin.top / 2.5))
                        .attr('width', 15)
                        .attr('height', 15)
                        .style('fill', 'grey')
                }

                svg.append('text')
                    .attr('class', 'legend')
                    .attr('x', 20 + i * 150)
                    .attr('y', -(margin.top / 3.8))
                    .text(labels[i])
            }

            // add budget bar if defined
            if (maxBudget !== undefined && maxBudget !== null && maxBudget > 0) {
                svg.append('rect')
                    .attr('x', labels.length * 150)
                    .attr('y', -(margin.top / 2.5))
                    .attr('width', 3)
                    .attr('height', 15)
                    .style('fill', 'darkcyan')

                svg.append('text')
                    .attr('class', 'legend')
                    .attr('x', 20 + labels.length * 150)
                    .attr('y', -(margin.top / 3.8))
                    .text('Budget')
            }
        }
    }
    return <div ref={containerDivRef}></div>
}

export { HorizontalStackedBarChart }
