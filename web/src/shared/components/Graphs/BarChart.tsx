import { axisBottom, axisLeft, interpolateRainbow, scaleBand, scaleLinear, select } from 'd3'
import React from 'react'
import formatMoney from '../../utilities/formatMoney'
import LoadingDucks from '../LoadingDucks/LoadingDucks'

export interface IData {
    label: string
    value: number
}

interface BarChartProps {
    data: IData[]
    maxSlices: number
    colors?: (t: number) => string | undefined
    isLoading: boolean
}

export const BarChart: React.FC<BarChartProps> = ({ data, maxSlices, colors, isLoading }) => {
    const colorFunc: (t: number) => string | undefined = colors ?? interpolateRainbow
    const margin = React.useMemo(() => ({ top: 50, right: 0, bottom: 150, left: 100 }), [])
    //   const width = 1000 - margin.left - margin.right;
    const height = 400 - margin.top - margin.bottom

    const containerDivRef = React.useRef<HTMLDivElement | null>(null)
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

    React.useEffect(() => {
        if (isLoading || !data || data.length === 0 || width === 0) {
            return
        }

        const contDiv = containerDivRef.current
        if (!contDiv) {
            return
        }

        // reset svg
        contDiv.innerHTML = ''

        const scaleX = scaleBand()
            .domain(data.map(({ label }) => label))
            .range([0, width - margin.left - margin.right])
            .padding(0.5)
        const scaleY = scaleLinear()
            .domain([0, Math.max(...data.map(({ value }) => value))])
            .range([height, 0])

        // construct svg
        const svg = select(contDiv)
            .append('svg')
            .attr('width', `${width + margin.left + margin.right}`)
            .attr('height', `${height + margin.top + margin.bottom}`)
            .append('g')
            .attr('transform', `translate(${margin.left}, ${margin.top})`)

        // Series
        svg.selectAll('whatever')
            .data(data)
            .enter()
            .append('rect')
            .attr('key', (d) => `bar-${d.label}`)
            .attr('x', (d) => scaleX(d.label) ?? 0)
            .attr('y', (d) => scaleY(d.value) ?? 0)
            .attr('id', (d, i) => `rect${i}`)
            .attr('width', scaleX.bandwidth())
            .attr('height', (d) => height - scaleY(d.value))
            .attr('fill', (d, i) => colorFunc(i / maxSlices) ?? 'black')
            .attr('stroke', '#fff')

        // Axis Left
        svg.append('g')
            .call(axisLeft(scaleY))
            .selectAll('text')
            .style('text-anchor', 'end')
            .style('font-size', '1.5em')
            .attr('transform', 'translate(-10, 0)')

        // Axis Bottom
        svg.append('g')
            .attr('transform', `translate(0, ${height})`)
            .call(axisBottom(scaleX))
            .selectAll('text')
            .style('text-anchor', 'end')
            .style('font-size', '1.5em')
            .attr('id', (d, i) => `lgd${i}`)
            .attr('transform', 'translate(-10, 0) rotate(-25)')

        // Labels
        svg.append('g')
            .attr('text-anchor', 'middle')
            .style('font-size', '1.1em')
            .selectAll('text')
            .data(data)
            .join('text')
            .attr('transform', (d) => `translate(${scaleX(d.label)},${scaleY(d.value) - 5})`)
            .attr('dx', '2em')
            .attr('id', (d, i) => `lbl${i}`)
            .selectAll('tspan')
            .data((d) => `${formatMoney(d.value)}`.split(/\n/))
            .join('tspan')
            .attr('font-weight', (_, i) => (i ? null : 'normal'))
            .text((d) => d)
    }, [data, width, isLoading, maxSlices, colorFunc, height, margin])

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

    return <div ref={containerDivRef}></div>
}
