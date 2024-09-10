import * as d3 from 'd3'
import * as React from 'react'
import { Card, Container } from 'semantic-ui-react'

import { ourdnaColours } from './Colours'

interface DonutChartProps {
    header: string
    data: { [key: string]: number }
    icon: React.ReactNode
}

const OurDonutChart: React.FC<DonutChartProps> = ({ header, data, icon }) => {
    const svgRef = React.useRef<SVGSVGElement | null>(null)

    const [dimensions, setDimensions] = React.useState<{ width: number; height: number }>({
        width: 0,
        height: 0,
    })

    React.useEffect(() => {
        const handleResize = () => {
            if (svgRef.current) {
                const { width, height } = svgRef.current.getBoundingClientRect()
                setDimensions({ width, height })
            }
        }

        handleResize()

        window.addEventListener('resize', handleResize)
        return () => window.removeEventListener('resize', handleResize)
    }, [])

    React.useEffect(() => {
        if (dimensions.width === 0 || dimensions.height === 0) return

        const svg = d3.select(svgRef.current)
        const margin = { top: 40, right: 40, bottom: 50, left: 40 }
        const width = dimensions.width - margin.left - margin.right
        const height = dimensions.height - margin.top - margin.bottom
        const radius = Math.min(width, height) / 2

        svg.selectAll('*').remove() // Clear previous content

        const g = svg.append('g').attr('transform', `translate(${width / 2},${height / 2})`)

        const color = d3
            .scaleOrdinal()
            .domain(Object.keys(data))
            .range(Object.values(ourdnaColours) || d3.schemeCategory10)

        const pie = d3.pie<any>().value((d: any) => d[1])
        const data_ready = pie(Object.entries(data))

        const arc = d3
            .arc<any>()
            .innerRadius(radius * 0.4)
            .outerRadius(radius * 0.8)

        const tooltip = d3
            .select('body')
            .append('div')
            .style('position', 'absolute')
            .style('visibility', 'hidden')
            .style('background', 'rgba(0, 0, 0, 0.7)')
            .style('color', '#fff')
            .style('padding', '5px 10px')
            .style('border-radius', '4px')
            .style('text-align', 'center')
            .text('')

        g.selectAll('allSlices')
            .data(data_ready)
            .enter()
            .append('path')
            .attr('d', arc)
            .attr('fill', (d) => color(d.data[0]))
            .attr('stroke', 'white')
            .style('stroke-width', '2px')
            .style('opacity', 1)
            .on('mouseover', function (event, d) {
                tooltip.text(`${d.data[0]}: ${d.data[1]}`)
                return tooltip.style('visibility', 'visible')
            })
            .on('mousemove', function (event) {
                return tooltip
                    .style('top', `${event.pageY - 10}px`)
                    .style('left', `${event.pageX + 10}px`)
            })
            .on('mouseout', function () {
                return tooltip.style('visibility', 'hidden')
            })

        // Remove any existing legend to avoid overlap
        d3.selectAll('.legend').remove()

        // Add legends at the bottom
        const legend = svg
            .append('g')
            .attr('class', 'legend')
            .attr('transform', `translate(${0}, ${height / 2 + radius + 40})`)

        // Calculate how many items fit per row dynamically based on width
        const itemsPerRow = Math.floor(width / 120) // Estimate based on width
        const legendItems = legend
            .selectAll('g')
            .data(data_ready)
            .enter()
            .append('g')
            .attr('transform', (d, i) => {
                const row = Math.floor(i / itemsPerRow) // Wrap items based on available space
                const col = i % itemsPerRow
                return `translate(${col * 120}, ${row * 20})`
            })

        legendItems
            .append('rect')
            .attr('width', 18)
            .attr('height', 18)
            .attr('fill', (d) => color(d.data[0]))

        legendItems
            .append('text')
            .attr('x', 24)
            .attr('y', 9)
            .attr('dy', '0.35em')
            .text((d) => d.data[0])
            .style('font-size', '12px')
            .style('fill', 'var(--color-text-primary)')
    }, [data, dimensions])

    return (
        <Card
            fluid
            style={{
                height: '100%',
                backgroundColor: 'var(--color-bg-card)',
                boxShadow: 'var(--color-bg-card-shadow)',
            }}
        >
            <Card.Content style={{ height: '100%' }}>
                <Card.Header className="dashboard-tile" style={{ fontSize: '1.25rem' }}>
                    {icon}
                    {header}
                </Card.Header>
                <Card.Description style={{ height: '100%' }}>
                    <Container style={{ position: 'relative', height: '100%' }}>
                        <svg
                            ref={svgRef}
                            viewBox={`0 0 ${dimensions.width} ${dimensions.height}`}
                            preserveAspectRatio="xMidYMid meet"
                            style={{ width: '100%', height: '100%', minHeight: '300px' }}
                        ></svg>
                    </Container>
                </Card.Description>
            </Card.Content>
        </Card>
    )
}

export default OurDonutChart
