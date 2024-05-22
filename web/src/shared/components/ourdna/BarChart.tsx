import * as React from 'react'
import { Card, Image, Container } from 'semantic-ui-react'
import * as d3 from 'd3'

import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend,
} from 'chart.js'
import { Bar } from 'react-chartjs-2'

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend)

export const chartOptions = {
    responsive: true,
    plugins: {
        legend: {
            position: 'bottom' as const,
            labels: {
                padding: 20,
            },
        },
        title: {
            display: false,
            text: 'Chart.js Bar Chart',
        },
    },
}

interface HistogramProps {
    icon: string
    header: string
    data: object
}

const OURDNA_COLOURS = [
    'var(--ourdna-red)', // OurDNA Red
    'var(--ourdna-yellow)', // OurDNA Yellow
    'var(--ourdna-green)', // OurDNA Green
    'var(--ourdna-blue)', // OurDNA Blue
    'var(--ourdna-charcoal)', // OurDNA Charcoal
]

const HistogramChart: React.FC<HistogramProps> = ({ icon, header, data }) => {
    const svgRef = React.useRef<SVGSVGElement | null>(null)
    interface TransformedData {
        site: string
        hour: string
        count: number
    }

    const [dimensions, setDimensions] = React.useState<{ width: number; height: number }>({
        width: 0,
        height: 0,
    })

    const transformedData: TransformedData[] = Object.entries(data).flatMap(([site, hours]) =>
        Object.entries(hours).map(([hour, count]) => ({
            site,
            hour,
            count,
        }))
    )

    const groupedData = d3.groups(transformedData, (d) => d.hour)

    React.useEffect(() => {
        // Function to handle window resize events and update dimensions
        const handleResize = () => {
            if (svgRef.current) {
                const { width, height } = svgRef.current.getBoundingClientRect()
                setDimensions({ width, height })
            }
        }

        // Initial call to set dimensions
        handleResize()

        // Add event listener for window resize
        window.addEventListener('resize', handleResize)

        // Cleanup event listener on component unmount
        return () => window.removeEventListener('resize', handleResize)
    }, [])

    React.useEffect(() => {
        if (dimensions.width === 0 || dimensions.height === 0) return

        const svg = d3.select(svgRef.current)
        const margin = { top: 20, right: 20, bottom: 60, left: 60 }
        const width = dimensions.width - margin.left - margin.right
        const height = dimensions.height - margin.top - margin.bottom

        svg.selectAll('*').remove() // Clear previous content

        const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`)

        const x0 = d3
            .scaleBand()
            .domain(transformedData.map((d) => d.hour))
            .rangeRound([0, width])
            .paddingInner(0.1)

        const y = d3
            .scaleLinear()
            .domain([0, d3.max(transformedData, (d) => d.count) || 0])
            .nice()
            .rangeRound([height, 0])

        const color = d3
            .scaleOrdinal<string>()
            .domain(transformedData.map((d) => d.site))
            .range(OURDNA_COLOURS)

        groupedData.forEach(([hour, values]) => {
            if (x0(hour) === undefined) {
                return
            }

            const x1 = d3
                .scaleBand()
                .domain(values.map((d) => d.site))
                .rangeRound([0, x0.bandwidth()])
                .padding(0.05)

            if (Number.isNaN(x0(hour))) {
                return
            }

            g.append('g')
                .attr('transform', `translate(${x0(hour)},0)`)
                .selectAll('rect')
                .data(values)
                .enter()
                .append('rect')
                .attr('x', (d) => {
                    const x = x1(d.site)
                    if (Number.isNaN(x)) {
                        return 0
                    }
                    return x
                })
                .attr('y', (d) => y(d.count))
                .attr('width', x1.bandwidth())
                .attr('height', (d) => height - y(d.count))
                .attr('fill', (d) => color(d.site))
        })

        g.append('g')
            .attr('class', 'axis-label')
            .attr('transform', `translate(0,${height})`)
            .call(d3.axisBottom(x0))
            .selectAll('text')
            .attr('dy', '1em') // Adjust the position of the x-axis labels

        g.append('g')
            .attr('class', 'axis-label')
            .call(d3.axisLeft(y).ticks(y.domain()[1]).tickFormat(d3.format('d')))
            .selectAll('text')
            .attr('dx', '-1em') // Adjust the position of the y-axis labels

        g.append('text')
            .attr('class', 'axis-label')
            .attr('text-anchor', 'end')
            .attr('x', width / 2)
            .attr('y', height + margin.bottom - 15)
            .text('Hours Taken')

        g.append('text')
            .attr('class', 'axis-label')
            .attr('text-anchor', 'end')
            .attr('transform', 'rotate(-90)')
            .attr('x', -height / 2)
            .attr('y', -margin.left + 15)
            .text('Sample Count')

        const legend = g
            .append('g')
            .attr('font-family', 'sans-serif')
            .attr('font-size', 10)
            .attr('text-anchor', 'end')
            .selectAll('g')
            .data(transformedData.map((d) => d.site).filter((v, i, a) => a.indexOf(v) === i))
            .enter()
            .append('g')
            .attr('transform', (d, i) => `translate(0,${i * 20})`)

        legend
            .append('rect')
            .attr('x', width - 19)
            .attr('width', 19)
            .attr('height', 19)
            .attr('fill', (d) => color(d))

        legend
            .append('text')
            .attr('x', width - 24)
            .attr('y', 9.5)
            .attr('dy', '0.32em')
            .text((d) => d)
    }, [transformedData])

    //   return (
    //     <svg ref={svgRef} width="800" height="400"></svg>
    //   );

    return (
        <Card
            fluid
            style={{
                backgroundColor: 'var(--color-bg-card)',
                boxShadow: 'rgba(0, 0, 0, 0.24) 0px 3px 8px',
            }}
        >
            <Card.Content>
                <Card.Header style={{ fontSize: '1.25rem' }}>
                    <Image src={icon} alt="Icon" size="mini" spaced="right" />
                    {header}
                </Card.Header>
                <Card.Description>
                    <Container style={{ position: 'relative' }}>
                        <svg ref={svgRef} width="100%" height="50vh"></svg>
                    </Container>
                </Card.Description>
            </Card.Content>
        </Card>
    )
}

export default HistogramChart
