import * as React from 'react'
import { Card, Image, Container } from 'semantic-ui-react'

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
    'rgba(191, 0, 61, 0.5)', // OurDNA Red
    'rgba(233, 199, 30, 0.5)', // OurDNA Yellow
    'rgba(161, 202, 56, 0.5)', // OurDNA Green
    'rgba(114, 173, 225, 0.5)', // OurDNA Blue
    'rgba(85, 85, 85, 0.5)', // OurDNA Charcoal
]

const HistogramChart: React.FC<HistogramProps> = ({ icon, header, data }) => {
    const datasets = Object.keys(data).map((key, index) => ({
        label: key,
        data: Object.values(data[key]),
        backgroundColor: OURDNA_COLOURS[index % OURDNA_COLOURS.length],
    }))

    const labels = [
        ...new Set(
            Object.entries(data).reduce(
                (ll: string[], [key, val]) => ll.concat(Object.keys(val)),
                []
            )
        ),
    ]

    const barData = {
        labels,
        datasets,
    }

    return (
        <Card fluid style={{ backgroundColor: 'white' }}>
            <Card.Content>
                <Card.Header>
                    <Image src={icon} alt="Icon" size="mini" spaced="right" />
                    {header}
                </Card.Header>
                <Card.Description>
                    <Container style={{ position: 'relative' }}>
                        <Bar data={barData} options={chartOptions} />
                    </Container>
                </Card.Description>
            </Card.Content>
        </Card>
    )
}

export default HistogramChart
