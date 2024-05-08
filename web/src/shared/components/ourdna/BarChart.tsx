// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'
import { Container, Card, CardContent, CardHeader } from 'semantic-ui-react'

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

// export const data = {
//     "Garvan": {
//         "1": 35,
//         "2": 10,
//         "3": 5,
//         "4": 50,
//         "5": 20,
//     },
//     "BBV": {
//         "1": 10,
//         "2": 50,
//         "3": 5,
//         "4": 35,
//         "5": 20,
//     },
//     "Westmead": {
//         "1": 5,
//         "2": 20,
//         "3": 15,
//         "4": 10,
//         "5": 50,
//     },
// };

const labels = ['1 hour', '2 hours', '3 hours', '4 hours', '5 hours']

export const data = {
    labels,
    datasets: [
        {
            label: 'Garvan',
            data: [100, 200, 300, 400, 500],
            backgroundColor: 'rgba(255, 99, 132, 0.5)',
        },
        {
            label: 'BBV',
            data: [20, 80, 150, 380, 420],
            backgroundColor: 'rgba(53, 162, 235, 0.5)',
        },
        {
            label: 'Westmead',
            data: [70, 90, 80, 180, 470],
            backgroundColor: 'rgba(161, 202, 56, 0.5)',
        },
    ],
}

export const options = {
    responsive: true,
    plugins: {
        legend: {
            position: 'bottom' as const,
            labels: {
                padding: 20,
                font: {
                    family: 'Plus Jakarta Sans',
                },
            },
        },
        title: {
            display: true,
            text: 'Chart.js Bar Chart',
        },
    },
}

interface HistogramProps {
    header: string
}

const HistogramChart: React.FC<HistogramProps> = ({ header }) => {
    return (
        <>
            <Card className="ourdna-tile" style={{ width: '100%', height: '100%' }}>
                <CardContent>
                    <CardHeader className="ourdna-tile-header">{header}</CardHeader>
                    <Container
                        className="ourdna-tile-barchart"
                        style={{ width: '100%', height: '100%' }}
                    >
                        {/* INSERT PIE CHART HERE */}
                        <Bar data={data} options={options} />
                    </Container>
                </CardContent>
            </Card>
        </>
    )
}

export default HistogramChart
