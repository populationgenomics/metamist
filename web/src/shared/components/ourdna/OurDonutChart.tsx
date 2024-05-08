// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'
import { Container, Card, CardContent, CardHeader } from 'semantic-ui-react'

import { Chart as ChartJS, ArcElement, Tooltip, Legend } from 'chart.js'
import { Doughnut } from 'react-chartjs-2'

ChartJS.register(ArcElement, Tooltip, Legend)

export const data = {
    labels: ['Walk-ins', 'Events', 'Activities'],
    datasets: [
        {
            label: '# of Samples',
            data: [12, 19, 3],
            backgroundColor: [
                'rgba(233, 199, 30, 0.8)',
                'rgba(191, 0, 61, 0.8)',
                'rgba(161, 202, 56, 0.8)',
            ],
            borderColor: ['rgba(233, 199, 30, 1)', 'rgba(191, 0, 61, 1)', 'rgba(161, 202, 56, 1)'],
            borderWidth: 1,
        },
    ],
}

interface PieChartProps {
    header: string
}

const OurDonutChart: React.FC<PieChartProps> = ({ header }) => {
    return (
        <>
            <Card className="ourdna-tile" style={{ width: '100%', height: '100%' }}>
                <CardContent>
                    <CardHeader className="ourdna-tile-header">{header}</CardHeader>
                    <Container
                        className="ourdna-tile-piechart"
                        style={{ width: '100%', height: '100%' }}
                    >
                        {/* INSERT PIE CHART HERE */}
                        <Doughnut
                            data={data}
                            options={{
                                layout: { padding: 10 },
                                plugins: {
                                    legend: {
                                        position: 'bottom',
                                        labels: {
                                            padding: 20,
                                            font: { family: 'Plus Jakarta Sans' },
                                        },
                                    },
                                },
                            }}
                        />
                    </Container>
                </CardContent>
            </Card>
        </>
    )
}

export default OurDonutChart
