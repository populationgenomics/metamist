import * as React from 'react'
import { Card, Image, Container } from 'semantic-ui-react'

import { Chart as ChartJS, ArcElement, Tooltip, Legend } from 'chart.js'
import { Doughnut } from 'react-chartjs-2'

ChartJS.register(ArcElement, Tooltip, Legend)

interface PieChartProps {
    header: string
    data: object
    icon: string
}

const OurDonutChart: React.FC<PieChartProps> = ({ header, data, icon }) => {
    const donutData = {
        labels: Object.keys(data),
        datasets: [
            {
                label: '# of Samples',
                data: Object.values(data),
                backgroundColor: [
                    'rgba(233, 199, 30, 0.8)',
                    'rgba(191, 0, 61, 0.8)',
                    'rgba(161, 202, 56, 0.8)',
                ],
                borderColor: [
                    'rgba(233, 199, 30, 1)',
                    'rgba(191, 0, 61, 1)',
                    'rgba(161, 202, 56, 1)',
                ],
                borderWidth: 1,
            },
        ],
    }

    return (
        <Card fluid style={{ height: '100%', backgroundColor: 'white' }}>
            <Card.Content style={{ height: '100%' }}>
                <Card.Header>
                    <Image src={icon} alt="Icon" size="mini" spaced="right" />
                    {header}
                </Card.Header>
                <Card.Description>
                    <Container style={{ position: 'relative' }}>
                        <Doughnut
                            data={donutData}
                            options={{
                                responsive: true,
                                maintainAspectRatio: false,
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
                            style={{ width: '100%', height: '100%' }}
                        />
                    </Container>
                </Card.Description>
            </Card.Content>
        </Card>
    )
}

export default OurDonutChart
