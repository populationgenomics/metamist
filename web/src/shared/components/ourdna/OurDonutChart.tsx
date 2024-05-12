// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'
import { Box, Heading, Image, Flex } from '@chakra-ui/react'

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
        <>
            <Flex
                height="100%"
                padding={6}
                borderWidth="1px"
                borderRadius="lg"
                overflow="hidden"
                justifyContent={'center'}
            >
                {/* INSERT PIE CHART HERE */}
                <Image src={icon} alt="Icon" boxSize="24px" mr="2" />
                <Heading size="md">{header}</Heading>
                <Doughnut
                    data={donutData}
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
            </Flex>
        </>
    )
}

export default OurDonutChart
