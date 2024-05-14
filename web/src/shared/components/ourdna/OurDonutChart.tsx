// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'
import { Center, Image, Flex } from '@chakra-ui/react'

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
                maxHeight="50vh"
                padding={6}
                borderWidth="1px"
                borderRadius="lg"
                overflow="hidden"
                flexDirection={'column'}
                width="100%"
                height="100%"
            >
                <Flex alignItems="center" fontSize={['xs', 'sm', 'md', 'lg']} fontWeight="bold">
                    <Image src={icon} alt="Icon" boxSize={['12px', '24px']} mr="2" />
                    {header}
                </Flex>
                <Center width="100%" height="100%">
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
                </Center>
            </Flex>
        </>
    )
}

export default OurDonutChart
