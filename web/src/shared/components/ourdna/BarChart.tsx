// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'
import { Box, Flex, Image, Text } from '@chakra-ui/react'

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
    'rgba(85, 85, 85, 0.5)', // OurDNA Charchoal
]

const HistogramChart = ({ icon, header, data }: HistogramProps) => {
    let datasets = []
    const numColors = OURDNA_COLOURS.length

    const labels = [
        ...new Set(
            Object.entries(data).reduce(
                (ll: string[], [key, val]) => ll.concat(Object.keys(val)),
                []
            )
        ),
    ]

    datasets = Object.keys(data).map((key, index) => {
        let values = Object.values(data[key])
        return {
            label: key,
            data: values,
            backgroundColor: OURDNA_COLOURS[index % numColors],
        }
    }, [])

    const barData = {
        labels,
        datasets,
    }

    return (
        <>
            <Box
                height="100%"
                maxHeight="50vh"
                p="6"
                borderWidth="1px"
                borderRadius="lg"
                overflow="hidden"
                boxShadow="lg"
            >
                <Flex alignItems="center">
                    <Image src={icon} alt="Icon" boxSize="24px" mr="2" />
                    <Text fontSize={['xs', 'sm', 'md']} fontWeight="bold">
                        {header}
                    </Text>
                </Flex>
                <Box height="100%" paddingY={5}>
                    <Bar data={barData} options={chartOptions} style={{ height: '100%' }} />
                </Box>
            </Box>
        </>
    )
}

export default HistogramChart
