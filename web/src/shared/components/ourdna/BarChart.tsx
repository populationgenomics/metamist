// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'
import { Box, Heading } from '@chakra-ui/react'

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
    data: object
}

const OURDNA_COLOURS = [
    'rgba(191, 0, 61, 0.5)', // OurDNA Red
    'rgba(233, 199, 30, 0.5)', // OurDNA Yellow
    'rgba(161, 202, 56, 0.5)', // Green
    'rgba(114, 173, 225, 0.5)', // OurDNA Blue
    'rgba(85, 85, 85, 0.5)', // OurDNA Charchoal
]

const HistogramChart: React.FC<HistogramProps> = ({ header, data }) => {
    let labels = []
    let datasets = []
    const numColors = OURDNA_COLOURS.length

    Object.keys(data).forEach((key) => {
        labels = labels.concat(Object.keys(data[key]))
    })

    labels = [...new Set(labels)]

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
            <Box maxW="4xl" p="6" borderWidth="1px" borderRadius="lg" overflow="hidden">
                {/* INSERT PIE CHART HERE */}
                <Heading size="md">{header}</Heading>
                {/* INSERT PIE CHART HERE */}
                <Bar data={barData} options={options} />
            </Box>
        </>
    )
}

export default HistogramChart
