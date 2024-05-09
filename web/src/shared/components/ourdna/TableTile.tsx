// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'

import { Box, Text, Table, Thead, Tbody, Tfoot, Tr, Th, Td, TableCaption } from '@chakra-ui/react'

interface TableTileProps {
    header: string
    data: object
    columns: Array<string>
}

const Tile: React.FC<TableTileProps> = ({ header, data, columns }) => {
    return (
        <>
            <Box p="6" borderWidth="1px" borderRadius="lg">
                <Text>{header}</Text>
                <Box height="150px" width="300px" overflowY="auto">
                    <Table variant="simple">
                        <Thead position="sticky" top={0} backgroundColor={'white'}>
                            <Tr>
                                {columns &&
                                    columns.map(
                                        (column, index) => <Th key={index}>{column}</Th>,
                                        this
                                    )}
                            </Tr>
                        </Thead>
                        <Tbody>
                            {data &&
                                Object.keys(data).map(
                                    (key, index) => (
                                        <Tr key={`${index}-row`}>
                                            <Td key={`${index}-col1`}>{key}</Td>
                                            <Td key={`${index}-col2`}>{data[key]}</Td>
                                        </Tr>
                                    ),
                                    this
                                )}
                        </Tbody>
                        <Tfoot position="sticky" bottom={0} backgroundColor={'white'}>
                            <Tr>
                                {columns &&
                                    columns.map(
                                        (column, index) => <Th key={index}>{column}</Th>,
                                        this
                                    )}
                            </Tr>
                        </Tfoot>
                    </Table>
                </Box>
            </Box>
        </>
    )
}

export default Tile
