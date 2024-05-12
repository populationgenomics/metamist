// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'

import {
    Box,
    Flex,
    Image,
    Text,
    Table,
    Thead,
    Tbody,
    Tfoot,
    Tr,
    Th,
    Td,
    TableCaption,
} from '@chakra-ui/react'

interface TableTileProps {
    header: string
    data: object
    columns: Array<string>
    tile_icon: string
}

const Tile: React.FC<TableTileProps> = ({ header, data, columns, tile_icon }) => {
    return (
        <>
            <Box
                height="100%"
                px="6"
                paddingTop="6"
                paddingBottom="10"
                borderWidth="1px"
                borderRadius="lg"
            >
                <Flex alignItems="center">
                    <Image src={tile_icon} alt="Icon" boxSize="24px" mr="2" />
                    <Text>{header}</Text>
                </Flex>
                <Box height="100%" overflowY="scroll">
                    <Table variant="simple" height="100%">
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
                    </Table>
                </Box>
            </Box>
        </>
    )
}

export default Tile
