// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'

import { Box, Flex, Image, Text, Table, Thead, Tbody, Tr, Th, Td } from '@chakra-ui/react'

interface TableTileProps {
    header: string
    data: Record<string, string | number>
    columns: Array<string>
    tile_icon: string
}

const Tile: React.FC<TableTileProps> = ({ header, data, columns, tile_icon }) => {
    return (
        <>
            <Flex
                height="100%"
                px="6"
                paddingTop="4"
                paddingBottom="4"
                borderWidth="1px"
                borderRadius="lg"
                flexDirection="column"
                maxHeight="24vh"
                boxShadow="lg"
            >
                <Flex alignItems="center">
                    <Image src={tile_icon} alt="Icon" boxSize="24px" mr="2" />
                    <Text fontSize={['xs', 'sm', 'md']} fontWeight="bold">
                        {header}
                    </Text>
                </Flex>
                <Box overflowY="auto" height="100%">
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
                        <Tbody fontSize={['xs', 'sm']}>
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
            </Flex>
        </>
    )
}

export default Tile
