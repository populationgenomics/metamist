// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'

import { Badge, Box, HStack, Stat, StatLabel, StatNumber, StatHelpText } from '@chakra-ui/react'

interface TileProps {
    header: string
    stat: string
    units: string
    description: string
}

const Tile: React.FC<TileProps> = ({ header, stat, units, description }) => {
    return (
        <>
            <Box p="6" borderWidth="1px" borderRadius="lg" overflow="hidden">
                <Stat>
                    <StatLabel>{header}</StatLabel>
                    <HStack spacing="24px">
                        <StatNumber>{stat}</StatNumber>
                        <Box>
                            <Badge borderRadius="full" px="2" colorScheme="teal">
                                {units}
                            </Badge>
                        </Box>
                    </HStack>
                    <StatHelpText>{description}</StatHelpText>
                </Stat>
            </Box>
        </>
    )
}

export default Tile
