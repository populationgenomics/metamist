// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'

import {
    Badge,
    Box,
    Flex,
    HStack,
    Image,
    Stat,
    StatLabel,
    StatNumber,
    StatHelpText,
} from '@chakra-ui/react'

interface TileProps {
    header: string
    stat: string
    units: string
    units_colour: string
    description: string
    tile_icon: string
}

const Tile: React.FC<TileProps> = ({
    header,
    stat,
    units,
    units_colour,
    description,
    tile_icon,
}) => {
    return (
        <>
            <Box p="6" borderWidth="1px" borderRadius="lg" overflow="hidden">
                <Stat>
                    <StatLabel style={{ fontSize: '16px', fontWeight: 'bold' }}>
                        <Flex alignItems="center">
                            <Image src={tile_icon} alt="Icon" boxSize="24px" mr="2" />
                            {header}
                        </Flex>
                    </StatLabel>
                    <HStack spacing="24px">
                        <StatNumber>{stat}</StatNumber>
                        <Box>
                            <Badge borderRadius="full" px="2" bg={units_colour} color="white">
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
