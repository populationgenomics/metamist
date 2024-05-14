// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'

import {
    Badge,
    Box,
    Flex,
    Stack,
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
            <Flex height="100%" px="6" borderWidth="1px" borderRadius="lg">
                <Stat height="100%">
                    <Stack direction={'column'} spacing={[2, 5]}>
                        <StatLabel
                            fontSize={['xs', 'sm', 'md', 'lg']}
                            fontWeight="bold"
                            paddingY={['2', '4']}
                        >
                            <Flex alignItems="center">
                                <Image src={tile_icon} alt="Icon" boxSize="24px" mr="2" />
                                {header}
                            </Flex>
                        </StatLabel>
                        <Flex flexDirection={['column', 'row']}>
                            <StatNumber fontSize={['md', 'lg', 'xl']}>{stat}</StatNumber>
                            <Box paddingX={['0', '3']} paddingY={['3', '0']}>
                                <Badge borderRadius="full" px="2" bg={units_colour} color="white">
                                    {units}
                                </Badge>
                            </Box>
                        </Flex>
                        <StatHelpText fontSize={['xs', 'sm']} paddingY={['2', '4']}>
                            {description}
                        </StatHelpText>
                    </Stack>
                </Stat>
            </Flex>
        </>
    )
}

export default Tile
