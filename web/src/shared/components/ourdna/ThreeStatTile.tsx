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

interface ThreeStatTileProps {
    header: string
    stats: { value: string; units: string; unitsColour: string }[]
    tile_icon: string
    description: string
}

const ThreeStatTile: React.FC<ThreeStatTileProps> = ({ header, stats, tile_icon, description }) => {
    return (
        <Box p="6" borderWidth="1px" borderRadius="lg" overflow="hidden">
            <Stat>
                <StatLabel style={{ fontSize: '16px', fontWeight: 'bold', paddingBottom: '10px' }}>
                    <Flex alignItems="center">
                        <Image src={tile_icon} alt="Icon" boxSize="24px" mr="2" />
                        {header}
                    </Flex>
                </StatLabel>
                <HStack spacing="24px">
                    {stats.map((stat, index) => (
                        <Box key={index}>
                            <StatNumber>{stat.value}</StatNumber>
                            <Badge borderRadius="full" px="2" bg={stat.unitsColour} color="white">
                                {stat.units}
                            </Badge>
                        </Box>
                    ))}
                </HStack>
                <StatHelpText>{description}</StatHelpText>
            </Stat>
        </Box>
    )
}

export default ThreeStatTile
