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
        <Flex height="100%" px="6" borderWidth="1px" borderRadius="lg">
            <Stat height="100%">
                <StatLabel
                    fontSize={['xs', 'sm', 'md', 'lg']}
                    fontWeight="bold"
                    paddingY={['2', '4']}
                >
                    <Flex alignItems="center">
                        <Image src={tile_icon} alt="Icon" boxSize={['12px', '24px']} mr="2" />
                        {header}
                    </Flex>
                </StatLabel>
                <Flex flexDirection={['column', 'column', 'column', 'column', 'row']}>
                    {stats.map((stat, index) => (
                        <Box key={index} paddingX={['0', '3']} paddingY={['3', '0']}>
                            <StatNumber fontSize={['md', 'lg', 'xl']}>{stat.value}</StatNumber>
                            <Badge borderRadius="full" px="2" bg={stat.unitsColour} color="white">
                                {stat.units}
                            </Badge>
                        </Box>
                    ))}
                </Flex>
                <StatHelpText fontSize={['xs', 'sm']} paddingY={['2', '4']}>
                    {description}
                </StatHelpText>
            </Stat>
        </Flex>
    )
}

export default ThreeStatTile
