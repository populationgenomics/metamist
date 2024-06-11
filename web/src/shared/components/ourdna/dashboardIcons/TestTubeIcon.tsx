import React from 'react'
import { Image } from 'semantic-ui-react'

interface IconProps {
    fill?: string
    width?: string
    height?: string
    className?: string
    style?: React.CSSProperties
}

const TestTubeIcon: React.FC<IconProps> = ({
    fill,
    width = '100%',
    height = '100%',
    className,
    style,
}) => (
    <Image
        as="svg"
        viewBox="0 0 70 70" // Ensure viewBox covers the SVG's dimensions
        style={{ width, height, ...style }}
        className={className}
    >
        <rect width="70" height="70" fill="none" />
        <g clipPath="url(#clip0_2_252)">
            <path
                fillRule="evenodd"
                clipRule="evenodd"
                d="M16.8605 4.99542C15.0853 4.99542 13.6462 6.43451 13.6462 8.20971C13.6462 9.98493 15.0853 11.424 16.8605 11.424H20V16.4547H30.7143C32.1937 16.4547 33.3929 17.6539 33.3929 19.1333C33.3929 20.6126 32.1937 21.8118 30.7143 21.8118H20V30.1785H30.7143C32.1937 30.1785 33.3929 31.3778 33.3929 32.8571C33.3929 34.3364 32.1937 35.5357 30.7143 35.5357H20V43.9024H30.7143C32.1937 43.9024 33.3929 45.1016 33.3929 46.581C33.3929 48.0603 32.1937 49.2594 30.7143 49.2594H20V50C20 53.9784 21.5803 57.7936 24.3934 60.6067C27.2064 63.4199 31.0217 65 35 65C38.9782 65 42.7936 63.4199 45.6066 60.6067C48.4198 57.7936 50 53.9784 50 50V11.424H53.1393C54.9148 11.424 56.3536 9.98493 56.3536 8.20971C56.3536 6.43451 54.9148 4.99542 53.1393 4.99542H16.8605Z"
                fill={fill}
            />
        </g>
        <defs>
            <clipPath id="clip0_2_252">
                <rect width="60" height="60" fill="white" transform="translate(5 5)" />
            </clipPath>
        </defs>
    </Image>
)

export default TestTubeIcon
