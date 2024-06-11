import React from 'react'
import { Image } from 'semantic-ui-react'

interface IconProps {
    fill?: string
    width?: string
    height?: string
    className?: string
    style?: React.CSSProperties
}

const ClockIcon: React.FC<IconProps> = ({
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
        <g clipPath="url(#clip0_2_343)">
            <path
                fillRule="evenodd"
                clipRule="evenodd"
                d="M35 65C51.5686 65 65 51.5686 65 35C65 18.4315 51.5686 5 35 5C18.4315 5 5 18.4315 5 35C5 51.5686 18.4315 65 35 65ZM37.6786 24.2857C37.6786 22.8064 36.4793 21.6071 35 21.6071C33.5207 21.6071 32.3214 22.8064 32.3214 24.2857V35C32.3214 35.6266 32.5411 36.2334 32.9423 36.7148L43.6565 49.5719C44.6036 50.7084 46.2926 50.8619 47.4291 49.9147C48.5656 48.968 48.719 47.2788 47.772 46.1423L37.6786 34.0302V24.2857Z"
                fill={fill}
            />
        </g>
        <defs>
            <clipPath id="clip0_2_343">
                <rect width="60" height="60" fill="white" transform="translate(5 5)" />
            </clipPath>
        </defs>
    </Image>
)

export default ClockIcon
