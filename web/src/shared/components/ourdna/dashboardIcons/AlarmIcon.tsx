import React from 'react'
import { Image } from 'semantic-ui-react'

interface IconProps {
    fill?: string
    width?: string
    height?: string
    className?: string
    style?: React.CSSProperties
}

const AlarmIcon: React.FC<IconProps> = ({
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
        <g clipPath="url(#clip0_1_49)">
            <path
                fillRule="evenodd"
                clipRule="evenodd"
                d="M20.4398 11.0518C22.0066 10.2172 22.6001 8.27045 21.7655 6.70369C20.9309 5.13693 18.9842 4.54343 17.4174 5.37805C12.9945 7.73416 9.04791 10.8915 5.7784 14.6893C4.6202 16.0346 4.77191 18.0641 6.11724 19.2223C7.46258 20.3805 9.49207 20.2288 10.6503 18.8834C13.4003 15.6891 16.7198 13.0335 20.4398 11.0518ZM35 65.0004C49.2016 65.0004 60.7143 53.4881 60.7143 39.2864C60.7143 25.0847 49.2016 13.5721 35 13.5721C20.7984 13.5721 9.28576 25.0847 9.28576 39.2864C9.28576 53.4881 20.7984 65.0004 35 65.0004ZM48.2347 6.70369C49.0691 5.13693 51.0161 4.54343 52.5826 5.37805C57.0054 7.73416 60.9521 10.8915 64.2217 14.6893C65.3797 16.0346 65.228 18.0641 63.8827 19.2223C62.5374 20.3805 60.5081 20.2288 59.3497 18.8834C56.6 15.6891 53.2803 13.0335 49.5603 11.0518C47.9934 10.2172 47.4 8.27045 48.2347 6.70369ZM37.6786 26.4286C37.6786 24.9492 36.4793 23.75 35 23.75C33.5207 23.75 32.3214 24.9492 32.3214 26.4286V39.2857C32.3214 40.7651 33.5207 41.9643 35 41.9643H45.7143C47.1936 41.9643 48.3929 40.7651 48.3929 39.2857C48.3929 37.8064 47.1936 36.6071 45.7143 36.6071H37.6786V26.4286Z"
                fill={fill}
            />
        </g>
        <defs>
            <clipPath id="clip0_1_49">
                <rect width="60" height="60" fill="white" transform="translate(5 5)" />
            </clipPath>
        </defs>
    </Image>
)

export default AlarmIcon
