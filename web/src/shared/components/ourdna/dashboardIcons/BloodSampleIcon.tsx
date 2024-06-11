import React from 'react'
import { Image } from 'semantic-ui-react'

interface IconProps {
    fill?: string
    width?: string
    height?: string
    className?: string
    style?: React.CSSProperties
}

const BloodSampleIcon: React.FC<IconProps> = ({
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
        <g clipPath="url(#clip0_1_7)">
            <path
                fillRule="evenodd"
                clipRule="evenodd"
                d="M35 7.14286L36.7153 5.85851C36.3107 5.31812 35.6751 5 35 5C34.3249 5 33.6893 5.31812 33.2847 5.85851L35 7.14286ZM55.2179 50.672C56.012 48.4117 56.4286 46.013 56.4286 43.5714C56.4286 40.4016 55.0396 36.4242 53.2177 32.4791C51.3581 28.4519 48.8973 24.149 46.4639 20.2259C44.026 16.2955 41.5916 12.7078 39.7686 10.1048C38.8564 8.80241 38.0955 7.74417 37.5617 7.01013C37.2948 6.64306 37.0845 6.35691 36.9403 6.16168L36.7747 5.93821L36.7313 5.87993L36.7198 5.86455L36.7153 5.85851C36.7151 5.85815 36.7153 5.85851 35 7.14286C33.2847 5.85851 33.2849 5.85815 33.2847 5.85851L33.2802 5.86455L33.2687 5.87993L33.2253 5.93821L33.0597 6.16168C32.9155 6.35691 32.7052 6.64306 32.4383 7.01013C31.9045 7.74417 31.1436 8.80241 30.2314 10.1048C28.5267 12.5388 26.2875 15.8338 24.0107 19.465L10.4871 5.94144C9.23187 4.68619 7.1967 4.68619 5.94144 5.94144C4.68619 7.1967 4.68619 9.23187 5.94144 10.4871L59.513 64.0584C60.7683 65.3137 62.8031 65.3137 64.0584 64.0584C65.3137 62.8031 65.3137 60.7683 64.0584 59.513L55.2179 50.672ZM17.975 30.0155L48.3183 60.3586C44.5487 63.3496 39.8594 65 35 65C29.3168 65 23.8664 62.7423 19.8477 58.7236C15.8291 54.7053 13.5714 49.2547 13.5714 43.5714C13.5714 40.4016 14.9605 36.4242 16.7822 32.4791C17.1571 31.6673 17.5564 30.8443 17.975 30.0155Z"
                fill={fill}
            />
        </g>
        <defs>
            <clipPath id="clip0_1_7">
                <rect width="60" height="60" fill="white" transform="translate(5 5)" />
            </clipPath>
        </defs>
    </Image>
)

export default BloodSampleIcon
