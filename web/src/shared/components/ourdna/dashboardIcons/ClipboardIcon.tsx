import React from 'react'
import { Image } from 'semantic-ui-react'

interface IconProps {
    fill?: string
    width?: string
    height?: string
    className?: string
    style?: React.CSSProperties
}

const ClipboardIcon: React.FC<IconProps> = ({
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
        <path
            fillRule="evenodd"
            clipRule="evenodd"
            d="M28.5714 5C26.2045 5 24.2857 6.91878 24.2857 9.28571V11.4286C24.2857 13.7955 26.2045 15.7143 28.5714 15.7143H41.4286C43.7955 15.7143 45.7143 13.7955 45.7143 11.4286V9.28571C45.7143 6.91878 43.7955 5 41.4286 5H28.5714ZM18.9284 9.28571H16.7857C13.2353 9.28571 10.3571 12.1639 10.3571 15.7143V58.5714C10.3571 62.1217 13.2353 65 16.7857 65H53.2143C56.7646 65 59.6429 62.1217 59.6429 58.5714V15.7143C59.6429 12.1639 56.7646 9.28571 53.2143 9.28571H51.0714V11.4286C51.0714 16.7542 46.754 21.0714 41.4284 21.0714H28.5713C23.2457 21.0714 18.9284 16.7542 18.9284 11.4286V9.28571ZM21.0713 41.4286C21.0713 39.6534 22.5104 38.2143 24.2856 38.2143H45.7142C47.4893 38.2143 48.9286 39.6534 48.9286 41.4286C48.9286 43.2038 47.4893 44.6429 45.7142 44.6429H24.2856C22.5104 44.6429 21.0713 43.2038 21.0713 41.4286Z"
            fill={fill}
        />
    </Image>
)

export default ClipboardIcon
