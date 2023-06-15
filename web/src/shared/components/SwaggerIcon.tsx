import * as React from 'react'
import swaggerIcon from '../../swagger-icon.svg'

const SwaggerIcon: React.FunctionComponent<
    React.DetailedHTMLProps<React.ImgHTMLAttributes<HTMLImageElement>, HTMLImageElement>
> = (props) => <img src={swaggerIcon} alt="Muck the duck" {...props} />
export default SwaggerIcon
