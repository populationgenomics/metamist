import * as React from 'react'
import muck from './muck-the-duck.svg'

const MuckTheDuck: React.FunctionComponent<
    React.DetailedHTMLProps<React.ImgHTMLAttributes<HTMLImageElement>, HTMLImageElement>
> = (props) => <img src={muck} alt="Muck the duck" {...props} />
export default MuckTheDuck
