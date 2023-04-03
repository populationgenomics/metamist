import * as React from 'react'
import muck from '../../muck-the-duck.svg'
import muckDark from '../../muck-the-dark-duck.svg'
import { useMediaQuery } from 'react-responsive'

const MuckTheDuck: React.FunctionComponent<
    React.DetailedHTMLProps<React.ImgHTMLAttributes<HTMLImageElement>, HTMLImageElement>
> = (props) => {
    const muckDuck = useMediaQuery({ query: '(prefers-color-scheme: dark)' }) ? muckDark : muck
    return <img src={muckDuck} alt="Muck the duck" {...props} />
}
export default MuckTheDuck
