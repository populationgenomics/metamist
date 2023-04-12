import * as React from 'react'
import { useMediaQuery } from 'react-responsive'
import muck from '../../muck-the-duck.svg'
import muckDark from '../../muck-the-dark-duck.svg'

const MuckTheDuck: React.FunctionComponent<
    React.DetailedHTMLProps<React.ImgHTMLAttributes<HTMLImageElement>, HTMLImageElement>
> = (props) => {
    const muckDuck = useMediaQuery({ query: '(prefers-color-scheme: dark)' }) ? muckDark : muck
    return <img src={muckDuck} alt="Muck the duck" {...props} />
}
export default MuckTheDuck
