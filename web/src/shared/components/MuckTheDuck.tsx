import * as React from 'react'
import muckDark from '../../muck-the-dark-duck.svg'
import muck from '../../muck-the-duck.svg'
import { ThemeContext } from './ThemeProvider'

const MuckTheDuck: React.FunctionComponent<
    React.DetailedHTMLProps<React.ImgHTMLAttributes<HTMLImageElement>, HTMLImageElement>
> = (props) => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'

    const muckDuck = isDarkMode ? muckDark : muck
    return <img src={muckDuck} alt="Muck the duck" {...props} />
}
export default MuckTheDuck
