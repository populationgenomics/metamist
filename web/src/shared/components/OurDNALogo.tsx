import * as React from 'react'
import ourDNA from '../../ourdna-logo-rgb.svg'
import ourDNADark from '../../ourdna-white.svg'
import { ThemeContext } from './ThemeProvider'

const OurDNALogo: React.FunctionComponent<
    React.DetailedHTMLProps<React.ImgHTMLAttributes<HTMLImageElement>, HTMLImageElement>
> = (props) => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'

    const ourDNAlogo = isDarkMode ? ourDNADark : ourDNA
    return <img src={ourDNAlogo} alt="OurDNA Logo" {...props} />
}
export default OurDNALogo
