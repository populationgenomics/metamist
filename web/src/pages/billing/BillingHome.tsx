import * as React from 'react'

import { ThemeContext } from '../../shared/components/ThemeProvider'

interface IBillingHomeProps {}

const BillingHome: React.FunctionComponent<IBillingHomeProps> = (props: IBillingHomeProps) => {
    const theme = React.useContext(ThemeContext)
    // const isDarkMode = theme.theme === 'dark-mode'

    return (
        <div className="article">
            <h1> Billing Homepage </h1>
            <p>This will probably where we have the main billing documentation</p>
        </div>
    )
}

export default BillingHome
