import React from 'react'

import ToggleSwitch from './ToggleSwitch/ToggleSwitch'
import { ThemeContext } from './ThemeProvider'

const ToggleDarkMode: React.FunctionComponent = () => {
    const { theme, toggleTheme } = React.useContext(ThemeContext)

    const onToggle = () => {
        toggleTheme()
    }

    return (
        <>
            <ToggleSwitch onToggle={onToggle} isActive={theme === 'dark-mode'} />
            <span
                style={{
                    paddingLeft: '10px',
                    paddingRight: '20px',
                    color: '#f2f2f2',
                }}
            >
                Dark Mode
            </span>
        </>
    )
}

export default ToggleDarkMode
