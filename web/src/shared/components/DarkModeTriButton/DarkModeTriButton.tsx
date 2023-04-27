import * as React from 'react'
import './DarkModeTriButton.css'
import LightModeIcon from '@mui/icons-material/LightMode'
import DarkModeIcon from '@mui/icons-material/DarkMode'
import ComputerIcon from '@mui/icons-material/Computer'
import { ThemeContext } from '../ThemeProvider'

const TriButton: React.FunctionComponent = () => {
    const { toggleTheme } = React.useContext(ThemeContext)
    const [selection, updateSelection] = React.useState(
        localStorage.getItem('theme') || 'system-mode'
    )
    return (
        <div className="switch-toggle">
            <input
                id="on"
                name="state-d"
                type="radio"
                onChange={() => {
                    toggleTheme('light-mode')
                    updateSelection('light-mode')
                }}
                checked={selection === 'light-mode'}
            />
            <label htmlFor="on">
                <LightModeIcon />
            </label>

            <input
                id="na"
                name="state-d"
                type="radio"
                onChange={() => {
                    toggleTheme('system-mode')
                    updateSelection('system-mode')
                }}
                checked={selection === 'system-mode'}
            />
            <label htmlFor="na">
                <ComputerIcon />
            </label>

            <input
                id="off"
                name="state-d"
                type="radio"
                onChange={() => {
                    toggleTheme('dark-mode')
                    updateSelection('dark-mode')
                }}
                checked={selection === 'dark-mode'}
            />
            <label htmlFor="off">
                <DarkModeIcon />
            </label>
        </div>
    )
}

export default TriButton
