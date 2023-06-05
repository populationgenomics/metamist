import * as React from 'react'
import './DarkModeTriButton.css'
import LightModeIcon from '@mui/icons-material/LightMode'
import DarkModeIcon from '@mui/icons-material/DarkMode'
import ComputerIcon from '@mui/icons-material/Computer'
import { Popup } from 'semantic-ui-react'
import { ThemeContext } from '../../ThemeProvider'

const TriButtonOptions = [
    { theme: 'light-mode', icon: <LightModeIcon />, text: 'Light Theme' },
    { theme: 'system-mode', icon: <ComputerIcon />, text: 'Follow System' },
    { theme: 'dark-mode', icon: <DarkModeIcon />, text: 'Dark Theme' },
]

const DarkModeTriButton: React.FunctionComponent = () => {
    const theme = React.useContext(ThemeContext)
    const [selection, updateSelection] = React.useState(
        localStorage.getItem('theme') || 'system-mode'
    )
    return (
        <div className="switch-toggle">
            {TriButtonOptions.map((option) => (
                <React.Fragment key={option.theme}>
                    <input
                        id={option.theme}
                        type="radio"
                        onChange={() => {
                            theme?.toggleTheme(option.theme)
                            updateSelection(option.theme)
                        }}
                        checked={selection === option.theme}
                    />
                    <label htmlFor={option.theme}>
                        <Popup trigger={option.icon} hoverable position="bottom center">
                            <h5>{option.text}</h5>
                        </Popup>
                    </label>
                </React.Fragment>
            ))}
        </div>
    )
}

export default DarkModeTriButton
