import * as React from 'react'
import './ToggleSwitch.css'

interface ToggleSwitchProps {
    onToggle: () => void
    isActive: boolean
}

const ToggleSwitch: React.FunctionComponent<ToggleSwitchProps> = ({ onToggle, isActive }) => (
    <>
        <label className="switch">
            <input type="checkbox" onChange={onToggle} checked={isActive} />
            <span className="slider round"></span>
        </label>
    </>
)

export default ToggleSwitch
