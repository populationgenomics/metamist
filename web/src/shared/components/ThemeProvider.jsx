import * as React from 'react'
import { useMediaQuery } from 'react-responsive'

const ThemeContext = React.createContext()

const ThemeProvider = (props) => {
    const preferredMode = useMediaQuery({ query: '(prefers-color-scheme: dark)' })
        ? 'dark-mode'
        : 'light-mode'
    const storedTheme = localStorage.getItem('theme') || 'system-mode'
    const savedTheme = storedTheme !== 'system-mode' ? storedTheme : preferredMode
    const [theme, setTheme] = React.useState(savedTheme)

    const toggleTheme = (type) => {
        if (type === 'light-mode') {
            localStorage.setItem('theme', 'light-mode')
            document.documentElement.setAttribute('data-theme', 'light-mode')
            setTheme('light-mode')
        } else if (type === 'dark-mode') {
            localStorage.setItem('theme', 'dark-mode')
            document.documentElement.setAttribute('data-theme', 'dark-mode')
            setTheme('dark-mode')
        } else {
            localStorage.setItem('theme', 'system-mode')
            document.documentElement.setAttribute('data-theme', preferredMode)
            setTheme(preferredMode)
        }
    }

    React.useEffect(() => {
        document.documentElement.setAttribute('data-theme', savedTheme)
        setTheme(savedTheme)
    }, [savedTheme])

    return (
        <div>
            <ThemeContext.Provider value={{ theme, toggleTheme }}>
                {/* eslint-disable react/prop-types */}
                {props.children}
            </ThemeContext.Provider>
        </div>
    )
}

export { ThemeContext, ThemeProvider }
