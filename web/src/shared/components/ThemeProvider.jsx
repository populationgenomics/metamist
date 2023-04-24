import * as React from 'react'

const ThemeContext = React.createContext()

const ThemeProvider = (props) => {
    const [theme, setTheme] = React.useState(localStorage.getItem('theme') || 'light-mode')
    const toggleTheme = () => {
        if (theme === 'light-mode') {
            localStorage.setItem('theme', 'dark-mode')
            document.documentElement.setAttribute('data-theme', 'dark-mode')

            setTheme('dark-mode')
        } else {
            localStorage.setItem('theme', 'light-mode')
            document.documentElement.setAttribute('data-theme', 'light-mode')
            setTheme('light-mode')
        }
    }

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
