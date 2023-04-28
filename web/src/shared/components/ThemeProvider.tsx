import * as React from 'react'
import { useMediaQuery } from 'react-responsive'

interface ThemeContextType {
    theme: string
    toggleTheme: (type: string) => void
}

const ThemeContext = React.createContext<ThemeContextType>({} as ThemeContextType)

const ThemeProvider: React.FunctionComponent<{ children?: React.ReactNode }> = ({ children }) => {
    const preferredMode = useMediaQuery({ query: '(prefers-color-scheme: dark)' })
        ? 'dark-mode'
        : 'light-mode'
    const storedTheme = localStorage.getItem('theme') || 'system-mode'
    const savedTheme = storedTheme !== 'system-mode' ? storedTheme : preferredMode
    const [theme, setTheme] = React.useState(savedTheme)

    const toggleTheme = (type: string) => {
        let newTheme = type
        switch (type) {
            case 'system-mode': {
                newTheme = preferredMode
                break
            }
            // no default
        }

        localStorage.setItem('theme', type)
        document.documentElement.setAttribute('data-theme', newTheme)
        setTheme(newTheme)
    }

    React.useEffect(() => {
        document.documentElement.setAttribute('data-theme', savedTheme)
        setTheme(savedTheme)
    }, [savedTheme])

    return (
        <div>
            <ThemeContext.Provider value={{ theme, toggleTheme }}>{children}</ThemeContext.Provider>
        </div>
    )
}

export { ThemeContext, ThemeProvider }
