import * as React from 'react'

export interface AppContextState {
    project?: string
    setProject: (project: string) => void
}

export const AppContext = React.createContext<Partial<AppContextState>>({})

// @ts-ignore
export const AppContextProvider = ({ children }) => {

    const [project, setProject] = React.useState<string | undefined>()


    const value: AppContextState = {
        project, setProject
    }

    return <AppContext.Provider value={value}>
        {children}
    </AppContext.Provider>
}
