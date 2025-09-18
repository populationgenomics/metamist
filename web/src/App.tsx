import { Alert, Snackbar } from '@mui/material'
import * as React from 'react'
import { BrowserRouter as Router } from 'react-router-dom'
import Routes from './Routes'
import NavBar from './shared/components/Header/NavBar'
import { useViewer, ViewerContext } from './viewer'

const App: React.FunctionComponent = () => {
    const { viewer, error, loading } = useViewer()
    const [showAlert, setShowAlert] = React.useState(true)

    return (
        <Router>
            <ViewerContext.Provider value={{viewer, loading, error}}>
                <NavBar />
                <Routes />
                {error && (
                    <Snackbar open={showAlert}>
                        <Alert
                            onClose={() => setShowAlert(false)}
                            severity="error"
                            variant="filled"
                            sx={{ width: '100%' }}
                        >
                            Error loading current user permissions, some interactive actions may be
                            unavailable.
                        </Alert>
                    </Snackbar>
                )}
            </ViewerContext.Provider>
        </Router>
    )
}

export default App
