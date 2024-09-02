import * as React from 'react'
import { BrowserRouter as Router } from 'react-router-dom'
import { ViewerContext, useViewer } from './viewer'

// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'

import Routes from './Routes'
import NavBar from './shared/components/Header/NavBar'

const App: React.FunctionComponent = () => {
    const viewer = useViewer()

    return (
        <Router>
            <ViewerContext.Provider value={viewer}>
                <NavBar />
                <Routes />
            </ViewerContext.Provider>
        </Router>
    )
}

export default App
