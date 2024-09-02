import { BrowserRouter as Router } from 'react-router-dom'
import * as React from 'react'

// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'

import Routes from './Routes'
import NavBar from './shared/components/Header/NavBar'

const App: React.FunctionComponent = () => (
    <Router>
        <div className="App">
            <NavBar />
            <div className="body">
                <Routes />
            </div>
        </div>
    </Router>
)

export default App
