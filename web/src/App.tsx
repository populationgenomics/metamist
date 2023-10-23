import * as React from 'react'
import { BrowserRouter as Router } from 'react-router-dom'

// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'

import Routes from './Routes'
import NavBar from './shared/components/Header/NavBar'
import ResponsiveContainer from './shared/components/ResponsiveContainer'

const App: React.FunctionComponent = () => (
    <Router>
        <div className="App">
            {/* <ResponsiveContainer> */}
            <NavBar />
            <div className="body">
                <Routes />
            </div>
            {/* </ResponsiveContainer> */}
        </div>
    </Router>
)

export default App
