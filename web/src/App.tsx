import * as React from 'react'
import { BrowserRouter as Router, Link } from 'react-router-dom'

// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'
import Routes from './Routes'
import Searchbar from './shared/components/Search'
import MuckTheDuck from './shared/components/MuckTheDuck'
import ToggleDarkMode from './shared/components/ToggleDarkMode'

const App: React.FunctionComponent = () => (
    <Router>
        <div className="App">
            <header className="App-header">
                <div className="header">
                    <Link className="metamist-img" to="/">
                        <MuckTheDuck height={28} style={{ marginRight: '5px' }} />
                    </Link>
                    <Link className="metamist" to="/">
                        METAMIST
                    </Link>
                    <Link className="navbarLink" to="/project">
                        Explore
                    </Link>
                    <Link className="navbarLink" to="/analysis-runner">
                        Analysis Runner
                    </Link>
                    <Link className="navbarLink" to="/swagger">
                        Swagger
                    </Link>
                    <Link className="navbarLink" to="/documentation">
                        Docs
                    </Link>
                    <div style={{ marginLeft: 'auto' }}>
                        <ToggleDarkMode />
                    </div>
                    <Searchbar />
                </div>
            </header>
            <div className="body">
                <Routes />
            </div>
        </div>
    </Router>
)

export default App
