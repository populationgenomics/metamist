import * as React from 'react'
import { BrowserRouter as Router, Link } from 'react-router-dom'

// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'
import ExploreIcon from '@mui/icons-material/Explore'
import DescriptionIcon from '@mui/icons-material/Description'
import Routes from './Routes'
import Searchbar from './shared/components/Search'
import MuckTheDuck from './shared/components/MuckTheDuck'
import TriButton from './shared/components/DarkModeTriButton/DarkModeTriButton'

const App: React.FunctionComponent = () => (
    <Router>
        <div className="App">
            <header className="App-header">
                <div className="header">
                    <Link className="metamist-img" to="/">
                        <MuckTheDuck height={28} style={{ marginRight: '5px' }} />
                    </Link>

                    <Link className="metamist" to="/">
                        <span className="d-none d-lg-block">METAMIST</span>
                    </Link>

                    <Link className="navbarLink" to="/project">
                        <span className="d-none d-lg-block">Explore</span>
                        <span className="d-lg-none">
                            <ExploreIcon />
                        </span>
                    </Link>
                    <span className="d-none d-lg-block">
                        <Link className="navbarLink" to="/analysis-runner">
                            Analysis Runner
                        </Link>
                    </span>
                    <span className="d-none d-lg-block">
                        <Link className="navbarLink" to="/swagger">
                            Swagger
                        </Link>
                    </span>
                    <Link className="navbarLink" to="/documentation">
                        <span className="d-none d-lg-block">Docs</span>
                        <span className="d-lg-none">
                            <DescriptionIcon />
                        </span>
                    </Link>
                    <div style={{ marginLeft: 'auto' }}>
                        <TriButton />
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
