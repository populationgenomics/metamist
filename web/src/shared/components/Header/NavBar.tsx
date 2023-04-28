import * as React from 'react'
import { Link } from 'react-router-dom'
import { Popup } from 'semantic-ui-react'

// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'
import ExploreIcon from '@mui/icons-material/Explore'
import DescriptionIcon from '@mui/icons-material/Description'
import InsightsIcon from '@mui/icons-material/Insights'
import Searchbar from './Search'
import MuckTheDuck from '../MuckTheDuck'
import DarkModeTriButton from './DarkModeTriButton/DarkModeTriButton'
import SwaggerIcon from '../SwaggerIcon'
import './NavBar.css'

const NavBar: React.FunctionComponent = () => (
    <header className="App-header">
        <div className="header">
            <Link className="metamist-img" to="/">
                <MuckTheDuck height={28} style={{ marginRight: '5px' }} />
            </Link>

            <Link className="metamist" to="/">
                <span className="d-none d-lg-block">METAMIST</span>
            </Link>

            <Link to="/project">
                <span className="d-none d-lg-block navbarLink">Explore</span>
                <span className="d-lg-none navbarIcon">
                    <Popup trigger={<ExploreIcon />} hoverable position="bottom center">
                        <h5>Explore</h5>
                    </Popup>
                </span>
            </Link>
            <Link to="/analysis-runner">
                <span className="d-none d-lg-block navbarLink">Analysis Runner</span>
                <span className="d-lg-none navbarIcon">
                    <Popup trigger={<InsightsIcon />} hoverable position="bottom center">
                        <h5>Analysis Runner</h5>
                    </Popup>
                </span>
            </Link>
            <Link to="/swagger">
                <span className="d-none d-lg-block navbarLink">Swagger</span>
                <span className="d-lg-none navbarIcon">
                    <Popup
                        trigger={<SwaggerIcon height={22} style={{ marginTop: '2px' }} />}
                        hoverable
                        position="bottom center"
                    >
                        <h5>Swagger</h5>
                    </Popup>
                </span>
            </Link>
            <Link to="/documentation">
                <span className="d-none d-lg-block navbarLink">Docs</span>
                <span className="d-lg-none navbarIcon">
                    <Popup trigger={<DescriptionIcon />} hoverable position="bottom center">
                        <h5>Docs</h5>
                    </Popup>
                </span>
            </Link>
            <div style={{ marginLeft: 'auto' }}>
                <DarkModeTriButton />
            </div>
            <Searchbar />
        </div>
    </header>
)

export default NavBar
