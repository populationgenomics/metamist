import * as React from 'react'
import { Link } from 'react-router-dom'
import { Popup } from 'semantic-ui-react'

// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'
import ExploreIcon from '@mui/icons-material/Explore'
import DescriptionIcon from '@mui/icons-material/Description'
import InsightsIcon from '@mui/icons-material/Insights'
import BuildIcon from '@mui/icons-material/Build'
import Searchbar from './Search'
import MuckTheDuck from '../MuckTheDuck'
import DarkModeTriButton from './DarkModeTriButton/DarkModeTriButton'
import SwaggerIcon from '../SwaggerIcon'
import './NavBar.css'

const menuItems = [
    {
        title: 'Explore',
        url: '/project',
        icon: <ExploreIcon />,
    },
    {
        title: 'Analysis Runner',
        url: '/analysis-runner',
        icon: <InsightsIcon />,
    },
    {
        title: 'Billing',
        url: '/billing',
        icon: <InsightsIcon />,
    },
    {
        title: 'Swagger',
        url: '/swagger',
        icon: <SwaggerIcon height={22} style={{ marginTop: '2px' }} />
    },
    {
        title: 'Docs',
        url: '/documentation',
        icon: <DescriptionIcon />,
    },
    {
        title: 'GraphQL',
        url: '/graphql',
        icon: <BuildIcon />,
    }
]

const NavBar: React.FunctionComponent = () => (
    <header className="App-header">
        <div className="header">
            <Link className="metamist-img" to="/">
                <MuckTheDuck height={28} style={{ marginRight: '5px' }} />
            </Link>

            <Link className="metamist" to="/">
                <span className="d-none d-lg-block">METAMIST</span>
            </Link>

            {menuItems.map((item, index) => {
                return (
                    <Link to={item.url} key={index}>
                        <span className="d-none d-lg-block navbarLink">{item.title}</span>
                        <span className="d-lg-none navbarIcon">
                            <Popup trigger={<ExploreIcon />} hoverable position="bottom center">
                                <h5>{item.title}</h5>
                            </Popup>
                        </span>
                    </Link>
                )
            })}

            <div style={{ marginLeft: 'auto' }}>
                <DarkModeTriButton />
            </div>
            <Searchbar />
        </div>
    </header>
)

export default NavBar
