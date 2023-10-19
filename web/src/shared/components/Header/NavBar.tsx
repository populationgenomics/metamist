import * as React from 'react'
import { Link } from 'react-router-dom'
import { Menu, Dropdown, Popup } from 'semantic-ui-react'

// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'
import Searchbar from './Search'
import MuckTheDuck from '../MuckTheDuck'
import SwaggerIcon from '../SwaggerIcon'
import HomeIcon from '@mui/icons-material/Home'
import ExploreIcon from '@mui/icons-material/Explore'
import InsightsIcon from '@mui/icons-material/Insights'
import TableRowsIcon from '@mui/icons-material/TableRows'
import AttachMoneyIcon from '@mui/icons-material/AttachMoney'
import DescriptionIcon from '@mui/icons-material/Description'
import TroubleshootIcon from '@mui/icons-material/Troubleshoot'
import DarkModeTriButton from './DarkModeTriButton/DarkModeTriButton'

import { ThemeContext } from '../ThemeProvider'

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
        icon: <AttachMoneyIcon />,
        submenu: [
            {
                title: 'Home',
                url: '/billing',
                icon: <HomeIcon />,
            },
            {
                title: 'Billing Data',
                url: '/billing/data',
                icon: <TableRowsIcon />,
            }
        ]
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
        icon: <TroubleshootIcon />,
    }
]

interface MenuItem {
    title: string
    url: string
    icon: JSX.Element
    submenu?: MenuItem[]
}
interface MenuItemProps {
    index: number
    item: MenuItem
}

const MenuItem: React.FC<MenuItemProps> = ({ index, item }) => {
    const theme = React.useContext(ThemeContext)

    return item.submenu ? (
        <Dropdown className="navitem navbarLink" text={item.title} key={index}>
            <Dropdown.Menu>
                {item.submenu.map((subitem, subindex) => (
                    <Dropdown.Item as={Link} to={subitem.url} key={subindex}>
                        <span className="d-none d-lg-block navbarLink">{subitem.title}</span>
                        <span className="d-lg-none navbarIcon">
                            <Popup trigger={subitem.icon} hoverable position="bottom center">
                                <h5>{subitem.title}</h5>
                            </Popup>
                        </span>
                    </Dropdown.Item>
                ))}
            </Dropdown.Menu>

            {/* <span className="d-none d-lg-block navbarLink">{item.title}</span>
            <span className="d-lg-none navbarIcon">
                <Popup trigger={item.icon} hoverable position="bottom center">
                    <h5>{item.title}</h5>
                </Popup>
            </span> */}
        </Dropdown>
    ) : (
        <Menu.Item as={Link} className="navitem" to={item.url} key={index}>
            <span className="d-none d-lg-block navbarLink">{item.title}</span>
            <span className="d-lg-none navbarIcon">
                <Popup trigger={item.icon} hoverable position="bottom center">
                    <h5>{item.title}</h5>
                </Popup>
            </span>
        </Menu.Item>
    )
}

const NavBar: React.FC = () => (
    <header className="App-header">
        <Menu className="header">
            <Menu.Item as={Link} id="metamist-img" to="/">
                <MuckTheDuck height={28} style={{ marginRight: '5px' }} />
            </Menu.Item>

            <Menu.Item as={Link} id="metamist" to="/">
                METAMIST
            </Menu.Item>

            {menuItems.map((item, index) => <MenuItem index={index} item={item} />)}

            <div style={{ marginLeft: 'auto' }}>
                <DarkModeTriButton />
            </div>
            <Searchbar />
        </Menu>
    </header >
)

export default NavBar
