import * as React from 'react'
import { Link } from 'react-router-dom'
import { Icon, Menu, Dropdown, Popup } from 'semantic-ui-react'

// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'
import Searchbar from './Search'
import MuckTheDuck from '../MuckTheDuck'
import SwaggerIcon from '../SwaggerIcon'
import HomeIcon from '@mui/icons-material/Home'
import MenuIcon from '@mui/icons-material/Menu'
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
            },
            {
                title: 'Billing Dashboard',
                url: '/billing/dashboard',
                icon: <TableRowsIcon />,
            }
        ],
    },
    {
        title: 'Swagger',
        url: '/swagger',
        icon: <SwaggerIcon height={22} style={{ marginTop: '2px' }} />,
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
    },
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
    const isDarkMode = theme.theme === 'dark-mode'

    const dropdown = (item: MenuItem) => (
        <Dropdown text={item.title} key={index}>
            <Dropdown.Menu>
                {item.submenu &&
                    item.submenu.map((subitem, subindex) => (
                        <Dropdown.Item
                            as={Link}
                            className="navItem dropitem"
                            to={subitem.url}
                            key={subindex}
                        >
                            {subitem.title}
                        </Dropdown.Item>
                    ))}
            </Dropdown.Menu>
        </Dropdown>
    )

    const popup = (child: React.ReactNode, icon: JSX.Element) => (
        <>
            <span className="d-none d-lg-block navbarLink">{child}</span>
            <span className="d-lg-none navbarIcon">
                <Popup
                    inverted={isDarkMode}
                    className="navPopup"
                    trigger={icon}
                    hoverable
                    position="bottom center"
                >
                    <h5>{child}</h5>
                </Popup>
            </span>
        </>
    )

    return item.submenu ? (
        <Menu.Item className="navItem">{popup(dropdown(item), item.icon)}</Menu.Item>
    ) : (
        <Menu.Item as={Link} className="navItem" to={item.url} key={index}>
            {popup(item.title, item.icon)}
        </Menu.Item>
    )
}

interface NavBarProps {
    fixed?: boolean
}

const NavBar: React.FC<NavBarProps> = ({ fixed }) => (
    <header className="App-header">
        <Menu className="header">
            <Menu.Item as={Link} id="metamist-img" to="/">
                <MuckTheDuck height={28} style={{ marginRight: '5px' }} />
                METAMIST
            </Menu.Item>

            {menuItems.map((item, index) => (
                <MenuItem index={index} item={item} />
            ))}

            <Menu.Menu position="right">
                <Menu.Item>
                    <DarkModeTriButton />
                </Menu.Item>

                <Menu.Item>
                    <Searchbar />
                </Menu.Item>
            </Menu.Menu>
        </Menu>
    </header>
)

export default NavBar
