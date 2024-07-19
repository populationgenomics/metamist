import * as React from 'react'
import { Link } from 'react-router-dom'
import { Dropdown, Menu, Popup } from 'semantic-ui-react'

import { BillingApi } from '../../../sm-api'

// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'
import AttachMoneyIcon from '@mui/icons-material/AttachMoney'
import DescriptionIcon from '@mui/icons-material/Description'
import PeopleIcon from '@mui/icons-material/People'
import ExploreIcon from '@mui/icons-material/Explore'
import InsightsIcon from '@mui/icons-material/Insights'
import TableRowsIcon from '@mui/icons-material/TableRows'
import TroubleshootIcon from '@mui/icons-material/Troubleshoot'
import AnalyticsIcon from '@mui/icons-material/Analytics'
import DisplaySettingsIcon from '@mui/icons-material/DisplaySettings'
import SummarizeIcon from '@mui/icons-material/Summarize'
import MuckTheDuck from '../MuckTheDuck'
import SwaggerIcon from '../SwaggerIcon'
import DarkModeTriButton from './DarkModeTriButton/DarkModeTriButton'
import Searchbar from './Search'

import { ThemeContext } from '../ThemeProvider'
import { IBillingPage, billingPages } from '../../../pages/billing/BillingPages'

import './NavBar.css'

const billingPagesMenu = {
    title: 'Billing',
    url: '/billing',
    icon: <AttachMoneyIcon />,
    submenu: billingPages,
}

const InsightsPages = {
    title: 'Insights',
    url: '/insights',
    icon: <AnalyticsIcon />,
    submenu: [
        {
            title: 'Details',
            url: '/insights/details',
            icon: <DisplaySettingsIcon />,
        },
        {
            title: 'Summary',
            url: '/insights/summary',
            icon: <SummarizeIcon />,
        },
    ],
}

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
        <Dropdown id="navDrop" text={item.title} key={index} inverted={isDarkMode}>
            <Dropdown.Menu id="navDropMenu">
                {item.submenu &&
                    item.submenu.map((subitem, subindex) => (
                        <Dropdown.Item
                            id="navDropMenuItem"
                            as={Link}
                            to={subitem.url}
                            key={subindex}
                        >
                            {subitem.icon} <span />
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
                    pinned
                >
                    {child}
                </Popup>
            </span>
        </>
    )

    return item.submenu ? (
        <Menu.Item className="headerMenuItem">{popup(dropdown(item), item.icon)}</Menu.Item>
    ) : (
        <Menu.Item className="headerMenuItem" to={item.url} key={index}>
            {popup(item.title, item.icon)}
        </Menu.Item>
    )
}

interface NavBarProps {
    fixed?: boolean
}

const NavBar: React.FC<NavBarProps> = ({ fixed }) => {
    const [menuItems, setMenuItems] = React.useState<MenuItem[]>([
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
            title: 'OurDNA',
            url: '/ourdna',
            icon: <PeopleIcon />,
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
        InsightsPages,
    ])

    React.useEffect(() => {
        new BillingApi().isBillingEnabled().then((response) => {
            if (response.status === 200 && response.data === true) {
                setMenuItems([...menuItems.slice(0, 2), billingPagesMenu, ...menuItems.slice(2)])
            }
        })
    }, [])

    return (
        <header className="App-header">
            <Menu className="header">
                <Menu.Item as={Link} id="metamist-img" to="/">
                    <MuckTheDuck height={28} style={{ marginRight: '5px' }} />
                    METAMIST
                </Menu.Item>

                {menuItems.map((item, index) => (
                    <MenuItem index={index} item={item} key={index} />
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
}

export default NavBar
