import * as React from 'react'
import { Link, LinkProps } from 'react-router-dom'
import { Dropdown, Menu, Popup } from 'semantic-ui-react'

import { BillingApi } from '../../../sm-api'

// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'
import AnalyticsIcon from '@mui/icons-material/Analytics'
import AttachMoneyIcon from '@mui/icons-material/AttachMoney'
import DescriptionIcon from '@mui/icons-material/Description'
import DisplaySettingsIcon from '@mui/icons-material/DisplaySettings'
import ExploreIcon from '@mui/icons-material/Explore'
import InsightsIcon from '@mui/icons-material/Insights'
import PeopleIcon from '@mui/icons-material/People'
import SummarizeIcon from '@mui/icons-material/Summarize'
import TroubleshootIcon from '@mui/icons-material/Troubleshoot'
import MuckTheDuck from '../MuckTheDuck'
import SwaggerIcon from '../SwaggerIcon'
import DarkModeTriButton from './DarkModeTriButton/DarkModeTriButton'
import Searchbar from './Search'

import { billingPages } from '../../../pages/billing/BillingPages'
import { ThemeContext } from '../ThemeProvider'

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
    reloadDocument?: boolean
}
interface MenuItemProps {
    index: number
    item: MenuItem
}

// A link component that reloads the document when clicked
// This is used for the graphiql link which doesn't render
// properly without a reload
const ReloadingLink = (props: LinkProps) => {
    return <Link reloadDocument={true} {...props} />
}

const MenuItem: React.FC<MenuItemProps> = ({ index, item }) => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'

    const MenuLinkComponent = item.reloadDocument ? ReloadingLink : Link

    const dropdown = (item: MenuItem) => (
        <Dropdown id="navDrop" text={item.title} key={index} inverted={isDarkMode}>
            <Dropdown.Menu id="navDropMenu">
                {item.submenu &&
                    item.submenu.map((subitem, subindex) => (
                        <Dropdown.Item
                            id="navDropMenuItem"
                            as={MenuLinkComponent}
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
        <Menu.Item className="headerMenuItem" as={MenuLinkComponent} to={item.url} key={index}>
            {popup(item.title, item.icon)}
        </Menu.Item>
    )
}

const NavBar = () => {
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
            reloadDocument: true,
            icon: <TroubleshootIcon />,
        },
        InsightsPages,
    ])

    React.useEffect(() => {
        new BillingApi().isBillingEnabled().then((response) => {
            if (response.status === 200 && response.data === true) {
                setMenuItems((menuItems) => [
                    ...menuItems.slice(0, 2),
                    billingPagesMenu,
                    ...menuItems.slice(2),
                ])
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
