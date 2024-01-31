import * as React from 'react'
import { Link } from 'react-router-dom'
import { Menu, Dropdown, Popup } from 'semantic-ui-react'

// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'

import ConstructionIcon from '@mui/icons-material/Construction'
import HomeIcon from '@mui/icons-material/Home'
import ExploreIcon from '@mui/icons-material/Explore'
import InsightsIcon from '@mui/icons-material/Insights'
import TableRowsIcon from '@mui/icons-material/TableRows'
import AttachMoneyIcon from '@mui/icons-material/AttachMoney'
import DescriptionIcon from '@mui/icons-material/Description'
import TroubleshootIcon from '@mui/icons-material/Troubleshoot'
import CodeIcon from '@mui/icons-material/Code'
import DarkModeTriButton from './DarkModeTriButton/DarkModeTriButton'

// import { BillingApi } from '../../../sm-api'

import MuckTheDuck from '../MuckTheDuck'
import SwaggerIcon from '../SwaggerIcon'
import { ThemeContext } from '../ThemeProvider'

import Searchbar from './Search'

import './NavBar.css'

const billingPages = {
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
            title: 'Invoice Month Cost',
            url: '/billing/invoiceMonthCost',
            icon: <TableRowsIcon />,
        },
        {
            title: 'Cost By Time',
            url: '/billing/costByTime',
            icon: <TableRowsIcon />,
        },
        {
            title: 'Cost By Analysis',
            url: '/billing/costByAnalysis',
            icon: <TableRowsIcon />,
        },
        {
            title: 'Cost By Category',
            url: '/billing/costByCategory',
            icon: <TableRowsIcon />,
        },
        {
            title: 'Seqr Prop Map',
            url: '/billing/seqrPropMap',
            icon: <TableRowsIcon />,
        },
    ],
}

// const billingPages = {
//     title: 'Billing',
//     url: '/billing',
//     icon: <AttachMoneyIcon />,
//     submenu: [
//         {
//             title: 'Home',
//             url: '/billing',
//             icon: <HomeIcon />,
//         },
//         {
//             title: 'Invoice Month Cost',
//             url: '/billing/invoiceMonthCost',
//             icon: <TableRowsIcon />,
//         },
//         {
//             title: 'Cost By Time',
//             url: '/billing/costByTime',
//             icon: <TableRowsIcon />,
//         },
//         {
//             title: 'Seqr Prop Map',
//             url: '/billing/seqrPropMap',
//             icon: <TableRowsIcon />,
//         },
//     ],
// }

interface MenuItemDetails {
    title: string
    url: string
    icon: JSX.Element
    external?: boolean
    submenu?: MenuItemDetails[]
}

interface MenuItemProps {
    index: number
    item: MenuItemDetails
}

const MenuItem: React.FC<MenuItemProps> = ({ index, item }) => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'

    const dropdown = (i: MenuItemDetails) => (
        <Dropdown text={i.title} key={index} simple>
            <Dropdown.Menu id="navDrop">
                {i.submenu &&
                    i.submenu.map((subitem, subindex) => {
                        if (subitem.external) {
                            return (
                                <Dropdown.Item id="navItem" key={subindex}>
                                    <a href={subitem.url}>{subitem.title}</a>
                                </Dropdown.Item>
                            )
                        }

                        return (
                            <Dropdown.Item as={Link} id="navItem" to={subitem.url} key={subindex}>
                                {subitem.title}
                            </Dropdown.Item>
                        )
                    })}
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

const NavBar: React.FC<NavBarProps> = ({ fixed }) => {
    const menuItems: MenuItemDetails[] = [
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
            title: 'Cohort Builder',
            url: '/cohort-builder',
            icon: <ConstructionIcon />,
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
                    title: 'Invoice Month Cost',
                    url: '/billing/invoiceMonthCost',
                    icon: <TableRowsIcon />,
                },
                {
                    title: 'Cost By Time',
                    url: '/billing/costByTime',
                    icon: <TableRowsIcon />,
                },
                {
                    title: 'Seqr Prop Map',
                    url: '/billing/seqrPropMap',
                    icon: <TableRowsIcon />,
                },
            ],
        },
        {
            title: 'API',
            url: '/api',
            icon: <CodeIcon />,
            submenu: [
                {
                    title: 'Swagger',
                    url: '/swagger',
                    icon: <SwaggerIcon height={22} style={{ marginTop: '2px' }} />,
                },
                {
                    title: 'GraphQL',
                    url: '/graphql',
                    icon: <TroubleshootIcon />,
                    external: true,
                },
            ],
        },
        {
            title: 'Docs',
            url: '/documentation',
            icon: <DescriptionIcon />,
        },
    ]
    // const [menuItems, setMenuItems] = React.useState<MenuItemDetails[]>([
    //     {
    //         title: 'Explore',
    //         url: '/project',
    //         icon: <ExploreIcon />,
    //     },
    //     {
    //         title: 'Analysis Runner',
    //         url: '/analysis-runner',
    //         icon: <InsightsIcon />,
    //     },
    //     {
    //         title: 'Cohort Builder',
    //         url: '/cohort-builder',
    //         icon: <ConstructionIcon />,
    //     },
    //     {
    //         title: 'Billing',
    //         url: '/billing',
    //         icon: <AttachMoneyIcon />,
    //         submenu: [
    //             {
    //                 title: 'Home',
    //                 url: '/billing',
    //                 icon: <HomeIcon />,
    //             },
    //             {
    //                 title: 'Invoice Month Cost',
    //                 url: '/billing/invoiceMonthCost',
    //                 icon: <TableRowsIcon />,
    //             },
    //             {
    //                 title: 'Cost By Time',
    //                 url: '/billing/costByTime',
    //                 icon: <TableRowsIcon />,
    //             },
    //             {
    //                 title: 'Seqr Prop Map',
    //                 url: '/billing/seqrPropMap',
    //                 icon: <TableRowsIcon />,
    //             },
    //         ],
    //     },
    //     {
    //         title: 'Swagger',
    //         url: '/swagger',
    //         icon: <SwaggerIcon height={22} style={{ marginTop: '2px' }} />,
    //     },
    //     {
    //         title: 'Docs',
    //         url: '/documentation',
    //         icon: <DescriptionIcon />,
    //     },
    //     {
    //         title: 'GraphQL',
    //         url: '/graphql',
    //         icon: <TroubleshootIcon />,
    //     },
    // ])

    React.useEffect(() => {
        new BillingApi().isBillingEnabled().then((response) => {
            if (response.status === 200 && response.data === true) {
                setMenuItems([...menuItems.slice(0, 2), billingPages, ...menuItems.slice(2)])
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
