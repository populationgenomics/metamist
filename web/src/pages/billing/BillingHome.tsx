import * as React from 'react'
import ReactGoogleSlides from 'react-google-slides'

<<<<<<< HEAD
import { IBillingPage, billingPages } from './BillingPages';
import { ThemeContext } from '../../shared/components/ThemeProvider';
import { Button, Icon, Input, Menu, MenuItem, MenuMenu, Segment } from 'semantic-ui-react';
=======
import { IBillingPage, billingPages } from './BillingPages'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import { Icon, Input, Menu, MenuItem, MenuMenu, Segment } from 'semantic-ui-react'
>>>>>>> 1fb68e07 (FIX: add ruff to linter and pass all local pre-commit checks)

// Google Slides
const Slides = React.memo(({ link }: { link: string }) => {
    return <ReactGoogleSlides width="100%" slidesLink={link} position={2} showControls />
})

// Menu component
interface MenuProps {
    inverted: boolean
    activeItem: string
    onItemSelect: (e: any, page: IBillingPage) => void
}

const MenuItems = (props: MenuProps) => {
    const { inverted, activeItem, onItemSelect } = props

    const Descriptions = () => {
        return <>
            {Object.values(billingPages).map((page) => (
                page.name === activeItem && <Segment attached='bottom' inverted={inverted}>
                    <h3><Button size='large' href={page.url}>{page.title}</Button></h3>
                    <p>{page.description}</p>
                </Segment>
            ))}
        </>
    }

    return (
        <>
            <Menu widths={billingPages.length} pointing icon='labeled' attached='top' inverted={inverted} >
                {Object.values(billingPages).map((page) => {
                    return (
                        <MenuItem
                            link
                            name={page.name}
                            active={activeItem === page.name}
                            onClick={(e) => onItemSelect(e, page)}
                        >
                            {page.icon}
                        </MenuItem>
                    )
                })}
            </Menu>
            <Descriptions />
        </>
    )
}

interface IBillingHomeProps { }

const BillingHome: React.FunctionComponent<IBillingHomeProps> = (props: IBillingHomeProps) => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'
    const [activeItem, setActiveItem] = React.useState('home')
    const handleItemClick = (e: any, page: IBillingPage) => {
        console.log('Click item', page.title)
        e.preventDefault()
        e.stopPropagation()
        setActiveItem(page.name)
    }

    const billingSlidesLink = React.useMemo(
        () => 'https://docs.google.com/presentation/d/12cyHMMVx82f5RjJhg_VrGIdQBTCrAWJrMPWAtDLmBxU',
        []
    )

    return (
        <div className="article">
            <h1> Billing Homepage </h1>
            <p>Welcome to the Billing Homepage</p>

            <h2>Pages</h2>
            <p>Here you can find an overview of the different pages in the Billing section</p>
            <MenuItems
                inverted={isDarkMode}
                activeItem={activeItem}
                onItemSelect={handleItemClick}
            />

            <h2>An overview of billing at the CPG</h2>
            <Slides link={billingSlidesLink} />
        </div>
    )
}

export default BillingHome
