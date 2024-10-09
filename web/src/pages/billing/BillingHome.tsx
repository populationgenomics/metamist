import * as React from 'react'
import ReactGoogleSlides from 'react-google-slides'
import { Button, Menu, MenuItem, Segment, SemanticWIDTHS } from 'semantic-ui-react'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import { billingPages, IBillingPage } from './BillingPages'

// Google Slides
const Slides = React.memo(function ReactGoogleSlidesWrapper({ link }: { link: string }) {
    return <ReactGoogleSlides width="100%" slidesLink={link} position={2} showControls />
})

// Menu component
interface MenuProps {
    inverted: boolean
    activeItem: string
    onItemSelect: (e: MouseEvent, page: IBillingPage) => void
}

const MenuItems = (props: MenuProps) => {
    const { inverted, activeItem, onItemSelect } = props

    const Descriptions = () => {
        return (
            <>
                {Object.values(billingPages).map(
                    (page) =>
                        page.name === activeItem && (
                            <Segment attached="bottom" inverted={inverted} key={page.name}>
                                <h3>
                                    <Button size="large" href={page.url}>
                                        {page.title}
                                    </Button>
                                </h3>
                                <p>{page.description}</p>
                            </Segment>
                        )
                )}
            </>
        )
    }

    return (
        <>
            <Menu
                widths={billingPages.length.toString() as SemanticWIDTHS}
                pointing
                icon="labeled"
                attached="top"
                inverted={inverted}
            >
                {Object.values(billingPages).map((page) => {
                    return (
                        <MenuItem
                            key={page.name}
                            link
                            name={page.name}
                            active={activeItem === page.name}
                            onMouseOver={(event: MouseEvent) => onItemSelect(event, page)}
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

const BillingHome = () => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'
    const [activeItem, setActiveItem] = React.useState('home')
    const handleItemClick = (e: MouseEvent, page: IBillingPage) => {
        e.preventDefault()
        e.stopPropagation()
        setActiveItem(page.name)
    }

    const billingSlidesLink = React.useMemo(
        () => 'https://docs.google.com/presentation/d/12cyHMMVx82f5RjJhg_VrGIdQBTCrAWJrMPWAtDLmBxU',
        []
    )

    return (
        <PaddedPage>
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
        </PaddedPage>
    )
}

export default BillingHome
