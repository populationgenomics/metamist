import * as React from 'react'
import ReactGoogleSlides from 'react-google-slides'
import { Button, Menu, MenuItem, Segment, SemanticWIDTHS, Grid, Header, Container, List, Icon } from 'semantic-ui-react'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import { billingPages, IBillingPage } from './BillingPages'
import SellIcon from '@mui/icons-material/Sell'
import CalendarMonthIcon from '@mui/icons-material/CalendarMonth'
import OpenInNewIcon from '@mui/icons-material/OpenInNew';

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

const url = `https://sample-metadata.populationgenomics.org.au/billing` //TODO make this dynamic

const renderLinkItem = (label: string, href: string) => (
    <List.Item key={label}>
        <a href={href} target="_blank" rel="noreferrer">
            {label} <OpenInNewIcon/>
        </a>
    </List.Item>
)

const renderGridCell = (costByInvoiceProjects: string[], costByInvoiceTopics: string[], costAcrossInvoiceTopics: string[]) => (
    <Grid.Column>
        <Header as="h3">R&D Team</Header>

        <Header as="h5">
            Cost By Invoice Month <CalendarMonthIcon/>
        </Header>

        <List relaxed>
            {costByInvoiceProjects.map((item) =>
                renderLinkItem(
                    `Projects (${item})`,
                    `${url}/invoiceMonthCost?groupBy=gcp_project&gcpProjects=${item}`
                )
            )}
            {costByInvoiceTopics.map((item) =>
                renderLinkItem(
                    `Topics (${item})`,
                    `${url}/invoiceMonthCost?groupBy=topic&topics=${item}`
                )
            )}
        </List>

        <Header as="h5">
            Cost Across Invoice Months <SellIcon />
        </Header>

        <List divided relaxed>
            {costAcrossInvoiceTopics.map((item) =>
                renderLinkItem(
                    `Topics (${item})`,
                    `${url}/costByMonth?groupBy=topic&topics=${item}`
                )
            )}
        </List>
    </Grid.Column>
)

// stores filters for R&D team
const costByInvoiceRandDProjects = ["acute-care,ag-hidden"]
const costByInvoiceRandDTopics = ["acute-care"]
const costAcrossInvoiceRandDTopics = ["acute-care,ag-hidden"]


// stores filters for popGen team
const costByInvoicePopGenProjects = ["acute-care,ag-hidden"]
const costByInvoicePopGenTopics = ["acute-care,ag-hidden"]
const costAcrossInvoicePopGenTopics = ["acute-care,ag-hidden"]

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

                <h2>Useful Shortcuts </h2>
                <p>Billing shortcuts for quick access</p>
                
                <Container fluid style={{padding: "2em 0", margin:'0px'}}>
                    <Grid stackable columns={2} relaxed >
                        {/* R&D Team */}
                        {renderGridCell(costByInvoiceRandDProjects, costByInvoiceRandDTopics, costAcrossInvoiceRandDTopics)}

                        {/* PopGen Team */}
                        {renderGridCell(costByInvoicePopGenProjects, costByInvoicePopGenTopics, costAcrossInvoicePopGenTopics)}
                    </Grid>
                </Container>

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