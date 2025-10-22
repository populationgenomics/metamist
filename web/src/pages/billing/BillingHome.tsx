import OpenInNewIcon from '@mui/icons-material/OpenInNew'
import * as React from 'react'
import ReactGoogleSlides from 'react-google-slides'
import { Location, useLocation } from 'react-router-dom'
import {
    Button,
    Container,
    Grid,
    Header,
    List,
    Menu,
    MenuItem,
    Segment,
    SemanticWIDTHS,
} from 'semantic-ui-react'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import { BillingApi, BillingProjectGroup, BillingTeamRecord, BillingTopicGroup } from '../../sm-api'
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

const renderLinkItem = (label: string, href: string) => (
    <List.Item key={label}>
        <a href={href} target="_blank" rel="noreferrer">
            {label} <OpenInNewIcon />
        </a>
    </List.Item>
)

const renderGridColumn = (
    teamName: string,
    billingGroupInfoList: (BillingProjectGroup | BillingTopicGroup)[],
    location: Location
) => (
    <Grid.Column>
        <Header as="h3">{teamName} Team</Header>

        <Header as="h5">Cost By Invoice Month by Project</Header>

        <List>
            {billingGroupInfoList
                .filter((item): item is BillingProjectGroup => item.group_by === 'gcp_project')
                .map((item) =>
                    renderLinkItem(
                        `${item.name}`,
                        `${location.pathname}/invoiceMonthCost?groupBy=gcp_project&gcpProjects=${item.gcp_projects}`
                    )
                )}
        </List>

        <Header as="h5">Cost By Invoice Month by Topic</Header>

        <List>
            {billingGroupInfoList
                .filter((item): item is BillingTopicGroup => item.group_by === 'topic')
                .map((item) =>
                    renderLinkItem(
                        `${item.name}`,
                        `${location.pathname}/invoiceMonthCost?groupBy=topic&topics=${item.topics}`
                    )
                )}
        </List>

        <Header as="h5">Cost Across Invoice Months by Topic</Header>

        <List>
            {billingGroupInfoList
                .filter((item): item is BillingTopicGroup => item.group_by === 'topic')
                .map((item) =>
                    renderLinkItem(
                        `${item.name}`,
                        `${location.pathname}/costByMonth?groupBy=topic&topics=${item.topics}`
                    )
                )}
        </List>
    </Grid.Column>
)

const billingApi = new BillingApi()

const BillingHome = () => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'
    const [activeItem, setActiveItem] = React.useState('home')
    const [billingGroupList, setBillingGroupList] = React.useState<BillingTeamRecord[] | []>([])

    const handleItemClick = (e: MouseEvent, page: IBillingPage) => {
        e.preventDefault()
        e.stopPropagation()
        setActiveItem(page.name)
    }

    const billingSlidesLink = React.useMemo(
        () => 'https://docs.google.com/presentation/d/12cyHMMVx82f5RjJhg_VrGIdQBTCrAWJrMPWAtDLmBxU',
        []
    )
    const location = useLocation()

    React.useEffect(() => {
        billingApi
            .getBillingGroupInfo()
            .then((result) => {
                setBillingGroupList(result.data)
            })
            .catch(() => {
                setBillingGroupList([])
            })
    }, [])

    return (
        <PaddedPage>
            <div className="article">
                <h1> Billing Homepage </h1>
                <p>Welcome to the Billing Homepage</p>

                <h2>Useful Shortcuts </h2>
                <p>Billing shortcuts for quick access</p>

                <Container fluid style={{ padding: '2em 0', margin: '0px' }}>
                    <Grid stackable columns={2} relaxed>
                        {billingGroupList.map((item) =>
                            renderGridColumn(item.team_name, item.billing_groups, location)
                        )}
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
