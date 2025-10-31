import AccessTimeIcon from '@mui/icons-material/AccessTime'
import CalendarMonthIcon from '@mui/icons-material/CalendarMonth'
import CategoryIcon from '@mui/icons-material/Category'
import HomeIcon from '@mui/icons-material/Home'
import QueryStatsIcon from '@mui/icons-material/QueryStats'
import SellIcon from '@mui/icons-material/Sell'
import SsidChartIcon from '@mui/icons-material/SsidChart'
import SampleIcon from '../../shared/components/icons/SampleIcon'

interface IBillingPage {
    title: string
    name: string
    url: string
    icon: JSX.Element
    description?: string
}

const billingPages: IBillingPage[] = [
    {
        title: 'Home',
        name: 'home',
        url: '/billing',
        icon: <HomeIcon />,
        description:
            'The main billing home page. This page is the starting point for all billing related activities. Find a description of each page as well as a presentation outlining billing at the CPG.',
    },
    {
        title: 'Cost By Invoice Month',
        name: 'invoiceMonthCost',
        url: '/billing/invoiceMonthCost',
        icon: <CalendarMonthIcon />,
        description:
            'GCP groups costs by invoice month. This page shows the cost of each invoice month grouped by either gcp project (a google project grouping), topic (a cpg concept for grouping costs), or by stage.',
    },
    {
        title: 'Cost Across Invoice Months (Topics only)',
        name: 'costByMonth',
        url: '/billing/costByMonth',
        icon: <SellIcon />,
        description:
            'Very similar to the Cost By Invoice Month page, but only shows costs grouped by topic. However, you can view the costs across many invoice months all at the same time.',
    },
    {
        title: 'Cost By Time',
        name: 'costByTime',
        url: '/billing/costByTime',
        icon: <AccessTimeIcon />,
        description:
            'Choose a start and end date and get a summary of costs for that time period. Costs are grouped by topic, gcp project or stage and a single topic, stage or projcet can be selected.',
    },
    {
        title: 'Cost By Analysis',
        name: 'costByAnalysis',
        url: '/billing/costByAnalysis',
        icon: <QueryStatsIcon />,
        description:
            'The best place to find the costs associated with a particular analysis run on hail batch, cromwell, dataproc etc. Best way to search is by the arguid. This is also where the Analysis Runner page redirects to view run costs.',
    },
    {
        title: 'Cost By Category',
        name: 'costByCategory',
        url: '/billing/costByCategory',
        icon: <CategoryIcon />,
        description:
            'Extremely granular cost breakdowns buy sku. Can filter by prject, topic and cost categories as well.',
    },
    {
        title: 'Cost By Sample',
        name: 'costBySample',
        url: '/billing/costBySample',
        icon: <SampleIcon />,
        description: 'Break down costs to the sample or sequencing group level',
    },
    {
        title: 'Seqr Prop Map',
        name: 'seqrPropMap',
        url: '/billing/seqrPropMap',
        icon: <SsidChartIcon />,
        description: 'View the relative distribution of seqr costs across topics over time.',
    },
    {
        title: 'Sequencing Groups By Month',
        name: 'sequencingGroupsByMonth',
        url: '/billing/sequencingGroupsByMonth',
        icon: <SampleIcon />,
        description: 'The number of each type of sequencing group each month.',
    },
]

export { billingPages }
export type { IBillingPage }
