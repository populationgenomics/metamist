import HomeIcon from '@mui/icons-material/Home'
import TableRowsIcon from '@mui/icons-material/TableRows'
import CalendarMonthIcon from '@mui/icons-material/CalendarMonth'
import SellIcon from '@mui/icons-material/Sell'
import AccessTimeIcon from '@mui/icons-material/AccessTime'
import SsidChartIcon from '@mui/icons-material/SsidChart'
import CategoryIcon from '@mui/icons-material/Category'
import QueryStatsIcon from '@mui/icons-material/QueryStats'

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
        description: 'Billing home page',
    },
    {
        title: 'Cost By Invoice Month',
        name: 'invoiceMonthCost',
        url: '/billing/invoiceMonthCost',
        icon: <CalendarMonthIcon />,
        description: 'Cost by invoice month',
    },
    {
        title: 'Cost Across Invoice Months (Topics only)',
        name: 'costByMonth',
        url: '/billing/costByMonth',
        icon: <SellIcon />,
        description: 'Cost by month',
    },
    {
        title: 'Cost By Time',
        name: 'costByTime',
        url: '/billing/costByTime',
        icon: <AccessTimeIcon />,
        description: 'Cost by time',
    },
    {
        title: 'Cost By Analysis',
        name: 'costByAnalysis',
        url: '/billing/costByAnalysis',
        icon: <QueryStatsIcon />,
        description: 'Cost by analysis',
    },
    {
        title: 'Cost By Category',
        name: 'costByCategory',
        url: '/billing/costByCategory',
        icon: <CategoryIcon />,
        description: 'Cost by category',
    },
    {
        title: 'Seqr Prop Map',
        name: 'seqrPropMap',
        url: '/billing/seqrPropMap',
        icon: <SsidChartIcon />,
        description: 'Seqr prop map',
    },
]

export { IBillingPage, billingPages };
