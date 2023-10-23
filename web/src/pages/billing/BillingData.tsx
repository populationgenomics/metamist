import * as React from 'react'
import { useParams, Link } from 'react-router-dom'

import Markdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import remarkToc from 'remark-toc'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { vs, dracula } from 'react-syntax-highlighter/dist/esm/styles/prism'
import { ThemeContext } from '../../shared/components/ThemeProvider'

interface IBillingDataProps {
    articleid?: string
}

const RouterLink = (props: any) => {
    // use an anchor for external links
    if (props.href.match(/^(https?:)?\/\//)) return <a href={props.href}>{props.children}</a>

    let link: string = props.href
    // rewrite the internal link here if it's a relative link
    // because the openapi-generator template is incompatible with how we display markdown here.
    if (!link.startsWith('/')) {
        link = `/billing/data/${link}`
    }

    return <Link to={link}>{props.children}</Link>
}

const BillingData: React.FunctionComponent<IBillingDataProps> = (props: IBillingDataProps) => {
    const [text, setText] = React.useState<string | null>(null)
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'
    const match = useParams()

    return (
        <div className="article">
            <h1>Billing Data</h1>
            <p>Placeholder for table page</p>
        </div>
    )
}

export default BillingData
