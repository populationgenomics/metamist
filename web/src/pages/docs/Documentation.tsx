import * as React from 'react'
import { Link, useParams } from 'react-router-dom'

import Markdown from 'react-markdown'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { dracula, vs } from 'react-syntax-highlighter/dist/esm/styles/prism'
import remarkGfm from 'remark-gfm'
import remarkToc from 'remark-toc'
import { ThemeContext } from '../../shared/components/ThemeProvider'

interface IDocumentationArticleProps {
    articleid?: string
}

const RouterLink = (props: any) => {
    // use an anchor for external links
    if (props.href.match(/^(https?:)?\/\//)) return <a href={props.href}>{props.children}</a>

    let link: string = props.href
    // rewrite the internal link here if it's a relative link
    // because the openapi-generator template is incompatible with how we display markdown here.
    if (!link.startsWith('/')) {
        link = `/documentation/${link}`
    }

    return <Link to={link}>{props.children}</Link>
}

const DocumentationArticle: React.FunctionComponent<IDocumentationArticleProps> = (
    props: IDocumentationArticleProps
) => {
    const [text, setText] = React.useState<string | null>(null)
    const theme = React.useContext(ThemeContext)

    const isDarkMode = theme.theme === 'dark-mode'

    const match = useParams()

    React.useEffect(() => {
        async function fetchData() {
            const articleid = props.articleid || match.id || 'README'
            let filename = articleid?.split('#')[0]
            if (!filename.endsWith('.md')) {
                filename = `${filename}.md`
            }
            try {
                // this line turns ./static/sm_docs/{filename} into the bundled path
                // eg: /static/media/README.e3454f58.md

                // eslint-disable-next-line
                const response = await fetch(`/sm_docs/${filename}`)
                let text_ = await response.text()
                // Do some URL rewriting, because the openapi generator
                // generates links in a way that's incompatible with this hosting
                text_ = text_
                    .replaceAll('docs/', '/documentation/')
                    .replaceAll('../', '/documentation/')
                setText(text_)
            } catch (exception) {
                /* eslint-disable no-console */
                console.error(exception)
                setText(`Failed with error: ${exception}`)
            }
        }
        fetchData()
    })

    if (!text) {
        return (
            <p style={{ textAlign: 'center' }}>
                <em>Loading...</em>
            </p>
        )
    }

    return (
        <div className="article">
            {/* <p>{text}</p> */}
            <Markdown
                remarkPlugins={[remarkGfm, remarkToc]}
                components={{
                    a: RouterLink,
                    code({ node, inline, className, children, ...props_ }) {
                        const match_ = /language-(\w+)/.exec(className || '')
                        return !inline && match_ ? (
                            <SyntaxHighlighter
                                language={match_[1]}
                                PreTag="div"
                                // showLineNumbers={true}
                                style={isDarkMode ? dracula : vs}
                                {...props_}
                            >
                                {String(children).replace(/\n$/, '')}
                            </SyntaxHighlighter>
                        ) : (
                            <code className={className} {...props}>
                                {children}
                            </code>
                        )
                    },
                }}
            >
                {text}
            </Markdown>
        </div>
    )
}

export default DocumentationArticle
