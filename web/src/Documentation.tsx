import React, { useEffect } from 'react'
import {
    useParams, Link
} from "react-router-dom";

import Markdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import remarkToc from 'remark-toc'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
// import { materialDark as syntaxStyle } from 'react-syntax-highlighter/dist/esm/styles/prism'

interface IDocumentationArticleProps {
    articleId?: string
}

function RouterLink(props: any) {
    // use an anchor for external links
    if (props.href.match(/^(https?:)?\/\//))
        return <a href={props.href}>{props.children}</a>


    let link: string = props.href
    // rewrite the internal link here if it's a relative link
    // because the openapi-generator template is incompatible with how we display markdown here.
    if (!link.startsWith('/')) { link = '/documentation/' + link; }

    return <Link to={link}>{props.children}</Link>;
}

const DocumentationArticle: React.FunctionComponent<IDocumentationArticleProps> = (props: IDocumentationArticleProps) => {

    const [text, setText] = React.useState<string | null>(null)

    const match = useParams()

    useEffect(() => {
        console.log('Loading data')
        async function fetchData() {
            const articleId = props.articleId || match.id || 'README';
            let filename = articleId?.split('#')[0]
            if (!filename.endsWith('.md')) {
                filename = `${filename}.md`
            }
            try {
                // this line turns ./static/sm_docs/{filename} into the bundled path
                // eg: /static/media/README.e3454f58.md
                const file = require(`./static/sm_docs/${filename}`);
                const response = await fetch(file);
                let text = await response.text();
                // Do some URL rewriting, because the openapi generator
                // generates links in a way that's incompatible with this hosting
                text = text
                    .replaceAll('docs/', '/documentation/')
                    .replaceAll('../', '/documentation/')
                setText(text)
            }
            catch (exception) {
                console.error(exception)
                setText(`Failed with error: ${exception}`)
            }
        }
        fetchData()
    })

    if (!text) {
        return <p style={{ textAlign: 'center' }}><em>Loading...</em></p>
    }


    return <div className="article">
        {/* <p>{text}</p> */}
        <Markdown
            children={text}
            remarkPlugins={[remarkGfm, remarkToc]}
            components={{
                a: RouterLink,
                code({ node, inline, className, children, ...props }) {
                    const match = /language-(\w+)/.exec(className || '')
                    return !inline && match ? (
                        // @ts-ignore
                        <SyntaxHighlighter
                            children={String(children).replace(/\n$/, '')}
                            // style={syntaxStyle}
                            language={match[1]}
                            PreTag="div"
                            // showLineNumbers={true}
                            customStyle={{
                                margin: 0,
                                background: '#f8f8f8',
                            }}
                            {...props}
                        />
                    ) : (
                        <code className={className} {...props}>
                            {children}
                        </code>
                    )
                }
            }}
        />
    </div>


}


export default DocumentationArticle
