import React, { useEffect } from 'react'
import {
    useParams, Link
} from "react-router-dom";

import Markdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import remarkToc from 'remark-toc'

interface IDocumentationArticleProps {
    articleId?: string
}

function RouterLink(props: any) {
    if (props.href.match(/^(https?:)?\/\//))
        return <a href={props.href}>{props.children}</a>


    let link: string = props.href
    if (!link.startsWith('/')) { link = '/documentation/' + link; }

    return <Link to={link}>{props.children}</Link>;
}

const DocumentationArticle: React.FunctionComponent<IDocumentationArticleProps> = (props: IDocumentationArticleProps) => {

    const [text, setText] = React.useState<string>('')

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
                const file = await import(`../sm_docs/${filename}`);
                const response = await fetch(file.default);
                let text = await response.text();
                text = text
                    .replaceAll('docs/', '/documentation/')
                    .replaceAll('../', '/documentation/')
                setText(text)
            }
            catch (exception) {
                console.error(exception)
                setText(`> Failed with error: ${exception}`)
            }
        }
        fetchData()
    })


    return <div className="article">
        {/* <p>{text}</p> */}
        <Markdown
            children={text}
            remarkPlugins={[remarkGfm, remarkToc]}
            components={{ a: RouterLink }}
        />
    </div>


}


export default DocumentationArticle
